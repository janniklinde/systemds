/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.ooc.stream;

import java.util.function.LongUnaryOperator;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;

public final class AllocatedOOCStream<T> extends SubscribableTaskQueue<T> {
	// TODO Review
	private final OOCStream<T> _source;
	private final MemoryAllowance _allowance;
	private final LongUnaryOperator _reservationFromBytes;
	private final long _fixedReservationSize;
	private final boolean _limitPassiveOutput;
	private final AtomicBoolean _started = new AtomicBoolean();
	private volatile DMLRuntimeException _failure;
	private volatile int _pendingReservations;
	private SubscribableTaskQueue<T> _waiting;
	private boolean _reserving;
	private boolean _drainingReservations;
	private boolean _reservationDrainRequested;
	private boolean _sourceComplete;
	private boolean _outputClosed;

	public AllocatedOOCStream(OOCStream<T> source, MemoryAllowance allowance, long reservationSize,
		boolean limitPassiveOutput) {
		if(reservationSize < 0)
			throw new IllegalArgumentException("Cannot reserve negative bytes: " + reservationSize);
		_source = source;
		_allowance = allowance;
		_reservationFromBytes = null;
		_fixedReservationSize = reservationSize;
		_limitPassiveOutput = limitPassiveOutput;
		setData(source.getData());
	}

	public AllocatedOOCStream(OOCStream<T> source, MemoryAllowance allowance, LongUnaryOperator reservationFromBytes,
		boolean limitPassiveOutput) {
		_source = source;
		_allowance = allowance;
		_reservationFromBytes = reservationFromBytes;
		_fixedReservationSize = 0;
		_limitPassiveOutput = limitPassiveOutput;
		setData(source.getData());
	}

	@Override
	public void setSubscriber(Consumer<OOCStream.QueueCallback<T>> subscriber) {
		super.setSubscriber(subscriber);
		activate();
	}

	@Override
	public T dequeue() {
		activate();
		return super.dequeue();
	}

	@Override
	public OOCStream.QueueCallback<T> dequeueCB() {
		activate();
		return super.dequeueCB();
	}

	private void activate() {
		if(_started.compareAndSet(false, true))
			_source.setSubscriber(this::admit);
	}

	public static ReservationBudget detachBudget(OOCStream.QueueCallback<?> callback) {
		return callback instanceof BudgetedQueueCallback<?> budgeted ? budgeted.detachBudget() : null;
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return _source.getPrimitive();
	}

	private void admit(OOCStream.QueueCallback<T> callback) {
		if(callback.isFailure()) {
			try(callback) {
				callback.get();
			}
			catch(DMLRuntimeException failure) {
				fail(failure);
			}
			finishSource();
			return;
		}
		if(callback.isEos()) {
			callback.close();
			finishSource();
			return;
		}
		if(_failure != null) {
			callback.close();
			return;
		}
		try(callback) {
			long[] groupBytes = callback instanceof OOCStream.GroupQueueCallback<?> group ? new long[group.size()] : null;
			long bytes = reservationSize(callback, groupBytes);
			if(bytes == 0) {
				enqueueOwned(callback.keepOpen(), null);
				return;
			}
			boolean reserved = _pendingReservations == 0 &&
				(_limitPassiveOutput ? _allowance.tryReserveTask(bytes) : _allowance.tryReserve(bytes));
			if(reserved) {
				enqueueOwned(callback.keepOpen(), new ReservationBudget(_allowance, bytes), groupBytes);
				return;
			}
			retainUntilAllocated(callback);
		}
		catch(RuntimeException error) {
			fail(DMLRuntimeException.of(error));
		}
	}

	private long reservationSize(OOCStream.QueueCallback<T> callback, long[] groupBytes) {
		long bytes = 0;
		if(groupBytes != null) {
			OOCStream.GroupQueueCallback<T> group = (OOCStream.GroupQueueCallback<T>) callback;
			for(int i = 0; i < groupBytes.length; i++) {
				groupBytes[i] = _reservationFromBytes == null ? _fixedReservationSize : reservationSize(group.getBytes(i));
				bytes = Math.max(bytes, groupBytes[i]);
			}
		}
		else
			bytes = _reservationFromBytes == null ? _fixedReservationSize : reservationSize(callback.getBytes());
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot reserve negative bytes: " + bytes);
		return bytes;
	}

	private long reservationSize(long inputBytes) {
		if(inputBytes < 0)
			throw new IllegalStateException("Callback does not report its byte size");
		return _reservationFromBytes.applyAsLong(inputBytes);
	}

	private void retainUntilAllocated(OOCStream.QueueCallback<T> callback) {
		OOCStream.QueueCallback<T> retained = callback.keepOpen();
		SubscribableTaskQueue<T> waiting;
		synchronized(this) {
			if(_failure != null)
				waiting = null;
			else {
				if(_waiting == null)
					_waiting = new SubscribableTaskQueue<>();
				waiting = _waiting;
				_pendingReservations++;
			}
		}
		if(waiting == null) {
			retained.close();
			return;
		}
		try {
			waiting.enqueue(retained);
		}
		catch(RuntimeException error) {
			try {
				retained.close();
			}
			finally {
				releasePendingReservation();
			}
			throw error;
		}
		drainWaiting();
	}

	private void drainWaiting() {
		// TODO Review is this excessive synchronization really necessary?
		synchronized(this) {
			_reservationDrainRequested = true;
			if(_drainingReservations)
				return;
			_drainingReservations = true;
		}
		while(true) {
			synchronized(this) {
				_reservationDrainRequested = false;
			}
			reserveWaitingHead();
			synchronized(this) {
				if(!_reservationDrainRequested) {
					_drainingReservations = false;
					return;
				}
			}
		}
	}

	private void reserveWaitingHead() {
		// TODO Review is this excessive synchronization really necessary?
		SubscribableTaskQueue<T> waiting;
		boolean failed;
		synchronized(this) {
			waiting = _waiting;
			failed = _failure != null;
			if(waiting == null || !failed && _reserving)
				return;
			if(!failed)
				_reserving = true;
		}
		if(failed) {
			clearWaiting();
			return;
		}
		try {
			OOCStream.QueueCallback<T> head;
			long[] groupBytes;
			long bytes;
			synchronized(waiting) {
				head = waiting.peekCB();
				groupBytes = head instanceof OOCStream.GroupQueueCallback<?> group ? new long[group.size()] : null;
				bytes = head == null ? 0 : reservationSize(head, groupBytes);
			}
			if(head == null) {
				synchronized(this) {
					_reserving = false;
				}
				return;
			}
			OOCFuture<Void> reservation = _limitPassiveOutput ? _allowance.reserveTaskAsync(bytes) : _allowance.reserveAsync(bytes);
			reservation.whenComplete((ignored, error) -> completeReservation(waiting, head, bytes, groupBytes, error));
		}
		catch(RuntimeException error) {
			synchronized(this) {
				_reserving = false;
			}
			fail(DMLRuntimeException.of(error));
		}
	}

	private void completeReservation(SubscribableTaskQueue<T> waiting, OOCStream.QueueCallback<T> head,
		long bytes, long[] groupBytes, Throwable error) {
		OOCStream.QueueCallback<T> retained;
		synchronized(waiting) {
			retained = waiting.peekCB() == head ? waiting.pollCB() : null;
		}
		boolean removed = retained != null;
		try {
			if(error != null)
				fail(DMLRuntimeException.of(error));
			else if(retained == null || _failure != null)
				_allowance.release(bytes);
			else {
				OOCStream.QueueCallback<T> admitted = retained;
				retained = null;
				enqueueOwned(admitted, new ReservationBudget(_allowance, bytes), groupBytes);
			}
		}
		catch(RuntimeException completionError) {
			fail(DMLRuntimeException.of(completionError));
		}
		finally {
			try {
				if(retained != null)
					retained.close();
			}
			finally {
				if(removed)
					releasePendingReservation();
				synchronized(this) {
					_reserving = false;
				}
				drainWaiting();
			}
		}
	}

	private void clearWaiting() {
		SubscribableTaskQueue<T> waiting;
		synchronized(this) {
			waiting = _waiting;
		}
		if(waiting == null)
			return;
		OOCStream.QueueCallback<T> callback;
		while((callback = waiting.pollCB()) != null) {
			try {
				callback.close();
			}
			finally {
				releasePendingReservation();
			}
		}
	}

	private void enqueueOwned(OOCStream.QueueCallback<T> callback, ReservationBudget budget) {
		enqueueOwned(callback, budget, null);
	}

	private void enqueueOwned(OOCStream.QueueCallback<T> callback, ReservationBudget budget, long[] groupBytes) {
		OOCStream.QueueCallback<T> output = budget == null ? callback : groupBytes == null ?
			new BudgetedQueueCallback<>(callback, budget) :
			new BudgetedGroupQueueCallback<>((OOCStream.GroupQueueCallback<T>) callback, budget, groupBytes);
		try {
			enqueue(output);
		}
		catch(RuntimeException error) {
			output.close();
			throw error;
		}
	}

	private static final class BudgetedGroupQueueCallback<T> implements OOCStream.GroupQueueCallback<T> {
		private final OOCStream.GroupQueueCallback<T> _callback;
		private final SharedGroupBudget _budget;
		private final long[] _sizes;
		private boolean _closed;

		private BudgetedGroupQueueCallback(OOCStream.GroupQueueCallback<T> callback, ReservationBudget budget,
			long[] sizes) {
			this(callback, new SharedGroupBudget(budget), sizes);
		}

		private BudgetedGroupQueueCallback(OOCStream.GroupQueueCallback<T> callback, SharedGroupBudget budget,
			long[] sizes) {
			_callback = callback;
			_budget = budget;
			_sizes = sizes;
		}

		@Override
		public int size() {
			return _sizes.length;
		}

		@Override
		public long getBytes(int index) {
			return _callback.getBytes(index);
		}

		@Override
		public OOCStream.QueueCallback<T> getCallback(int index) {
			if(_closed)
				throw new IllegalStateException("Cannot open an item from a closed group callback");
			long bytes = _sizes[index];
			_budget.budget.reserveBlocking(bytes);
			return new BudgetedQueueCallback<>(_callback.getCallback(index),
				new ReservationBudget(_budget.budget, bytes));
		}

		@Override
		public T get() {
			return _callback.get();
		}

		@Override
		public T getIfResident() {
			return _callback.getIfResident();
		}

		@Override
		public long getBytes() {
			return _callback.getBytes();
		}

		@Override
		public synchronized OOCStream.QueueCallback<T> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed group callback");
			synchronized(_budget) {
				_budget.references++;
			}
			@SuppressWarnings("unchecked")
			OOCStream.GroupQueueCallback<T> retained =
				(OOCStream.GroupQueueCallback<T>) _callback.keepOpen();
			return new BudgetedGroupQueueCallback<>(retained, _budget, _sizes);
		}

		@Override
		public void close() {
			synchronized(this) {
				if(_closed)
					return;
				_closed = true;
			}
			try {
				_callback.close();
			}
			finally {
				ReservationBudget close = null;
				synchronized(_budget) {
					if(--_budget.references == 0)
						close = _budget.budget;
				}
				if(close != null)
					close.close();
			}
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_callback.fail(failure);
		}

		@Override
		public boolean isEos() {
			return _callback.isEos();
		}

		@Override
		public boolean isFailure() {
			return _callback.isFailure();
		}

		private static final class SharedGroupBudget {
			private final ReservationBudget budget;
			private int references = 1;

			private SharedGroupBudget(ReservationBudget budget) {
				this.budget = budget;
			}
		}
	}

	private boolean fail(DMLRuntimeException failure) {
		synchronized(this) {
			if(_failure != null)
				return false;
			_failure = failure;
		}
		try {
			super.propagateFailure(failure);
		}
		finally {
			clearWaiting();
		}
		return true;
	}

	private void releasePendingReservation() {
		boolean close;
		synchronized(this) {
			if(_pendingReservations <= 0)
				throw new IllegalStateException("Pending reservation count underflow");
			_pendingReservations--;
			close = _sourceComplete && _pendingReservations == 0 && !_outputClosed;
			if(close)
				_outputClosed = true;
		}
		if(close)
			closeInput();
	}

	private void finishSource() {
		boolean close;
		SubscribableTaskQueue<T> waiting;
		synchronized(this) {
			if(_sourceComplete)
				return;
			_sourceComplete = true;
			waiting = _waiting;
			close = _pendingReservations == 0 && !_outputClosed;
			if(close)
				_outputClosed = true;
		}
		if(waiting != null)
			waiting.closeInput();
		if(close)
			closeInput();
	}

	@Override
	public void propagateFailure(DMLRuntimeException failure) {
		if(fail(failure))
			_source.propagateFailure(failure);
	}

	private static final class BudgetedQueueCallback<T> implements OOCStream.QueueCallback<T> {
		private final OOCStream.QueueCallback<T> _callback;
		private final BudgetedQueueCallback<T> _budgetOwner;
		private ReservationBudget _budget;
		private int _budgetReferences;
		private boolean _closed;

		private BudgetedQueueCallback(OOCStream.QueueCallback<T> callback, ReservationBudget budget) {
			_callback = callback;
			_budgetOwner = this;
			_budget = budget;
			_budgetReferences = 1;
		}

		private BudgetedQueueCallback(OOCStream.QueueCallback<T> callback, BudgetedQueueCallback<T> budgetOwner) {
			_callback = callback;
			_budgetOwner = budgetOwner;
		}

		private synchronized ReservationBudget detachBudget() {
			if(_closed)
				throw new IllegalStateException("Cannot detach from a closed callback");
			return _budgetOwner.takeBudget();
		}

		private synchronized ReservationBudget takeBudget() {
			ReservationBudget budget = _budget;
			_budget = null;
			return budget;
		}

		@Override
		public T get() {
			return _callback.get();
		}

		@Override
		public T getIfResident() {
			return _callback.getIfResident();
		}

		@Override
		public long getBytes() {
			return _callback.getBytes();
		}

		@Override
		public synchronized OOCStream.QueueCallback<T> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed callback");
			OOCStream.QueueCallback<T> retained = _callback.keepOpen();
			_budgetOwner.retainBudget();
			return new BudgetedQueueCallback<>(retained, _budgetOwner);
		}

		private synchronized void retainBudget() {
			_budgetReferences++;
		}

		@Override
		public void close() {
			synchronized(this) {
				if(_closed)
					return;
				_closed = true;
			}
			try {
				_callback.close();
			}
			finally {
				_budgetOwner.releaseBudget();
			}
		}

		private void releaseBudget() {
			ReservationBudget budget = null;
			synchronized(this) {
				_budgetReferences--;
				if(_budgetReferences == 0) {
					budget = _budget;
					_budget = null;
				}
			}
			if(budget != null)
				budget.close();
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_callback.fail(failure);
		}

		@Override
		public boolean isEos() {
			return _callback.isEos();
		}

		@Override
		public boolean isFailure() {
			return _callback.isFailure();
		}
	}
}
