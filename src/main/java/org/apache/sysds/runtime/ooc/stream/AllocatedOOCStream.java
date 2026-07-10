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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.legacy.OOCCacheScheduler;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;

/**
 * Stream wrapper that forwards source callbacks only after reserving the requested operator memory.
 * The admitted reservation is carried as a budget on the forwarded callback until the consuming
 * operator detaches it or the callback is closed.
 */
public final class AllocatedOOCStream<T> extends SubscribableTaskQueue<T> {
	@FunctionalInterface
	public interface BudgetReserveFunction {
		ReservationBudget reserve(MemoryAllowance allowance, long bytes);
	}

	private final OOCStream<T> _source;
	private final MemoryAllowance _allowance;
	private final ToLongFunction<T> _reservationFn;
	private final BudgetReserveFunction _budgetReserveFn;
	private final boolean _reserve;
	private final AtomicInteger _pending;
	private final AtomicBoolean _closed;
	private volatile DMLRuntimeException _failure;

	public AllocatedOOCStream(OOCStream<T> source, MemoryAllowance allowance, ToLongFunction<T> allocFn,
		boolean reserve) {
		this(source, allowance, allocFn, reserve, ReservationBudget::admitted);
	}

	public AllocatedOOCStream(OOCStream<T> source, MemoryAllowance allowance, ToLongFunction<T> allocFn,
		boolean reserve, BudgetReserveFunction budgetReserveFn) {
		_source = source;
		_allowance = allowance;
		_reservationFn = allocFn;
		_budgetReserveFn = budgetReserveFn;
		_reserve = reserve;
		_pending = new AtomicInteger(1);
		_closed = new AtomicBoolean(false);
		source.setSubscriber(this::admit);
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return _source.getPrimitive();
	}

	private void admit(OOCStream.QueueCallback<T> callback) {
		try {
			if(callback.isFailure()) {
				DMLRuntimeException failure;
				try {
					callback.get();
					failure = new DMLRuntimeException("Source stream failed without cause.");
				}
				catch(DMLRuntimeException ex) {
					failure = ex;
				}
				callback.close();
				fail(failure);
				releasePending();
				return;
			}
			if(callback.isEos()) {
				callback.close();
				releasePending();
				return;
			}
			long bytes = _reserve ? _reservationFn.applyAsLong(callback.get()) : 0;
			if(bytes > 0 && OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC ADMIT TRACE] admit source=" + System.identityHashCode(_source)
					+ " stream=" + System.identityHashCode(this)
					+ " bytes=" + bytes
					+ " allowance=" + System.identityHashCode(_allowance)
					+ " cb=" + System.identityHashCode(callback));
			if(bytes < 0)
				throw new IllegalArgumentException("Cannot allocate negative bytes: " + bytes);
			if(!_reserve || bytes == 0) {
				forward(callback, null);
				return;
			}
			if(_allowance.tryReserve(bytes)) {
				forward(callback, createBudget(bytes));
				return;
			}
			retainUntilAllocated(callback, bytes);
		}
		catch(Throwable t) {
			callback.close();
			fail(DMLRuntimeException.of(t));
		}
	}

	private void retainUntilAllocated(OOCStream.QueueCallback<T> callback, long bytes) {
		OOCStream.QueueCallback<T> retained = callback.keepOpen();
		_pending.incrementAndGet();
		callback.close();
		OOCFuture<Void> reservation;
		try {
			reservation = _allowance.reserveAsync(bytes);
		}
		catch(Throwable t) {
			retained.close();
			releasePending();
			throw t;
		}
		reservation.whenComplete((ignored, error) -> {
			if(error != null) {
				retained.close();
				fail(DMLRuntimeException.of(error));
				releasePending();
				return;
			}
			ReservationBudget budget = null;
			if(_failure != null) {
				_allowance.release(bytes);
				retained.close();
				releasePending();
				return;
			}
			try {
				budget = createBudget(bytes);
				forward(retained, budget);
				budget = null;
			}
			catch(Throwable t) {
				fail(DMLRuntimeException.of(t));
			}
			finally {
				if(budget != null)
					budget.close();
				releasePending();
			}
		});
	}

	private ReservationBudget createBudget(long bytes) {
		try {
			ReservationBudget budget = _budgetReserveFn.reserve(_allowance, bytes);
			if(budget == null)
				throw new IllegalStateException("Budget reserve function returned null for " + bytes + " bytes.");
			return budget;
		}
		catch(Throwable t) {
			_allowance.release(bytes);
			throw DMLRuntimeException.of(t);
		}
	}

	private void forward(OOCStream.QueueCallback<T> callback, ReservationBudget budget) {
		long reservedBytes = budget == null ? 0 : budget.getGrantedMemory();
		OOCStream.QueueCallback<T> retained = callback.keepOpen();
		BudgetedQueueCallback<T> budgeted = budget == null ? null : new BudgetedQueueCallback<>(retained, budget);
		try {
			if(reservedBytes > 0 && OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC ADMIT TRACE] forward stream=" + System.identityHashCode(this)
					+ " bytes=" + reservedBytes
					+ " allowance=" + System.identityHashCode(_allowance)
					+ " cb=" + System.identityHashCode(callback)
					+ " retained=" + System.identityHashCode(retained));
			enqueue(budgeted == null ? retained : budgeted);
			budgeted = null;
			retained = null;
			budget = null;
		}
		catch(Throwable t) {
			if(budget != null)
				budget.close();
			throw t;
		}
		finally {
			if(budgeted != null)
				budgeted.close();
			if(retained != null)
				retained.close();
			callback.close();
		}
	}

	private void fail(DMLRuntimeException failure) {
		if(_failure != null)
			return;
		_failure = failure;
		super.propagateFailure(failure);
	}

	private void releasePending() {
		if(_pending.decrementAndGet() == 0 && _closed.compareAndSet(false, true))
			closeInput();
	}

	@Override
	public void propagateFailure(DMLRuntimeException re) {
		boolean firstFailure = _failure == null;
		_failure = re;
		super.propagateFailure(re);
		if(firstFailure)
			_source.propagateFailure(re);
	}

	public static final class BudgetedQueueCallback<T> implements OOCStream.QueueCallback<T> {
		private final OOCStream.QueueCallback<T> _callback;
		private final SharedBudget _budget;
		private boolean _closed;

		private BudgetedQueueCallback(OOCStream.QueueCallback<T> callback, ReservationBudget budget) {
			this(callback, new SharedBudget(budget));
		}

		private BudgetedQueueCallback(OOCStream.QueueCallback<T> callback, SharedBudget budget) {
			_callback = callback;
			_budget = budget;
			_closed = false;
		}

		public ReservationBudget detachBudget() {
			return _budget.detach();
		}

		@Override
		public T get() {
			return _callback.get();
		}

		@Override
		public synchronized OOCStream.QueueCallback<T> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed callback");
			_budget.retain();
			try {
				return new BudgetedQueueCallback<>(_callback.keepOpen(), _budget);
			}
			catch(Throwable t) {
				_budget.release();
				throw DMLRuntimeException.of(t);
			}
		}

		@Override
		public synchronized void close() {
			if(_closed)
				return;
			_closed = true;
			try {
				_callback.close();
			}
			finally {
				_budget.release();
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

		@Override
		public long getManagedBytes() {
			return _callback.getManagedBytes();
		}

		@Override
		public OOCStream.QueueCallback<T> transferOwnershipBlocking(MemoryAllowance allowance) {
			_callback.transferOwnershipBlocking(allowance);
			return this;
		}

		@Override
		public OOCStream.QueueCallback<T> tryTransferOwnership(MemoryAllowance allowance) {
			OOCStream.QueueCallback<T> transferred = _callback.tryTransferOwnership(allowance);
			return transferred == null ? null : this;
		}

		@Override
		public void forget() {
			_callback.forget();
		}

		@Override
		public BlockKey getBlockKey() {
			return _callback.getBlockKey();
		}

		@Override
		public OOCCacheScheduler.AllowanceBackedPin getBackingPin() {
			return _callback.getBackingPin();
		}

		private static final class SharedBudget {
			private final AtomicInteger _refs;
			private final AtomicReference<ReservationBudget> _budget;

			private SharedBudget(ReservationBudget budget) {
				_refs = new AtomicInteger(1);
				_budget = new AtomicReference<>(budget);
			}

			private void retain() {
				_refs.incrementAndGet();
			}

			private ReservationBudget detach() {
				return _budget.getAndSet(null);
			}

			private void release() {
				if(_refs.decrementAndGet() != 0)
					return;
				ReservationBudget budget = _budget.getAndSet(null);
				if(budget != null)
					budget.close();
			}
		}
	}
}
