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

package org.apache.sysds.runtime.ooc.primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCPlanner;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

/** Fans complete matrix row groups from one materialized-store reader into multiple live streams. */
public final class SharedRowsOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _input;
	private final List<Output> _outputs;
	private final int _maxOpenRows;
	private final AtomicBoolean _finished;
	private List<OOCStream<IndexedMatrixValue>> _outputStreams;
	private MaterializedStore<IndexedMatrixValue> _store;
	private IndexedMaterializedStoreReader<IndexedMatrixValue> _reader;
	private int _rowBlocks;
	private int _colBlocks;
	private int _nextRow;
	private int _openRows;
	private long _rowBytes;
	private boolean _scheduling;
	private boolean _reschedule;

	public SharedRowsOOCPrimitive(OOCStreamable<IndexedMatrixValue> input,
		List<OOCStreamable<IndexedMatrixValue>> outputs, int maxOpenRows, StreamContext context) {
		super(context, input);
		if(outputs.isEmpty())
			throw new IllegalArgumentException("Shared rows require at least one output.");
		if(maxOpenRows <= 0)
			throw new IllegalArgumentException("Shared rows require at least one open row.");
		_input = input;
		_outputs = new ArrayList<>();
		outputs.forEach(output -> _outputs.add(new Output(output, false)));
		_maxOpenRows = maxOpenRows;
		_finished = new AtomicBoolean();
	}

	public SharedRowsOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, int maxOpenRows, StreamContext context) {
		super(context, input);
		if(maxOpenRows <= 0)
			throw new IllegalArgumentException("Shared rows require at least one open row.");
		_input = input;
		_outputs = new ArrayList<>();
		_maxOpenRows = maxOpenRows;
		_finished = new AtomicBoolean();
	}

	public synchronized void addOutput(OOCStreamable<IndexedMatrixValue> output) {
		if(hasStartedExecution())
			throw new IllegalStateException("Cannot add a shared-row consumer after execution started.");
		_outputs.add(new Output(output, true));
		output.assignPrimitive(this);
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		return List.of(new OOCMaterializedInputRequest(0, OOCStoreLayout.ROW_MAJOR, 1));
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		OOCPrimitive dependency = getInputDependency(0);
		if(dependency != null)
			dependency.requestPattern(OOCAccessPattern.ROW_MAJOR);
	}

	@Override
	protected long getMaxTaskReservationBytes() {
		DataCharacteristics dc = _input.getDataCharacteristics();
		return dc == null || !dc.dimsKnown() || dc.getBlocksize() <= 0 ? 0 : rowBytes(dc);
	}

	private static long rowBytes(DataCharacteristics dc) {
		return OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(OOCUtils.estimateFullTileBytes(dc)) *
			dc.getNumColBlocks();
	}

	@Override
	protected void startExecution() {
		if(OOCPlanner.deferStart(this::startDeferredExecution))
			return;
		startDeferredExecution();
	}

	private void startDeferredExecution() {
		if(_outputs.isEmpty())
			throw new DMLRuntimeException("Shared rows require at least one active consumer.");
		DataCharacteristics dc = _input.getDataCharacteristics();
		if(dc == null || !dc.dimsKnown() || dc.getBlocksize() <= 0)
			throw new DMLRuntimeException("Shared rows require known input dimensions and block size.");
		_rowBlocks = Math.toIntExact(dc.getNumRowBlocks());
		_colBlocks = Math.toIntExact(dc.getNumColBlocks());
		if(_rowBlocks <= 0 || _colBlocks <= 0)
			throw new DMLRuntimeException("Shared rows require non-empty input block geometry.");
		_rowBytes = rowBytes(dc);
		_outputStreams = _outputs.stream().map(Output::stream).map(OOCStreamable::getWriteStream).toList();
		for(OOCStream<IndexedMatrixValue> output : _outputStreams)
			getContext().addOutStream(output);

		getMaterializedInput(0).whenComplete((store, error) -> {
			if(error != null) {
				failAndFinish(error);
				return;
			}
			_store = store;
			store.completion().whenComplete((ignored, completionError) -> {
				if(completionError != null) {
					failAndFinish(completionError);
					return;
				}
				try {
					_reader = store.openIndexedReader(new CountingLiveness(_rowBlocks * _colBlocks, 1));
					scheduleRows();
				}
				catch(Throwable failure) {
					failAndFinish(failure);
				}
			});
		});
	}

	private void scheduleRows() {
		synchronized(this) {
			if(_scheduling) {
				_reschedule = true;
				return;
			}
			_scheduling = true;
		}
		while(true) {
			List<Integer> rows = new ArrayList<>();
			boolean complete;
			synchronized(this) {
				while(!hasFailed() && _openRows < _maxOpenRows && _nextRow < _rowBlocks) {
					rows.add(_nextRow++);
					_openRows++;
				}
				complete = _openRows == 0 && (hasFailed() || _nextRow == _rowBlocks);
			}
			for(int row : rows)
				requestRow(row);
			if(complete)
				finish();
			synchronized(this) {
				if(!_reschedule) {
					_scheduling = false;
					return;
				}
				_reschedule = false;
			}
		}
	}

	private void requestRow(int row) {
		_allowance.reserveAsync(_rowBytes).whenComplete((ignored, reservationError) -> {
			if(reservationError != null) {
				rowFinished(reservationError);
				return;
			}
			ReservationBudget budget = new ReservationBudget(_allowance, _rowBytes).enableReuse();
			List<OOCFuture<StoreLease<IndexedMatrixValue>>> requests = new ArrayList<>(_colBlocks);
			try {
				for(int col = 0; col < _colBlocks; col++)
					requests.add(_reader.request(row + 1L, col + 1L, budget));
			}
			catch(Throwable failure) {
				requests.forEach(request -> request.whenComplete((lease, error) -> closeLease(lease)));
				budget.close();
				rowFinished(failure);
				return;
			}
			OOCFuture.allOf(requests, SharedRowsOOCPrimitive::closeLease).whenComplete((leases, error) -> {
				if(error != null) {
					budget.close();
					rowFinished(error);
					return;
				}
				publishRow(row, leases, budget);
			});
		});
	}

	private void publishRow(int row, List<StoreLease<IndexedMatrixValue>> leases, ReservationBudget budget) {
		List<OOCStream.QueueCallback<IndexedMatrixValue>> callbacks = new ArrayList<>();
		AtomicBoolean rowClosed = new AtomicBoolean();
		List<Boolean> active = _outputs.stream().map(Output::isActive).toList();
		AtomicInteger remaining = new AtomicInteger(
			Math.toIntExact(active.stream().filter(Boolean::booleanValue).count() * leases.size()));
		Runnable callbackClosed = () -> {
			int left = remaining.decrementAndGet();
			if(left == 0 && rowClosed.compareAndSet(false, true))
				rowFinished(null);
		};
		boolean releasesScheduled = false;
		try {
			for(StoreLease<IndexedMatrixValue> lease : leases)
				if(lease == null)
					throw new DMLRuntimeException("Missing shared-row tile for block row " + (row + 1));
			for(int output = 0; output < _outputStreams.size(); output++)
				for(int col = 0; col < _colBlocks; col++) {
					MaterializedCallback<IndexedMatrixValue> callback = new MaterializedCallback<>(
						leases.get(col).retain(), row * _colBlocks + col, _store);
					if(!active.get(output))
						callback.tryPark();
					callbacks.add(new SharedRowCallback(callback, active.get(output) ? callbackClosed : null));
				}

			OOCFuture.allOf(leases.stream().map(StoreLease::closeAsync).toList()).whenComplete((ignored, error) -> {
				budget.close();
				if(error != null)
					fail(error);
			});
			releasesScheduled = true;

			int callback = 0;
			for(OOCStream<IndexedMatrixValue> output : _outputStreams)
				for(int col = 0; col < _colBlocks; col++) {
					OOCStream.QueueCallback<IndexedMatrixValue> value = callbacks.get(callback);
					output.enqueue(value);
					callbacks.set(callback++, null);
				}
			if(remaining.get() == 0 && rowClosed.compareAndSet(false, true))
				rowFinished(null);
		}
		catch(Throwable failure) {
			fail(failure);
			callbacks.forEach(SharedRowsOOCPrimitive::closeCallback);
			if(!releasesScheduled)
				OOCFuture.allOf(leases.stream().map(StoreLease::closeAsync).toList()).whenComplete((ignored, error) -> {
					budget.close();
					if(error != null)
						fail(error);
				});
			if(rowClosed.compareAndSet(false, true))
				rowFinished(failure);
		}
	}

	private void rowFinished(Throwable error) {
		if(error != null)
			fail(error);
		synchronized(this) {
			_openRows--;
		}
		scheduleRows();
	}

	private void failAndFinish(Throwable error) {
		fail(error);
		finish();
	}

	private void finish() {
		if(!_finished.compareAndSet(false, true))
			return;
		try {
			for(OOCStream<IndexedMatrixValue> output : _outputStreams)
				if(hasFailed())
					output.propagateFailure(DMLRuntimeException.of(getFailure()));
				else
					output.closeInput();
		}
		finally {
			try {
				if(_reader != null)
					_reader.close();
			}
			finally {
				try {
					if(_store != null)
						_store.close();
				}
				finally {
					onComplete();
				}
			}
		}
	}

	private static void closeLease(StoreLease<IndexedMatrixValue> lease) {
		if(lease != null)
			lease.close();
	}

	private static void closeCallback(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback != null)
			callback.close();
	}

	private static final class SharedRowCallback implements OOCStream.PurgeableQueueCallback<IndexedMatrixValue> {
		private OOCStream.QueueCallback<IndexedMatrixValue> _delegate;
		private final AtomicInteger _references;
		private final Runnable _onClosed;
		private boolean _referenceReleased;

		private SharedRowCallback(OOCStream.QueueCallback<IndexedMatrixValue> delegate, Runnable onClosed) {
			this(delegate, new AtomicInteger(1), onClosed);
		}

		private SharedRowCallback(OOCStream.QueueCallback<IndexedMatrixValue> delegate, AtomicInteger references,
			Runnable onClosed) {
			this(delegate, references, onClosed, false);
		}

		private SharedRowCallback(OOCStream.QueueCallback<IndexedMatrixValue> delegate, AtomicInteger references,
			Runnable onClosed, boolean referenceReleased) {
			_delegate = delegate;
			_references = references;
			_onClosed = onClosed;
			_referenceReleased = referenceReleased;
		}

		@Override
		public synchronized IndexedMatrixValue get() {
			return _delegate.get();
		}

		@Override
		public synchronized OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
			boolean trackReference = !_referenceReleased;
			if(trackReference)
				_references.incrementAndGet();
			try {
				return new SharedRowCallback(_delegate.keepOpen(), _references, _onClosed, !trackReference);
			}
			catch(Throwable failure) {
				if(trackReference)
					_references.decrementAndGet();
				throw failure;
			}
		}

		@Override
		public synchronized void fail(DMLRuntimeException failure) {
			_delegate.fail(failure);
		}

		@Override
		public synchronized void close() {
			if(_delegate == null)
				return;
			try {
				_delegate.close();
			}
			finally {
				_delegate = null;
				releaseReference();
			}
		}

		@Override
		public synchronized long tryPark() {
			if(!(_delegate instanceof MaterializedCallback<?> callback))
				return 0;
			long bytes = callback.tryPark();
			if(callback.isParked())
				releaseReference();
			return bytes;
		}

		@Override
		public synchronized void releaseRetention() {
			releaseReference();
		}

		private void releaseReference() {
			if(_referenceReleased)
				return;
			_referenceReleased = true;
			if(_references.decrementAndGet() == 0 && _onClosed != null)
				_onClosed.run();
		}

		@Override
		public synchronized boolean isEos() {
			return _delegate.isEos();
		}

		@Override
		public synchronized boolean isFailure() {
			return _delegate.isFailure();
		}

		@Override
		public synchronized OOCStream.QueueCallback<IndexedMatrixValue> delegate() {
			return _delegate;
		}
	}

	private record Output(OOCStreamable<IndexedMatrixValue> stream, boolean requireSubscriber) {
		private boolean isActive() {
			return !requireSubscriber || !(stream instanceof SubscribableTaskQueue<?>) ||
				((SubscribableTaskQueue<?>) stream).hasSubscriber();
		}
	}

}
