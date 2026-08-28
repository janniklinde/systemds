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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

/**
 * Applies a fused derive-and-combine operation to complete row groups of one materialized matrix. The anchor tiles
 * remain pinned from derivation through combination, allowing both stages to share one physical read of each group.
	 * The bounded driver can overlap multiple group fetches. Workspace and output estimates are upper bounds; outputs
	 * must not alias storage owned by an anchor tile.
 */
public final class CorrelatedScanOOCPrimitive<D, O> extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _input;
	private final OOCStreamable<O> _output;
	private final Function<List<IndexedMatrixValue>, D> _derive;
	private final BiFunction<List<IndexedMatrixValue>, D, List<O>> _combine;
	private final ToLongFunction<O> _outputSize;
	private final long _workspaceBytes;
	private final long _outputBytesPerGroup;
	private final Semaphore _fetchSlots;
	private final AtomicBoolean _cleaned = new AtomicBoolean();
	private final AtomicInteger _active = new AtomicInteger(1);
	private MaterializedStore<IndexedMatrixValue> _store;
	private IndexedMaterializedStoreReader<IndexedMatrixValue> _reader;
	private OOCStream<GroupWork> _ready;
	private OOCStream<O> _outputStream;
	private int _rowBlocks;
	private int _colBlocks;
	private long _taskBytes;

	public CorrelatedScanOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<O> output,
		Function<List<IndexedMatrixValue>, D> derive, BiFunction<List<IndexedMatrixValue>, D, List<O>> combine,
		ToLongFunction<O> outputSize, long workspaceBytes, long outputBytesPerGroup, int maxPendingFetches,
		StreamContext context) {
		super(context, input);
		if(workspaceBytes < 0 || outputBytesPerGroup < 0)
			throw new IllegalArgumentException("Correlated scan memory estimates must not be negative.");
		if(maxPendingFetches <= 0)
			throw new IllegalArgumentException("Correlated scan must allow at least one pending fetch.");
		_input = input;
		_output = output;
		_derive = derive;
		_combine = combine;
		_outputSize = outputSize;
		_workspaceBytes = workspaceBytes;
		_outputBytesPerGroup = outputBytesPerGroup;
		_fetchSlots = new Semaphore(maxPendingFetches);
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		long colBlocks = OOCUtils.getNumColBlocks(_input.getDataCharacteristics());
		return List.of(new OOCMaterializedInputRequest(0, OOCStoreLayout.ROW_MAJOR, 1,
			(row, col) -> (row - 1) * colBlocks + col - 1));
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(OOCAccessPattern.ROW_MAJOR);
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(OOCAccessPattern.ROW_MAJOR);
	}

	@Override
	protected long getMaxTaskReservationBytes() {
		DataCharacteristics dc = _input.getDataCharacteristics();
		return dc == null || !dc.dimsKnown() || dc.getBlocksize() <= 0 ? 0 : taskBytes(dc);
	}

	private long taskBytes(DataCharacteristics dc) {
		long pinned = OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(OOCUtils.estimateFullTileBytes(dc)) *
			dc.getNumColBlocks();
		return pinned + _workspaceBytes + _outputBytesPerGroup;
	}

	@Override
	protected void startExecution() {
		DataCharacteristics dc = _input.getDataCharacteristics();
		if(dc == null || !dc.dimsKnown() || dc.getBlocksize() <= 0)
			throw new DMLRuntimeException("Correlated scan requires known input dimensions and block size.");
		_rowBlocks = Math.toIntExact(dc.getNumRowBlocks());
		_colBlocks = Math.toIntExact(dc.getNumColBlocks());
		if(_rowBlocks <= 0 || _colBlocks <= 0)
			throw new DMLRuntimeException("Correlated scan requires non-empty input block geometry.");
		_taskBytes = taskBytes(dc);
		_outputStream = _output.getWriteStream();
		_ready = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _ready);
		OOCInstructionUtils.submitCloseableOOCTasks(_ready, this::process, getContext())
			.whenComplete((ignored, error) -> {
				try {
					if(error != null)
						fail(error);
					_outputStream.closeInput();
				}
				catch(Throwable failure) {
					fail(failure);
				}
				finally {
					cleanup();
				}
			});

		getMaterializedInput(0).whenComplete((store, error) -> {
			if(error != null) {
				fail(error);
				completeOne();
				return;
			}
			_store = store;
			store.completion().whenComplete((ignored, completionError) -> {
				if(completionError != null) {
					fail(completionError);
					completeOne();
					return;
				}
				try {
					_reader = store.openIndexedReader(new CountingLiveness(_rowBlocks * _colBlocks, 1));
					OOCInstructionUtils.submitOOCTask(this::drive, new StreamContext());
				}
				catch(Throwable failure) {
					fail(failure);
					completeOne();
				}
			});
		});
	}

	private void drive() {
		try {
			for(int group = 0; group < _rowBlocks && !hasFailed(); group++) {
				ReservationBudget budget = null;
				boolean fetchSlotAcquired = false;
				boolean handedOff = false;
				try {
					if(hasFailed())
						break;
					_allowance.reserveBlocking(_taskBytes);
					budget = new ReservationBudget(_allowance, _taskBytes).enableReuse();
					if(hasFailed())
						break;
					_fetchSlots.acquire();
					fetchSlotAcquired = true;
					if(hasFailed())
						break;
					_active.incrementAndGet();
					handedOff = true;
					requestGroup(group, budget);
					budget = null;
				}
				finally {
					if(budget != null)
						budget.close();
					if(fetchSlotAcquired && !handedOff)
						_fetchSlots.release();
				}
			}
		}
		catch(Throwable failure) {
			fail(failure);
		}
		finally {
			completeOne();
		}
	}

	private void requestGroup(int group, ReservationBudget budget) {
		List<OOCFuture<StoreLease<IndexedMatrixValue>>> requests = new ArrayList<>(_colBlocks);
		try {
			for(int col = 0; col < _colBlocks; col++)
				requests.add(_reader.request(group + 1L, col + 1L, budget));
		}
		catch(Throwable failure) {
			requests.forEach(request -> request.whenComplete((lease, error) -> closeLease(lease)));
			budget.close();
			_fetchSlots.release();
			fail(failure);
			completeGroup();
			return;
		}
		OOCFuture.allOf(requests, CorrelatedScanOOCPrimitive::closeLease).whenComplete((leases, error) -> {
			_fetchSlots.release();
			if(error != null) {
				budget.close();
				fail(error);
				completeGroup();
				return;
			}
			try {
				for(StoreLease<IndexedMatrixValue> lease : leases)
					if(lease == null)
						throw new DMLRuntimeException("Missing correlated-scan tile for block row " + (group + 1));
				_ready.enqueue(new GroupWork(leases, budget));
			}
			catch(Throwable failure) {
				leases.forEach(CorrelatedScanOOCPrimitive::closeLease);
				budget.close();
				fail(failure);
				completeGroup();
			}
		});
	}

	private void process(GroupWork work) {
		ReservationBudget budget = work.takeBudget();
		List<OOCStream.QueueCallback<O>> callbacks = new ArrayList<>();
		boolean workspaceReserved = false;
		try {
			if(!budget.tryReserve(_workspaceBytes))
				throw new DMLRuntimeException("Correlated-scan workspace exceeds its admitted task budget.");
			workspaceReserved = true;
			List<IndexedMatrixValue> anchor = work.values();
			D derived = _derive.apply(anchor);
			List<O> outputs = _combine.apply(anchor, derived);
			if(outputs == null)
				throw new DMLRuntimeException("Correlated scan produced a null output list.");
			for(O output : outputs) {
				if(output == null)
					throw new DMLRuntimeException("Correlated scan produced a null output.");
				long bytes = _outputSize.applyAsLong(output);
				if(!budget.tryReserve(bytes))
					throw new DMLRuntimeException("Correlated-scan output exceeds its admitted task budget.");
				callbacks.add(new InMemoryQueueCallback<>(output, null, budget, bytes));
			}
			budget.release(_workspaceBytes);
			workspaceReserved = false;
			work.releaseLeases();
			budget.close();
			for(int i = 0; i < callbacks.size(); i++) {
				OOCStream.QueueCallback<O> callback = callbacks.get(i);
				_outputStream.enqueue(callback);
				callbacks.set(i, null);
			}
		}
		catch(Throwable failure) {
			fail(failure);
		}
		finally {
			for(OOCStream.QueueCallback<O> callback : callbacks)
				if(callback != null)
					callback.close();
			if(workspaceReserved)
				budget.release(_workspaceBytes);
			budget.close();
			work.close();
			completeGroup();
		}
	}

	private void completeGroup() {
		completeOne();
	}

	private void completeOne() {
		if(_active.decrementAndGet() != 0)
			return;
		try {
			_ready.closeInput();
		}
		catch(IllegalStateException ignored) {
		}
	}

	private void cleanup() {
		if(!_cleaned.compareAndSet(false, true))
			return;
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

	private static void closeLease(StoreLease<IndexedMatrixValue> lease) {
		if(lease != null)
			lease.close();
	}

	private static final class GroupWork implements AutoCloseable {
		private List<StoreLease<IndexedMatrixValue>> _leases;
		private ReservationBudget _budget;

		private GroupWork(List<StoreLease<IndexedMatrixValue>> leases, ReservationBudget budget) {
			_leases = leases;
			_budget = budget;
		}

		private List<IndexedMatrixValue> values() {
			return _leases.stream().map(StoreLease::value).toList();
		}

		private ReservationBudget takeBudget() {
			ReservationBudget budget = _budget;
			_budget = null;
			return budget;
		}

		private void releaseLeases() {
			if(_leases == null)
				return;
			_leases.forEach(CorrelatedScanOOCPrimitive::closeLease);
			_leases = null;
		}

		@Override
		public void close() {
			releaseLeases();
			if(_budget != null) {
				_budget.close();
				_budget = null;
			}
		}
	}
}
