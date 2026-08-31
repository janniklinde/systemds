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
 * remain pinned from derivation through combination, allowing both stages to share one physical read of each group. The
 * bounded driver can overlap multiple group fetches. Workspace and output estimates are upper bounds; outputs must not
 * alias storage owned by an anchor tile.
 */
public final class CorrelatedScanOOCPrimitive<D, O> extends OOCPrimitive {
	public enum InputAccess {
		ROW_ALIGNED, FULL
	}

	private final List<OOCStreamable<IndexedMatrixValue>> _inputs;
	private final List<InputAccess> _inputAccess;
	private final List<OOCStreamable<O>> _outputs;
	private final BiFunction<List<IndexedMatrixValue>, List<List<IndexedMatrixValue>>, D> _derive;
	private final BiFunction<List<IndexedMatrixValue>, D, List<Output<O>>> _combine;
	private final ToLongFunction<O> _outputSize;
	private final long _workspaceBytes;
	private final long _outputBytesPerGroup;
	private final Semaphore _fetchSlots;
	private final AtomicBoolean _cleaned = new AtomicBoolean();
	private final AtomicBoolean _startupFinished = new AtomicBoolean();
	private final AtomicInteger _active = new AtomicInteger(1);
	private final AtomicInteger _pendingStores;
	private final MaterializedStore<IndexedMatrixValue>[] _stores;
	private final IndexedMaterializedStoreReader<IndexedMatrixValue>[] _readers;
	private OOCStream<GroupWork> _ready;
	private List<OOCStream<O>> _outputStreams;
	private int _rowBlocks;
	private int _colBlocks;
	private long _taskBytes;

	public CorrelatedScanOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<O> output,
		Function<List<IndexedMatrixValue>, D> derive, BiFunction<List<IndexedMatrixValue>, D, List<O>> combine,
		ToLongFunction<O> outputSize, long workspaceBytes, long outputBytesPerGroup, int maxPendingFetches,
		StreamContext context) {
		this(input, List.of(output), derive,
			(values, derived) -> combine.apply(values, derived).stream().map(value -> new Output<>(0, value)).toList(),
			outputSize, workspaceBytes, outputBytesPerGroup, maxPendingFetches, context);
	}

	public CorrelatedScanOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, List<? extends OOCStreamable<O>> outputs,
		Function<List<IndexedMatrixValue>, D> derive, BiFunction<List<IndexedMatrixValue>, D, List<Output<O>>> combine,
		ToLongFunction<O> outputSize, long workspaceBytes, long outputBytesPerGroup, int maxPendingFetches,
		StreamContext context) {
		this(List.of(input), outputs, (anchor, sides) -> derive.apply(anchor), combine, outputSize, workspaceBytes,
			outputBytesPerGroup, maxPendingFetches, context);
	}

	@SuppressWarnings("unchecked")
	public CorrelatedScanOOCPrimitive(List<? extends OOCStreamable<IndexedMatrixValue>> inputs,
		List<? extends OOCStreamable<O>> outputs,
		BiFunction<List<IndexedMatrixValue>, List<List<IndexedMatrixValue>>, D> derive,
		BiFunction<List<IndexedMatrixValue>, D, List<Output<O>>> combine, ToLongFunction<O> outputSize,
		long workspaceBytes, long outputBytesPerGroup, int maxPendingFetches, StreamContext context) {
		this(inputs, defaultInputAccess(inputs.size()), outputs, derive, combine, outputSize, workspaceBytes,
			outputBytesPerGroup, maxPendingFetches, context);
	}

	@SuppressWarnings("unchecked")
	public CorrelatedScanOOCPrimitive(List<? extends OOCStreamable<IndexedMatrixValue>> inputs,
		List<InputAccess> inputAccess, List<? extends OOCStreamable<O>> outputs,
		BiFunction<List<IndexedMatrixValue>, List<List<IndexedMatrixValue>>, D> derive,
		BiFunction<List<IndexedMatrixValue>, D, List<Output<O>>> combine, ToLongFunction<O> outputSize,
		long workspaceBytes, long outputBytesPerGroup, int maxPendingFetches, StreamContext context) {
		super(context, inputs.toArray(OOCStreamable[]::new));
		if(inputs.isEmpty())
			throw new IllegalArgumentException("Correlated scan requires an anchor input.");
		if(outputs.isEmpty())
			throw new IllegalArgumentException("Correlated scan requires at least one output stream.");
		if(workspaceBytes < 0 || outputBytesPerGroup < 0)
			throw new IllegalArgumentException("Correlated scan memory estimates must not be negative.");
		if(maxPendingFetches <= 0)
			throw new IllegalArgumentException("Correlated scan must allow at least one pending fetch.");
		if(inputAccess.size() != inputs.size() || inputAccess.get(0) != InputAccess.ROW_ALIGNED)
			throw new IllegalArgumentException(
				"Correlated scan requires one access mode per input and a row-aligned anchor.");
		_inputs = List.copyOf(inputs);
		_inputAccess = List.copyOf(inputAccess);
		_derive = derive;
		_combine = combine;
		_outputSize = outputSize;
		_workspaceBytes = workspaceBytes;
		_outputBytesPerGroup = outputBytesPerGroup;
		_fetchSlots = new Semaphore(maxPendingFetches);
		_outputs = List.copyOf(outputs);
		_pendingStores = new AtomicInteger(inputs.size());
		_stores = new MaterializedStore[inputs.size()];
		_readers = new IndexedMaterializedStoreReader[inputs.size()];
	}

	private static List<InputAccess> defaultInputAccess(int inputs) {
		List<InputAccess> access = new ArrayList<>(inputs);
		for(int i = 0; i < inputs; i++)
			access.add(i == 0 ? InputAccess.ROW_ALIGNED : InputAccess.FULL);
		return access;
	}

	public record Output<O>(int stream, O value) {
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		long colBlocks = OOCUtils.getNumColBlocks(_inputs.get(0).getDataCharacteristics());
		List<OOCMaterializedInputRequest> requests = new ArrayList<>(_inputs.size());
		requests.add(new OOCMaterializedInputRequest(0, OOCStoreLayout.ROW_MAJOR, 1,
			(row, col) -> (row - 1) * colBlocks + col - 1));
		for(int i = 1; i < _inputs.size(); i++) {
			long sideColBlocks = OOCUtils.getNumColBlocks(_inputs.get(i).getDataCharacteristics());
			requests.add(_inputAccess.get(i) == InputAccess.ROW_ALIGNED ? new OOCMaterializedInputRequest(i,
				OOCStoreLayout.ROW_MAJOR, 1,
				(row, col) -> (row - 1) * sideColBlocks + col - 1) : new OOCMaterializedInputRequest(i,
					OOCStoreLayout.ROW_MAJOR, 1));
		}
		return requests;
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
	public long getMaxTaskReservationBytes(IndexedMatrixValue... inputs) {
		DataCharacteristics dc = _inputs.get(0).getDataCharacteristics();
		return dc == null || !dc.dimsKnown() || dc.getBlocksize() <= 0 ? 0 : taskBytes(dc);
	}

	private long taskBytes(DataCharacteristics dc) {
		long pinned = OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(OOCUtils.estimateFullTileBytes(dc)) *
			dc.getNumColBlocks();
		for(int i = 1; i < _inputs.size(); i++) {
			DataCharacteristics side = _inputs.get(i).getDataCharacteristics();
			if(side == null || !side.dimsKnown() || side.getBlocksize() <= 0)
				throw new DMLRuntimeException("Correlated scan requires known side-input dimensions and block size.");
			long blocks = _inputAccess.get(i) == InputAccess.ROW_ALIGNED ? side.getNumColBlocks() : OOCUtils
				.getNumBlocks(side);
			pinned += OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(OOCUtils.estimateFullTileBytes(side)) *
				blocks;
		}
		return pinned + _workspaceBytes + _outputBytesPerGroup;
	}

	@Override
	protected void startExecution() {
		DataCharacteristics dc = _inputs.get(0).getDataCharacteristics();
		if(dc == null || !dc.dimsKnown() || dc.getBlocksize() <= 0)
			throw new DMLRuntimeException("Correlated scan requires known input dimensions and block size.");
		_rowBlocks = Math.toIntExact(dc.getNumRowBlocks());
		_colBlocks = Math.toIntExact(dc.getNumColBlocks());
		if(_rowBlocks <= 0 || _colBlocks <= 0)
			throw new DMLRuntimeException("Correlated scan requires non-empty input block geometry.");
		for(int i = 1; i < _inputs.size(); i++) {
			DataCharacteristics side = _inputs.get(i).getDataCharacteristics();
			if(_inputAccess.get(i) == InputAccess.ROW_ALIGNED && side.getNumRowBlocks() != _rowBlocks)
				throw new DMLRuntimeException(
					"Row-aligned correlated-scan inputs require matching row-block geometry.");
		}
		_taskBytes = taskBytes(dc);
		_outputStreams = _outputs.stream().map(OOCStreamable::getWriteStream).toList();
		_ready = new SubscribableTaskQueue<>();
		getContext().addOutStream(_ready);
		for(OOCStream<O> output : _outputStreams)
			getContext().addOutStream(output);
		OOCInstructionUtils.submitCloseableOOCTasks(_ready, this::process, getContext())
			.whenComplete((ignored, error) -> {
				try {
					if(error != null)
						fail(error);
					for(OOCStream<O> output : _outputStreams)
						output.closeInput();
				}
				catch(Throwable failure) {
					fail(failure);
				}
				finally {
					cleanup();
				}
			});

		for(int i = 0; i < _inputs.size(); i++) {
			int input = i;
			getMaterializedInput(i).whenComplete((store, error) -> prepareReader(input, store, error));
		}
	}

	private void prepareReader(int input, MaterializedStore<IndexedMatrixValue> store, Throwable error) {
		if(error != null) {
			failStartup(error);
			return;
		}
		_stores[input] = store;
		store.completion().whenComplete((ignored, completionError) -> {
			if(completionError != null) {
				failStartup(completionError);
				return;
			}
			try {
				long blocks = OOCUtils.getNumBlocks(_inputs.get(input).getDataCharacteristics());
				int uses = _inputAccess.get(input) == InputAccess.ROW_ALIGNED ? 1 : _rowBlocks;
				_readers[input] = store.openIndexedReader(new CountingLiveness(Math.toIntExact(blocks), uses));
				if(_pendingStores.decrementAndGet() == 0 && _startupFinished.compareAndSet(false, true))
					OOCInstructionUtils.submitOOCTask(this::drive, new StreamContext());
			}
			catch(Throwable failure) {
				failStartup(failure);
			}
		});
	}

	private void failStartup(Throwable error) {
		fail(error);
		if(_startupFinished.compareAndSet(false, true))
			completeOne();
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
		List<OOCFuture<StoreLease<IndexedMatrixValue>>> requests = new ArrayList<>();
		List<Integer> widths = new ArrayList<>(_inputs.size());
		try {
			for(int col = 0; col < _colBlocks; col++)
				requests.add(_readers[0].request(group + 1L, col + 1L, budget));
			widths.add(_colBlocks);
			for(int input = 1; input < _inputs.size(); input++) {
				DataCharacteristics dc = _inputs.get(input).getDataCharacteristics();
				boolean rowAligned = _inputAccess.get(input) == InputAccess.ROW_ALIGNED;
				int width = Math.toIntExact(rowAligned ? dc.getNumColBlocks() : OOCUtils.getNumBlocks(dc));
				widths.add(width);
				long firstRow = rowAligned ? group + 1L : 1L;
				long lastRow = rowAligned ? firstRow : dc.getNumRowBlocks();
				for(long row = firstRow; row <= lastRow; row++)
					for(long col = 1; col <= dc.getNumColBlocks(); col++)
						requests.add(_readers[input].request(row, col, budget));
			}
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
				_ready.enqueue(new GroupWork(leases, widths, budget));
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
		List<PendingOutput<O>> callbacks = new ArrayList<>();
		boolean workspaceReserved = false;
		try {
			if(!budget.tryReserve(_workspaceBytes))
				throw new DMLRuntimeException("Correlated-scan workspace exceeds its admitted task budget.");
			workspaceReserved = true;
			List<IndexedMatrixValue> anchor = work.values(0);
			List<List<IndexedMatrixValue>> sides = new ArrayList<>(_inputs.size() - 1);
			for(int input = 1; input < _inputs.size(); input++)
				sides.add(work.values(input));
			D derived = _derive.apply(anchor, sides);
			List<Output<O>> outputs = _combine.apply(anchor, derived);
			if(outputs == null)
				throw new DMLRuntimeException("Correlated scan produced a null output list.");
			for(Output<O> output : outputs) {
				if(output == null || output.value() == null)
					throw new DMLRuntimeException("Correlated scan produced a null output.");
				if(output.stream() < 0 || output.stream() >= _outputStreams.size())
					throw new DMLRuntimeException("Invalid correlated-scan output stream " + output.stream());
				long bytes = _outputSize.applyAsLong(output.value());
				if(!budget.tryReserve(bytes))
					throw new DMLRuntimeException("Correlated-scan output exceeds its admitted task budget.");
				callbacks.add(new PendingOutput<>(output.stream(),
					new InMemoryQueueCallback<>(output.value(), null, budget, bytes)));
			}
			budget.release(_workspaceBytes);
			workspaceReserved = false;
			work.releaseLeases();
			budget.close();
			for(int i = 0; i < callbacks.size(); i++) {
				PendingOutput<O> output = callbacks.get(i);
				_outputStreams.get(output.stream()).enqueue(output.callback());
				callbacks.set(i, null);
			}
		}
		catch(Throwable failure) {
			fail(failure);
		}
		finally {
			for(PendingOutput<O> output : callbacks)
				if(output != null)
					output.callback().close();
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
			for(IndexedMaterializedStoreReader<IndexedMatrixValue> reader : _readers)
				if(reader != null)
					reader.close();
		}
		finally {
			for(MaterializedStore<IndexedMatrixValue> store : _stores)
				if(store != null)
					store.close();
			onComplete();
		}
	}

	private static void closeLease(StoreLease<IndexedMatrixValue> lease) {
		if(lease != null)
			lease.close();
	}

	private record PendingOutput<O>(int stream, OOCStream.QueueCallback<O> callback) {
	}

	private static final class GroupWork implements AutoCloseable {
		private List<StoreLease<IndexedMatrixValue>> _leases;
		private final List<Integer> _widths;
		private ReservationBudget _budget;

		private GroupWork(List<StoreLease<IndexedMatrixValue>> leases, List<Integer> widths, ReservationBudget budget) {
			_leases = leases;
			_widths = widths;
			_budget = budget;
		}

		private List<IndexedMatrixValue> values(int input) {
			int offset = 0;
			for(int i = 0; i < input; i++)
				offset += _widths.get(i);
			List<IndexedMatrixValue> values = new ArrayList<>(_widths.get(input));
			for(int i = 0; i < _widths.get(input); i++)
				values.add(_leases.get(offset + i).value());
			return values;
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
