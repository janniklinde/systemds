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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiFunction;
import java.util.function.Function;

public class GroupedReduceOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _inputStreamable;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final Grouping _grouping;
	private final int _accumulatorsPerGroup;
	private final Function<IndexedMatrixValue, MatrixBlock> _partialFn;
	private final BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> _mergeFn;
	private final Function<MatrixBlock, MatrixBlock> _finalizeFn;
	@SuppressWarnings("unused")
	private final StreamContext _sc;
	private OperatorStateTable<IndexedMatrixValue> _table;

	private GroupedReduceOOCPrimitive(OOCPrimitive inputPrimitive, OOCStreamable<IndexedMatrixValue> inputStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, Grouping grouping, int accumulatorsPerGroup,
		Function<IndexedMatrixValue, MatrixBlock> partialFn, BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn,
		Function<MatrixBlock, MatrixBlock> finalizeFn, StreamContext sc) {
		super(inputPrimitive == null ? List.of() : List.of(inputPrimitive));
		if(accumulatorsPerGroup <= 0)
			throw new IllegalArgumentException("Number of accumulators per group must be positive.");
		_inputStreamable = reserveLazyHandle(inputStreamable);
		_outputStreamable = outputStreamable;
		_grouping = grouping;
		_accumulatorsPerGroup = accumulatorsPerGroup;
		_partialFn = partialFn;
		_mergeFn = mergeFn;
		_finalizeFn = finalizeFn;
		_sc = sc;
	}

	public GroupedReduceOOCPrimitive(OOCStreamable<IndexedMatrixValue> inputStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, Grouping grouping, int accumulatorsPerGroup,
		Function<MatrixBlock, MatrixBlock> partialFn, BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn,
		Function<MatrixBlock, MatrixBlock> finalizeFn, StreamContext sc) {
		this(safePrimitive(inputStreamable), inputStreamable, outputStreamable,
			grouping, accumulatorsPerGroup, imv -> partialFn.apply((MatrixBlock) imv.getValue()),
			mergeFn, finalizeFn, sc);
	}

	public static GroupedReduceOOCPrimitive indexedPartial(OOCStreamable<IndexedMatrixValue> inputStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, Grouping grouping, int accumulatorsPerGroup,
		Function<IndexedMatrixValue, MatrixBlock> partialFn,
		BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn,
		Function<MatrixBlock, MatrixBlock> finalizeFn, StreamContext sc) {
		return new GroupedReduceOOCPrimitive(safePrimitive(inputStreamable), inputStreamable, outputStreamable,
			grouping, accumulatorsPerGroup, partialFn, mergeFn, finalizeFn, sc);
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return List.of(_inputStreamable);
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStreamable);
	}

	@Override
	public boolean isMaterializationBoundary() {
		return true;
	}

	@Override
	public boolean requiresCache() {
		return false;
	}

	@Override
	public boolean requiresStateTable() {
		return true;
	}

	@Override
	public void bindStateTable(OperatorStateTable<IndexedMatrixValue> table) {
		_table = table;
	}

	@Override
	public void onComplete() {
		try {
			if(_table != null)
				_table.close();
		}
		finally {
			super.onComplete();
		}
	}

	@Override
	public long getDenseTileMemoryFactor() {
		return 2;
	}

	@Override
	public void inferPatterns() {
		if(_pattern == OOCAccessPattern.UNSET)
			_pattern = OOCAccessPattern.ANY;
		requestPreferredInputPattern();
		inferPatterns(getParents());
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = _pattern.preferred(accessPattern);
		requestPreferredInputPattern();
	}

	@Override
	public void startExecution() {
		final OOCStream<IndexedMatrixValue> in = _inputStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		final DataCharacteristics dc = _inputStreamable.getDataCharacteristics();
		final int numGroups = _grouping.numGroups(dc);
		final int groupSize = _grouping.groupSize(dc);
		final AtomicIntegerArray consumedPerGroup = new AtomicIntegerArray(numGroups);
		final AtomicIntegerArray emittedPerGroup = new AtomicIntegerArray(numGroups);
		final OOCStream<GroupedWork> work = new SubscribableTaskQueue<>();
		final WorkCoordinator coordinator =
			new WorkCoordinator(work, out, consumedPerGroup, emittedPerGroup, groupSize);

		OOCInstructionUtils.submitOOCTasks(work, cb -> processWork(cb.get(), coordinator), _sc)
			.thenRun(out::closeInput).exceptionally(t -> {
				out.propagateFailure(DMLRuntimeException.of(t));
				return null;
			}).thenRun(() -> out.getPrimitive().onComplete());

		in.setSubscriber(cb -> coordinator.acceptInput(cb), _allowance, _allocFn);
	}

	private void requestPreferredInputPattern() {
		if(!getChildren().isEmpty() && !getChildren().get(0).hasStartedExecution())
			getChildren().get(0).requestPattern(_grouping.preferredInputPattern());
	}

	/**
	 * Input, memory admission, and state-table futures are handled outside the compute pool. Workers
	 * receive only admitted work items and never block on OOC futures or reserveBlocking.
	 */
	private void processWork(GroupedWork work, WorkCoordinator coordinator) {
		work.process(coordinator);
	}

	private ManagedPayload<IndexedMatrixValue> payload(MatrixIndexes index, MatrixBlock block, long bytes) {
		return new ManagedPayload<>(new IndexedMatrixValue(index, block), bytes, _allowance);
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> outputCallback(MatrixIndexes index, MatrixBlock block,
		long bytes, boolean reserved) {
		IndexedMatrixValue imv = new IndexedMatrixValue(index, block);
		if(_crossBoundaries)
			return new InMemoryQueueCallback(imv, null, _allowance, reserved ? bytes : 0);
		if(reserved)
			_allowance.release(bytes);
		return new OOCStream.SimpleQueueCallback<>(imv, null);
	}

	private final class WorkCoordinator {
		private final OOCStream<GroupedWork> _work;
		private final OOCStream<IndexedMatrixValue> _out;
		private final AtomicIntegerArray _consumedPerGroup;
		private final AtomicIntegerArray _emittedPerGroup;
		private final int _groupSize;
		private final AtomicInteger _pending = new AtomicInteger(1);
		private final AtomicBoolean _closed = new AtomicBoolean(false);
		private final AtomicBoolean _failed = new AtomicBoolean(false);

		private WorkCoordinator(OOCStream<GroupedWork> work, OOCStream<IndexedMatrixValue> out,
			AtomicIntegerArray consumedPerGroup, AtomicIntegerArray emittedPerGroup, int groupSize) {
			_work = work;
			_out = out;
			_consumedPerGroup = consumedPerGroup;
			_emittedPerGroup = emittedPerGroup;
			_groupSize = groupSize;
		}

		private void acceptInput(OOCStream.QueueCallback<IndexedMatrixValue> cb) {
			try(cb) {
				if(cb.isEos()) {
					releasePending();
					return;
				}
				IndexedMatrixValue input = cb.get();
				MatrixIndexes inputIndex = input.getIndexes();
				int group = group(inputIndex);
				MatrixIndexes outputIndex = outputIndex(group);
				long bytes = _allocFn.applyAsLong(outputIndex);
				_allowance.reserveBlocking(bytes);
				if(_groupSize == 1)
					enqueue(new SingleGroupWork(cb.keepOpen(), outputIndex, bytes));
				else
					enqueue(new PartialWork(cb.keepOpen(), group, accumulatorSlot(inputIndex), outputIndex, bytes));
			}
			catch(Throwable t) {
				fail(t);
			}
		}

		private void submitPayload(int group, int slot, MatrixIndexes index,
			ManagedPayload<IndexedMatrixValue> candidate) {
			retainPending();
			_table.installOrTake(slot, candidate).whenComplete((existing, error) -> {
				try {
					if(error != null) {
						candidate.release();
						fail(error);
						return;
					}
					if(existing == null) {
						onAccumulated(group);
						return;
					}
					long bytes = _allocFn.applyAsLong(index);
					_allowance.reserveBlocking(bytes);
					try {
						enqueue(new MergeWork(existing, candidate, group, slot, index, bytes));
					}
					catch(Throwable t) {
						existing.close();
						candidate.release();
						_allowance.release(bytes);
						throw t;
					}
				}
				catch(Throwable t) {
					fail(t);
				}
				finally {
					releasePending();
				}
			});
		}

		private void onAccumulated(int group) {
			if(_consumedPerGroup.incrementAndGet(group) == _groupSize &&
				_emittedPerGroup.compareAndSet(group, 0, 1))
				submitFinalize(group);
		}

		private void submitFinalize(int group) {
			FinalizeCollector collector = new FinalizeCollector(this, outputIndex(group), _accumulatorsPerGroup);
			for(int stripe = 0; stripe < _accumulatorsPerGroup; stripe++) {
				retainPending();
				_table.take(accumulatorSlot(group, stripe)).whenComplete((lease, error) -> {
					try {
						if(error != null) {
							if(lease != null)
								lease.close();
							fail(error);
							return;
						}
						collector.add(lease);
					}
					catch(Throwable t) {
						fail(t);
					}
					finally {
						releasePending();
					}
				});
			}
		}

		private void enqueue(GroupedWork work) {
			if(_failed.get()) {
				work.close();
				return;
			}
			retainPending();
			try {
				_work.enqueue(work);
			}
			catch(Throwable t) {
				work.close();
				releasePending();
				throw t;
			}
		}

		private void finishWork() {
			releasePending();
		}

		private void retainPending() {
			_pending.incrementAndGet();
		}

		private void releasePending() {
			if(_pending.decrementAndGet() == 0 && _closed.compareAndSet(false, true))
				_work.closeInput();
		}

		private void fail(Throwable t) {
			if(!_failed.compareAndSet(false, true))
				return;
			DMLRuntimeException re = DMLRuntimeException.of(t);
			_work.propagateFailure(re);
			_out.propagateFailure(re);
			if(_sc != null)
				_sc.failAll(re);
		}
	}

	private interface GroupedWork {
		void process(WorkCoordinator coordinator);

		default void close() {
		}
	}

	private final class SingleGroupWork implements GroupedWork {
		private final OOCStream.QueueCallback<IndexedMatrixValue> _input;
		private final MatrixIndexes _outputIndex;
		private final long _bytes;

		private SingleGroupWork(OOCStream.QueueCallback<IndexedMatrixValue> input, MatrixIndexes outputIndex,
			long bytes) {
			_input = input;
			_outputIndex = outputIndex;
			_bytes = bytes;
		}

		@Override
		public void process(WorkCoordinator coordinator) {
			try(_input) {
				MatrixBlock result = _finalizeFn.apply(_partialFn.apply(_input.get()));
				coordinator._out.enqueue(outputCallback(_outputIndex, result, _bytes, true));
			}
			finally {
				coordinator.finishWork();
			}
		}

		@Override
		public void close() {
			_input.close();
			_allowance.release(_bytes);
		}
	}

	private final class PartialWork implements GroupedWork {
		private final OOCStream.QueueCallback<IndexedMatrixValue> _input;
		private final int _group;
		private final int _slot;
		private final MatrixIndexes _outputIndex;
		private final long _bytes;

		private PartialWork(OOCStream.QueueCallback<IndexedMatrixValue> input, int group, int slot,
			MatrixIndexes outputIndex, long bytes) {
			_input = input;
			_group = group;
			_slot = slot;
			_outputIndex = outputIndex;
			_bytes = bytes;
		}

		@Override
		public void process(WorkCoordinator coordinator) {
			boolean submitted = false;
			try(_input) {
				MatrixBlock partial = _partialFn.apply(_input.get());
				ManagedPayload<IndexedMatrixValue> candidate = payload(_outputIndex, partial, _bytes);
				coordinator.submitPayload(_group, _slot, _outputIndex, candidate);
				submitted = true;
			}
			finally {
				if(!submitted)
					_allowance.release(_bytes);
				coordinator.finishWork();
			}
		}

		@Override
		public void close() {
			_input.close();
			_allowance.release(_bytes);
		}
	}

	private final class MergeWork implements GroupedWork {
		private final OperatorStateTable.StateLease<IndexedMatrixValue> _existing;
		private final ManagedPayload<IndexedMatrixValue> _candidate;
		private final int _group;
		private final int _slot;
		private final MatrixIndexes _index;
		private final long _bytes;

		private MergeWork(OperatorStateTable.StateLease<IndexedMatrixValue> existing,
			ManagedPayload<IndexedMatrixValue> candidate, int group, int slot, MatrixIndexes index, long bytes) {
			_existing = existing;
			_candidate = candidate;
			_group = group;
			_slot = slot;
			_index = index;
			_bytes = bytes;
		}

		@Override
		public void process(WorkCoordinator coordinator) {
			boolean submitted = false;
			try(_existing) {
				MatrixBlock merged = _mergeFn.apply((MatrixBlock) _existing.value().getValue(),
					(MatrixBlock) _candidate.value().getValue());
				_candidate.release();
				ManagedPayload<IndexedMatrixValue> next = payload(_index, merged, _bytes);
				coordinator.submitPayload(_group, _slot, _index, next);
				submitted = true;
			}
			finally {
				if(!submitted)
					_allowance.release(_bytes);
				_candidate.release();
				coordinator.finishWork();
			}
		}

		@Override
		public void close() {
			_existing.close();
			_candidate.release();
			_allowance.release(_bytes);
		}
	}

	private final class FinalizeCollector {
		private final WorkCoordinator _coordinator;
		private final MatrixIndexes _outputIndex;
		private final AtomicInteger _remaining;
		private final List<OperatorStateTable.StateLease<IndexedMatrixValue>> _leases =
			Collections.synchronizedList(new ArrayList<>());

		private FinalizeCollector(WorkCoordinator coordinator, MatrixIndexes outputIndex, int remaining) {
			_coordinator = coordinator;
			_outputIndex = outputIndex;
			_remaining = new AtomicInteger(remaining);
		}

		private void add(OperatorStateTable.StateLease<IndexedMatrixValue> lease) {
			if(lease != null)
				_leases.add(lease);
			if(_remaining.decrementAndGet() != 0)
				return;
			if(_leases.isEmpty())
				return;
			long bytes = _startsRegion ? _allocFn.applyAsLong(_outputIndex) : 0;
			if(bytes > 0)
				_allowance.reserveBlocking(bytes);
			try {
				_coordinator.enqueue(new FinalizeWork(new ArrayList<>(_leases), _outputIndex, bytes));
			}
			catch(Throwable t) {
				for(OperatorStateTable.StateLease<IndexedMatrixValue> current : _leases)
					current.close();
				if(bytes > 0)
					_allowance.release(bytes);
				throw t;
			}
		}
	}

	private final class FinalizeWork implements GroupedWork {
		private final List<OperatorStateTable.StateLease<IndexedMatrixValue>> _leases;
		private final MatrixIndexes _outputIndex;
		private final long _bytes;

		private FinalizeWork(List<OperatorStateTable.StateLease<IndexedMatrixValue>> leases,
			MatrixIndexes outputIndex, long bytes) {
			_leases = leases;
			_outputIndex = outputIndex;
			_bytes = bytes;
		}

		@Override
		public void process(WorkCoordinator coordinator) {
			boolean outputSubmitted = false;
			try {
				MatrixBlock accumulator = null;
				for(OperatorStateTable.StateLease<IndexedMatrixValue> current : _leases) {
					try(current) {
						MatrixBlock value = (MatrixBlock) current.value().getValue();
						accumulator = accumulator == null ? value : _mergeFn.apply(accumulator, value);
					}
				}
				if(accumulator != null) {
					MatrixBlock result = _finalizeFn.apply(accumulator);
					coordinator._out.enqueue(outputCallback(_outputIndex, result, _bytes, _bytes > 0));
					outputSubmitted = true;
				}
			}
			finally {
				if(!outputSubmitted && _bytes > 0)
					_allowance.release(_bytes);
				coordinator.finishWork();
			}
		}

		@Override
		public void close() {
			for(OperatorStateTable.StateLease<IndexedMatrixValue> current : _leases)
				current.close();
			if(_bytes > 0)
				_allowance.release(_bytes);
		}
	}

	public Grouping getGrouping() {
		return _grouping;
	}

	public int getAccumulatorsPerGroup() {
		return _accumulatorsPerGroup;
	}

	public Function<IndexedMatrixValue, MatrixBlock> getPartialFn() {
		return _partialFn;
	}

	public BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> getMergeFn() {
		return _mergeFn;
	}

	public Function<MatrixBlock, MatrixBlock> getFinalizeFn() {
		return _finalizeFn;
	}

	public int getNumAccumulatorSlots() {
		return _grouping.numAccumulatorSlots(_inputStreamable.getDataCharacteristics(), _accumulatorsPerGroup);
	}

	public int accumulatorSlot(MatrixIndexes ix) {
		return _grouping.accumulatorSlot(ix, _inputStreamable.getDataCharacteristics(), _accumulatorsPerGroup);
	}

	public int accumulatorSlot(int group, int stripe) {
		return Math.toIntExact((long) group * _accumulatorsPerGroup + stripe);
	}

	public int group(MatrixIndexes ix) {
		return _grouping.group(ix, _inputStreamable.getDataCharacteristics());
	}

	public int stripe(MatrixIndexes ix) {
		return _grouping.stripe(ix, _inputStreamable.getDataCharacteristics(), _accumulatorsPerGroup);
	}

	public MatrixIndexes outputIndex(int group) {
		return _grouping.outputIndex(group);
	}

	public enum Grouping {
		ROW_BLOCKS {
			@Override
			public OOCAccessPattern preferredInputPattern() {
				return OOCAccessPattern.ROW_MAJOR;
			}

			@Override
			public int numGroups(DataCharacteristics dc) {
				validate(dc);
				return Math.toIntExact(dc.getNumRowBlocks());
			}

			@Override
			public int group(MatrixIndexes ix, DataCharacteristics dc) {
				validate(dc);
				return Math.toIntExact(ix.getRowIndex() - 1);
			}

			@Override
			public int stripe(MatrixIndexes ix, DataCharacteristics dc, int accumulatorsPerGroup) {
				validate(dc);
				return Math.toIntExact((ix.getColumnIndex() - 1) % accumulatorsPerGroup);
			}

			@Override
			public int groupSize(DataCharacteristics dc) {
				validate(dc);
				return Math.toIntExact(dc.getNumColBlocks());
			}

			@Override
			public MatrixIndexes outputIndex(int group) {
				return new MatrixIndexes(group + 1L, 1);
			}
		},

		COL_BLOCKS {
			@Override
			public OOCAccessPattern preferredInputPattern() {
				return OOCAccessPattern.COL_MAJOR;
			}

			@Override
			public int numGroups(DataCharacteristics dc) {
				validate(dc);
				return Math.toIntExact(dc.getNumColBlocks());
			}

			@Override
			public int group(MatrixIndexes ix, DataCharacteristics dc) {
				validate(dc);
				return Math.toIntExact(ix.getColumnIndex() - 1);
			}

			@Override
			public int stripe(MatrixIndexes ix, DataCharacteristics dc, int accumulatorsPerGroup) {
				validate(dc);
				return Math.toIntExact((ix.getRowIndex() - 1) % accumulatorsPerGroup);
			}

			@Override
			public int groupSize(DataCharacteristics dc) {
				validate(dc);
				return Math.toIntExact(dc.getNumRowBlocks());
			}

			@Override
			public MatrixIndexes outputIndex(int group) {
				return new MatrixIndexes(1, group + 1L);
			}
		},

		SINGLE {
			@Override
			public OOCAccessPattern preferredInputPattern() {
				return OOCAccessPattern.ROW_MAJOR;
			}

			@Override
			public int numGroups(DataCharacteristics dc) {
				validate(dc);
				return 1;
			}

			@Override
			public int group(MatrixIndexes ix, DataCharacteristics dc) {
				validate(dc);
				return 0;
			}

			@Override
			public int stripe(MatrixIndexes ix, DataCharacteristics dc, int accumulatorsPerGroup) {
				validate(dc);
				long colBlocks = dc.getNumColBlocks();
				long linear = (ix.getRowIndex() - 1) * colBlocks + ix.getColumnIndex() - 1;
				return Math.toIntExact(linear % accumulatorsPerGroup);
			}

			@Override
			public int groupSize(DataCharacteristics dc) {
				validate(dc);
				return Math.toIntExact(dc.getNumBlocks());
			}

			@Override
			public MatrixIndexes outputIndex(int group) {
				if(group != 0)
					throw new IllegalArgumentException("Single-group reduce only supports group 0.");
				return new MatrixIndexes(1, 1);
			}
		};

		public abstract OOCAccessPattern preferredInputPattern();
		public abstract int numGroups(DataCharacteristics dc);
		public abstract int group(MatrixIndexes ix, DataCharacteristics dc);
		public abstract int stripe(MatrixIndexes ix, DataCharacteristics dc, int accumulatorsPerGroup);
		public abstract int groupSize(DataCharacteristics dc);
		public abstract MatrixIndexes outputIndex(int group);

		public int accumulatorSlot(MatrixIndexes ix, DataCharacteristics dc, int accumulatorsPerGroup) {
			int group = group(ix, dc);
			int stripe = stripe(ix, dc, accumulatorsPerGroup);
			return Math.toIntExact((long) group * accumulatorsPerGroup + stripe);
		}

		public int numAccumulatorSlots(DataCharacteristics dc, int accumulatorsPerGroup) {
			return Math.toIntExact((long) numGroups(dc) * accumulatorsPerGroup);
		}

		private static void validate(DataCharacteristics dc) {
			if(dc == null || !dc.dimsKnown() || dc.getBlocksize() <= 0)
				throw new DMLRuntimeException("Grouped OOC reduce requires known matrix dimensions and block size.");
		}
	}
}
