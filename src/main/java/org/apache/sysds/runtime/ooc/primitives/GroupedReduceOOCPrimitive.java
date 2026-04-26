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
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiFunction;
import java.util.function.Function;

public class GroupedReduceOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _inputStreamable;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final Grouping _grouping;
	private final int _accumulatorsPerGroup;
	private final Function<MatrixBlock, MatrixBlock> _partialFn;
	private final BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> _mergeFn;
	private final Function<MatrixBlock, MatrixBlock> _finalizeFn;
	@SuppressWarnings("unused")
	private final StreamContext _sc;
	private CachedAllowance _cache;

	private GroupedReduceOOCPrimitive(OOCPrimitive inputPrimitive, OOCStreamable<IndexedMatrixValue> inputStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, Grouping grouping, int accumulatorsPerGroup,
		Function<MatrixBlock, MatrixBlock> partialFn, BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn,
		Function<MatrixBlock, MatrixBlock> finalizeFn, StreamContext sc) {
		super(inputPrimitive == null ? List.of() : List.of(inputPrimitive));
		if(accumulatorsPerGroup <= 0)
			throw new IllegalArgumentException("Number of accumulators per group must be positive.");
		_inputStreamable = inputStreamable;
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
		this(inputStreamable == null ? null : inputStreamable.getPrimitive(), inputStreamable, outputStreamable,
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
		return true;
	}

	@Override
	public void bindCache(CachedAllowance cache) {
		_cache = cache;
	}

	@Override
	public void onComplete() {
		try {
			if(_cache != null)
				_cache.shutdown();
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
		getParents().forEach(OOCPrimitive::inferPatterns);
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

		OOCInstructionUtils.submitOOCTasks(in, cb -> {
			IndexedMatrixValue input = cb.get();
			MatrixIndexes inputIndex = input.getIndexes();
			int group = group(inputIndex);
			int slot = accumulatorSlot(inputIndex);
			MatrixIndexes outputIndex = outputIndex(group);
			MatrixBlock partial = _partialFn.apply((MatrixBlock) input.getValue());

			mergeIntoAccumulator(slot, newTrackedCallback(outputIndex, partial));

			if(consumedPerGroup.incrementAndGet(group) == groupSize &&
				emittedPerGroup.compareAndSet(group, 0, 1)) {
				finalizeGroup(group, out);
			}
		}, _sc).thenRun(out::closeInput).exceptionally(t -> {
			out.propagateFailure(DMLRuntimeException.of(t));
			return null;
		}).thenRun(() -> out.getPrimitive().onComplete());
	}

	private void requestPreferredInputPattern() {
		if(!getChildren().isEmpty())
			getChildren().get(0).requestPattern(_grouping.preferredInputPattern());
	}

	private void mergeIntoAccumulator(int slot, OOCStream.QueueCallback<IndexedMatrixValue> initialCandidate) {
		OOCStream.QueueCallback<IndexedMatrixValue> candidate = initialCandidate;
		try {
			while(candidate != null) {
				OOCStream.QueueCallback<IndexedMatrixValue> existing = _cache.handoverOrTakeExisting(candidate, slot)
					.join();
				if(existing == null) {
					candidate = null;
					return;
				}

				OOCStream.QueueCallback<IndexedMatrixValue> merged;
				try(existing) {
					merged = mergeCallbacks(existing, candidate);
				}
				finally {
					candidate.close();
				}
				candidate = merged;
			}
		}
		finally {
			if(candidate != null)
				candidate.close();
		}
	}

	private void finalizeGroup(int group, OOCStream<IndexedMatrixValue> out) {
		MatrixIndexes outputIndex = outputIndex(group);
		OOCStream.QueueCallback<IndexedMatrixValue> accumulator = null;
		try {
			for(int stripe = 0; stripe < _accumulatorsPerGroup; stripe++) {
				OOCStream.QueueCallback<IndexedMatrixValue> current = _cache.take(accumulatorSlot(group, stripe))
					.join();
				if(current == null)
					continue;
				if(accumulator == null) {
					accumulator = current;
					continue;
				}

				OOCStream.QueueCallback<IndexedMatrixValue> merged;
				try(current) {
					merged = mergeCallbacks(accumulator, current);
				}
				finally {
					accumulator.close();
				}
				accumulator = merged;
			}

			if(accumulator == null)
				return;

			MatrixBlock result = _finalizeFn.apply((MatrixBlock) accumulator.get().getValue());
			out.enqueue(outputCallback(outputIndex, result));
		}
		finally {
			if(accumulator != null)
				accumulator.close();
		}
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> mergeCallbacks(
		OOCStream.QueueCallback<IndexedMatrixValue> left, OOCStream.QueueCallback<IndexedMatrixValue> right) {
		MatrixIndexes outputIndex = right.get().getIndexes();
		MatrixBlock merged = _mergeFn.apply((MatrixBlock) left.get().getValue(), (MatrixBlock) right.get().getValue());
		return newTrackedCallback(outputIndex, merged);
	}

	private InMemoryQueueCallback newTrackedCallback(MatrixIndexes index, MatrixBlock block) {
		long bytes = _allocFn.applyAsLong(index);
		_allowance.reserveBlocking(bytes);
		return new InMemoryQueueCallback(new IndexedMatrixValue(index, block), null, _allowance, bytes);
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> outputCallback(MatrixIndexes index, MatrixBlock block) {
		IndexedMatrixValue imv = new IndexedMatrixValue(index, block);
		if(_crossBoundaries) {
			long bytes = _allocFn.applyAsLong(index);
			_allowance.reserveBlocking(bytes);
			return new InMemoryQueueCallback(imv, null, _allowance, bytes);
		}
		return new OOCStream.SimpleQueueCallback<>(imv, null);
	}

	public Grouping getGrouping() {
		return _grouping;
	}

	public int getAccumulatorsPerGroup() {
		return _accumulatorsPerGroup;
	}

	public Function<MatrixBlock, MatrixBlock> getPartialFn() {
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
