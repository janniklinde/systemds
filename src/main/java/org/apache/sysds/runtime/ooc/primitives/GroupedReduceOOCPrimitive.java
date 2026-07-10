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
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.StateLease;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
	private StateTable<IndexedMatrixValue> _table;

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
		_table = new StateTable<>(OOCCacheManager.getGlobalCache(), CachingStream._streamSeq.getNextID());
		final OOCStream<IndexedMatrixValue> in = _inputStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		final DataCharacteristics dc = _inputStreamable.getDataCharacteristics();
		final DataCharacteristics outputDc = _outputStreamable.getDataCharacteristics();
		final long reduceTaskBudgetBytes = reduceTaskBudgetBytes(dc, outputDc);
		final long finalizeTaskBudgetBytes = finalizeTaskBudgetBytes(dc, outputDc);
		final OOCStream<IndexedMatrixValue> admitted =
			new AllocatedOOCStream<>(in, _allowance, input -> reduceTaskBudgetBytes, reduceTaskBudgetBytes > 0,
				ReservationBudget::admitted);
		final OOCStream<Integer> finalizeTasks = new SubscribableTaskQueue<>();
		final OOCStream<Integer> admittedFinalizers =
			new AllocatedOOCStream<>(finalizeTasks, _allowance, group -> finalizeTaskBudgetBytes,
				finalizeTaskBudgetBytes > 0, ReservationBudget::admitted);
		final int numGroups = _grouping.numGroups(dc);
		final int groupSize = _grouping.groupSize(dc);
		final AtomicIntegerArray consumedPerGroup = new AtomicIntegerArray(numGroups);
		final Object[] groupLocks = new Object[numGroups];
		for(int group = 0; group < numGroups; group++)
			groupLocks[group] = new Object();
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final Consumer<Throwable> fail = t -> {
			if(failure.compareAndSet(null, t)) {
				DMLRuntimeException re = DMLRuntimeException.of(t);
				out.propagateFailure(re);
				if(_sc != null)
					_sc.failAll(re);
			}
		};

		OOCInstructionUtils.submitOOCTasks(admittedFinalizers, cb -> {
			ReservationBudget budget = OOCInstructionUtils.detachBudget(cb);
			try {
				if(budget == null)
					throw new DMLRuntimeException("Missing admitted grouped-reduce finalization budget.");
				finalizeGroup(cb.get(), out, budget);
				budget = null;
			}
			finally {
				if(budget != null)
					budget.close();
			}
		}, _sc).whenComplete((ignored, error) -> {
			try {
				if(error != null)
					fail.accept(error);
				else if(failure.get() == null)
					out.closeInput();
			}
			finally {
				onComplete();
			}
		});

		OOCInstructionUtils.submitOOCTasks(admitted, cb -> {
			ReservationBudget budget = OOCInstructionUtils.detachBudget(cb);
			try {
				if(budget == null)
					throw new DMLRuntimeException("Missing admitted grouped-reduce task budget.");
				IndexedMatrixValue input = cb.get();
				int group = group(input.getIndexes());
				ManagedPayload<IndexedMatrixValue> p = payload(new IndexedMatrixValue(outputIndex(group),
					_partialFn.apply(input)), budget);
				synchronized(groupLocks[group]) {
					accumulateGroup(group, p, budget);
					if(consumedPerGroup.incrementAndGet(group) == groupSize)
						finalizeTasks.enqueue(group);
				}
			}
			catch(InterruptedException e) {
				throw new RuntimeException(e);
			}
			finally {
				if(budget != null)
					budget.close();
			}
		}, _sc).whenComplete((ignored, error) -> {
			if(error != null)
				fail.accept(error);
			try {
				finalizeTasks.closeInput();
			}
			catch(Throwable t) {
				fail.accept(t);
			}
		});
	}

	private void finalizeGroup(int group, OOCStream<IndexedMatrixValue> out, ReservationBudget budget) {
		StateLease<IndexedMatrixValue> lease = await(_table.take(group, budget));
		if(lease == null)
			throw new IllegalStateException("Missing grouped-reduce accumulator for group " + group);
		MatrixBlock result;
		try(lease) {
			result = _finalizeFn.apply((MatrixBlock) lease.value().getValue());
		}
		OOCInstructionUtils.enqueueExact(out, new IndexedMatrixValue(outputIndex(group), result), budget);
	}

	private void accumulateGroup(int group, ManagedPayload<IndexedMatrixValue> payload, ReservationBudget budget)
		throws InterruptedException {
		while(true) {
			StateLease<IndexedMatrixValue> existing;
			try {
				existing = await(_table.installOrTake(group, payload, budget));
			}
			catch(RuntimeException ex) {
				payload.release();
				throw ex;
			}
			if(existing == null)
				return;
			IndexedMatrixValue merged;
			try(existing) {
				merged = new IndexedMatrixValue(existing.value().getIndexes(),
					_mergeFn.apply((MatrixBlock) existing.value().getValue(),
						(MatrixBlock) payload.value().getValue()));
			}
			finally {
				payload.release();
			}
			MatrixBlock block = (MatrixBlock) merged.getValue();
			payload = payload(merged, budget);
		}
	}

	private ManagedPayload<IndexedMatrixValue> payload(IndexedMatrixValue value, MemoryAllowance owner) {
		MatrixBlock block = (MatrixBlock) value.getValue();
		long bytes = block.getExactSerializedSize();
		owner.reserveBlocking(bytes);
		return new ManagedPayload<>(value, bytes, owner);
	}

	private static long reduceTaskBudgetBytes(DataCharacteristics input, DataCharacteristics output) {
		long tileBytes = Math.max(OOCInstructionUtils.estimateOutputTileBytes(input),
			OOCInstructionUtils.estimateOutputTileBytes(output));
		return saturatingMultiply(tileBytes, 3);
	}

	private static long finalizeTaskBudgetBytes(DataCharacteristics input, DataCharacteristics output) {
		long tileBytes = Math.max(OOCInstructionUtils.estimateOutputTileBytes(input),
			OOCInstructionUtils.estimateOutputTileBytes(output));
		return saturatingAdd(tileBytes, tileBytes);
	}

	private static long saturatingMultiply(long value, long factor) {
		if(value > Long.MAX_VALUE / factor)
			return Long.MAX_VALUE;
		return value * factor;
	}

	private static long saturatingAdd(long a, long b) {
		long result = a + b;
		return result < 0 ? Long.MAX_VALUE : result;
	}

	private static <T> T await(OOCFuture<T> future) {
		try {
			return future.get();
		}
		catch(InterruptedException | ExecutionException ex) {
			throw DMLRuntimeException.of(ex.getCause());
		}
	}

	private void requestPreferredInputPattern() {
		if(!getChildren().isEmpty() && !getChildren().get(0).hasStartedExecution())
			getChildren().get(0).requestPattern(_grouping.preferredInputPattern());
	}

	public Grouping getGrouping() {
		return _grouping;
	}

	public int group(MatrixIndexes ix) {
		return _grouping.group(ix, _inputStreamable.getDataCharacteristics());
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
