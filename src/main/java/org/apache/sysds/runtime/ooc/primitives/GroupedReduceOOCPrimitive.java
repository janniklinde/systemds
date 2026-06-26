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
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
		final OOCStream<IndexedMatrixValue> in = new AllocatedOOCStream<>(_inputStreamable.getReadStream(), _allowance,
			t -> _allocFn.applyAsLong(t.getIndexes()), true);
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		final DataCharacteristics dc = _inputStreamable.getDataCharacteristics();
		final int numGroups = _grouping.numGroups(dc);
		final int groupSize = _grouping.groupSize(dc);
		final AtomicIntegerArray consumedPerGroup = new AtomicIntegerArray(numGroups);
		final AtomicInteger pending = new AtomicInteger(1);
		final AtomicBoolean completed = new AtomicBoolean(false);
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final Runnable releasePending = () -> {
			if(pending.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
				if(failure.get() == null)
					out.closeInput();
				onComplete();
			}
		};
		final Consumer<Throwable> fail = t -> {
			if(failure.compareAndSet(null, t)) {
				DMLRuntimeException re = DMLRuntimeException.of(t);
				out.propagateFailure(re);
				if(_sc != null)
					_sc.failAll(re);
			}
		};

		OOCInstructionUtils.submitOOCTasks(in, cb -> {
			IndexedMatrixValue input = cb.get();
			int group = group(input.getIndexes());
			long reservedBytes = _allocFn.applyAsLong(input.getIndexes());
			ManagedPayload<IndexedMatrixValue> p = new ManagedPayload<>(new IndexedMatrixValue(outputIndex(group),
				_partialFn.apply(input)), reservedBytes, _allowance);
			pending.incrementAndGet();
			try {
				_table.merge(group, p, (l, r) -> new IndexedMatrixValue(l.getIndexes(),
					_mergeFn.apply((MatrixBlock) l.getValue(), (MatrixBlock) r.getValue())))
					.whenComplete((ignored, error) -> {
						try {
							if(error != null) {
								fail.accept(error);
								return;
							}
							if(consumedPerGroup.incrementAndGet(group) == groupSize)
								finalizeGroup(group, out, pending, releasePending, fail);
						}
						finally {
							releasePending.run();
						}
					});
			}
			catch(Throwable t) {
				p.release();
				releasePending.run();
				throw t;
			}
		}, _sc).whenComplete((ignored, error) -> {
			if(error != null)
				fail.accept(error);
			releasePending.run();
		});
	}

	private void finalizeGroup(int group, OOCStream<IndexedMatrixValue> out, AtomicInteger pending,
		Runnable releasePending, Consumer<Throwable> fail) {
		pending.incrementAndGet();
		try {
			_table.take(group).whenComplete((lease, error) -> {
				try {
					if(error != null) {
						fail.accept(error);
						return;
					}
					if(lease == null)
						throw new IllegalStateException("Missing grouped-reduce accumulator for group " + group);
					MatrixBlock value = (MatrixBlock) lease.value().getValue();
					MatrixBlock result = _finalizeFn.apply(value);
					out.enqueue(new LeaseBackedOutputCallback(new IndexedMatrixValue(outputIndex(group), result), lease));
				}
				catch(Throwable t) {
					if(lease != null)
						lease.close();
					fail.accept(t);
				}
				finally {
					releasePending.run();
				}
			});
		}
		catch(Throwable t) {
			fail.accept(t);
			releasePending.run();
		}
	}

	private static final class LeaseBackedOutputCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final SharedLease _shared;
		private final IndexedMatrixValue _value;
		private DMLRuntimeException _failure;
		private boolean _closed;

		private LeaseBackedOutputCallback(IndexedMatrixValue value,
			OperatorStateTable.StateLease<IndexedMatrixValue> lease) {
			_shared = new SharedLease(lease);
			_value = value;
		}

		private LeaseBackedOutputCallback(IndexedMatrixValue value, SharedLease shared) {
			_shared = shared;
			_value = value;
		}

		@Override
		public IndexedMatrixValue get() {
			if(_failure != null)
				throw _failure;
			return _value;
		}

		@Override
		public synchronized OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed callback.");
			_shared.retain();
			return new LeaseBackedOutputCallback(_value, _shared);
		}

		@Override
		public synchronized void close() {
			if(_closed)
				return;
			_closed = true;
			_shared.release();
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_failure = failure;
		}

		@Override
		public boolean isEos() {
			return false;
		}

		@Override
		public boolean isFailure() {
			return _failure != null;
		}
	}

	private static final class SharedLease {
		private final OperatorStateTable.StateLease<IndexedMatrixValue> _lease;
		private final AtomicInteger _refs = new AtomicInteger(1);

		private SharedLease(OperatorStateTable.StateLease<IndexedMatrixValue> lease) {
			_lease = lease;
		}

		private void retain() {
			_refs.incrementAndGet();
		}

		private void release() {
			if(_refs.decrementAndGet() == 0)
				_lease.close();
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
