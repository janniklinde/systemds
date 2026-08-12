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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiFunction;
import java.util.function.Function;

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
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public final class GroupedReduceOOCPrimitive extends OOCPrimitive {
	public enum Grouping {
		ROW_BLOCKS, COL_BLOCKS, BLOCK_INDEX
	}

	private final List<OOCStreamable<IndexedMatrixValue>> _inputs;
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final Grouping _grouping;
	private final Function<IndexedMatrixValue, MatrixBlock> _partial;
	private final BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> _merge;
	private final Function<MatrixBlock, MatrixBlock> _finish;
	private final AtomicBoolean _cleaned;
	private final AtomicBoolean[] _sourceComplete;
	private final AtomicInteger _active;
	private final AtomicInteger _finalizedGroups;
	private StateTable<IndexedMatrixValue> _table;
	private OOCStream<MergeWork> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private AtomicIntegerArray _mergesLeft;
	private int _numGroups;
	private int _groupSize;
	private int _colBlocks;

	public GroupedReduceOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<IndexedMatrixValue> output,
		BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> merge, StreamContext context) {
		this(input, output, Grouping.ROW_BLOCKS, value -> (MatrixBlock) value.getValue(), merge, Function.identity(),
			context);
	}

	public GroupedReduceOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<IndexedMatrixValue> output,
		Grouping grouping, Function<IndexedMatrixValue, MatrixBlock> partial,
		BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> merge, Function<MatrixBlock, MatrixBlock> finish,
		StreamContext context) {
		this(List.of(input), output, grouping, partial, merge, finish, context);
	}

	public GroupedReduceOOCPrimitive(List<OOCStreamable<IndexedMatrixValue>> inputs,
		OOCStreamable<IndexedMatrixValue> output, Grouping grouping, Function<IndexedMatrixValue, MatrixBlock> partial,
		BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> merge, Function<MatrixBlock, MatrixBlock> finish,
		StreamContext context) {
		super(context, inputs.toArray(OOCStreamable[]::new));
		if(inputs.isEmpty())
			throw new IllegalArgumentException("Grouped OOC reduction requires at least one input.");
		if(inputs.size() > 1 && grouping != Grouping.BLOCK_INDEX)
			throw new IllegalArgumentException("Only block-index grouping folds multiple input streams.");
		_inputs = inputs;
		_output = output;
		_grouping = grouping;
		_partial = partial;
		_merge = merge;
		_finish = finish;
		_cleaned = new AtomicBoolean();
		_sourceComplete = new AtomicBoolean[inputs.size()];
		for(int i = 0; i < _sourceComplete.length; i++)
			_sourceComplete[i] = new AtomicBoolean();
		_active = new AtomicInteger(inputs.size());
		_finalizedGroups = new AtomicInteger();
	}

	@Override
	protected void inferPatternsInternal() {
		if(_grouping == Grouping.BLOCK_INDEX) {
			_pattern = OOCAccessPattern.ANY;
			for(OOCPrimitive child : getChildren())
				_pattern = _pattern.fused(child.getAccessPattern());
			if(_pattern.isPlannable() && _pattern != OOCAccessPattern.ANY)
				for(OOCPrimitive child : getChildren())
					child.requestPattern(_pattern);
			inferParentPatterns();
			return;
		}
		_pattern = groupingPattern();
		for(OOCPrimitive child : getChildren())
			child.requestPattern(_pattern);
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = _grouping == Grouping.BLOCK_INDEX ? accessPattern : groupingPattern();
		for(OOCPrimitive child : getChildren())
			child.requestPattern(_pattern);
	}

	private OOCAccessPattern groupingPattern() {
		return _grouping == Grouping.COL_BLOCKS ? OOCAccessPattern.COL_MAJOR : OOCAccessPattern.ROW_MAJOR;
	}

	@Override
	protected void startExecution() {
		DataCharacteristics inputDc = _inputs.get(0).getDataCharacteristics();
		if(inputDc == null || !inputDc.dimsKnown() || inputDc.getBlocksize() <= 0)
			throw new DMLRuntimeException("Grouped OOC reduction requires known input dimensions and block size.");
		configureGroups(inputDc);
		_outputStream = _output.getWriteStream();
		_ready = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _ready);
		_table = new StateTable<>(OOCCacheManager.getGlobalCache(), CachingStream._streamSeq.getNextID());

		OOCInstructionUtils.submitCloseableOOCTasks(_ready, this::process, getContext())
			.whenComplete((ignored, error) -> {
				try {
					_outputStream.closeInput();
				}
				catch(Throwable failure) {
					fail(failure);
				}
				finally {
					cleanup();
				}
			});

		long logicalBytes = OOCUtils.estimateFullTileBytes(_output.getDataCharacteristics());
		for(OOCStreamable<IndexedMatrixValue> input : _inputs)
			logicalBytes = Math.max(logicalBytes, OOCUtils.estimateFullTileBytes(input.getDataCharacteristics()));
		long pinBytes = OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(logicalBytes);
		long taskBytes = pinBytes + logicalBytes * 2;
		for(int i = 0; i < _inputs.size(); i++) {
			OOCStream<IndexedMatrixValue> input = getInputReadStream(i);
			getContext().addInStream(input);
			AllocatedOOCStream<IndexedMatrixValue> admitted = new AllocatedOOCStream<>(input, _allowance,
				ignored -> taskBytes);
			getContext().addInStream(admitted);
			final int source = i;
			admitted.setSubscriber(callback -> accept(source, callback));
		}
	}

	private void configureGroups(DataCharacteristics inputDc) {
		long rowBlocks = inputDc.getNumRowBlocks();
		long colBlocks = inputDc.getNumColBlocks();
		_colBlocks = Math.toIntExact(colBlocks);
		switch(_grouping) {
			case ROW_BLOCKS:
				_numGroups = Math.toIntExact(rowBlocks);
				_groupSize = Math.toIntExact(colBlocks);
				break;
			case COL_BLOCKS:
				_numGroups = Math.toIntExact(colBlocks);
				_groupSize = Math.toIntExact(rowBlocks);
				break;
			case BLOCK_INDEX:
				_numGroups = Math.toIntExact(Math.multiplyExact(rowBlocks, colBlocks));
				_groupSize = _inputs.size();
				break;
			default:
				throw new IllegalStateException("Unsupported grouped-reduce grouping: " + _grouping);
		}
		if(_numGroups <= 0 || _groupSize <= 0)
			throw new DMLRuntimeException("Grouped OOC reduction requires non-empty input block geometry.");
		_mergesLeft = new AtomicIntegerArray(_numGroups);
		for(int group = 0; group < _numGroups; group++)
			_mergesLeft.set(group, _groupSize - 1);
	}

	private void accept(int source, OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback.isEos() || callback.isFailure()) {
			try(callback) {
				if(callback.isFailure())
					callback.get();
			}
			catch(Throwable failure) {
				fail(failure);
			}
			finishSource(source);
			return;
		}

		ReservationBudget budget = null;
		ManagedPayload<IndexedMatrixValue> payload = null;
		_active.incrementAndGet();
		try(callback) {
			budget = AllocatedOOCStream.detachBudget(callback).enableReuse();
			IndexedMatrixValue input = callback.get();
			int group = group(input.getIndexes());
			MatrixBlock partial = _partial.apply(input);
			if(partial == null)
				throw new DMLRuntimeException("Grouped OOC reduction produced a null partial block.");
			payload = payload(new IndexedMatrixValue(outputIndexes(group), partial), budget);
			if(_groupSize == 1)
				finalizeGroup(group, payload, budget);
			else
				reduce(group, payload, budget);
			payload = null;
			budget = null;
		}
		catch(Throwable failure) {
			fail(failure);
			completeOne();
		}
		finally {
			if(payload != null)
				payload.release();
			if(budget != null)
				budget.close();
		}
	}

	private void reduce(int group, ManagedPayload<IndexedMatrixValue> incoming, ReservationBudget budget) {
		OOCFuture<StoreLease<IndexedMatrixValue>> match;
		try {
			match = _table.putOrTake(group, incoming, budget);
		}
		catch(Throwable failure) {
			incoming.release();
			budget.close();
			fail(failure);
			completeOne();
			return;
		}
		match.whenComplete((existing, error) -> {
			if(error != null) {
				incoming.release();
				budget.close();
				fail(error);
				completeOne();
			}
			else if(existing == null) {
				budget.close();
				completeOne();
			}
			else {
				MergeWork work = new MergeWork(group, incoming, existing, budget);
				try {
					_ready.enqueue(work);
				}
				catch(Throwable failure) {
					work.close();
					fail(failure);
					completeOne();
				}
			}
		});
	}

	private void process(MergeWork work) {
		ReservationBudget budget = work.takeBudget();
		ManagedPayload<IndexedMatrixValue> merged = null;
		OOCFuture<Void> released;
		final boolean complete;
		try {
			IndexedMatrixValue left = work._existing.value();
			IndexedMatrixValue right = work._incoming.value();
			int remaining = _mergesLeft.decrementAndGet(work._group);
			if(remaining < 0)
				throw new DMLRuntimeException("Too many partial tiles for grouped-reduce group " + (work._group + 1));
			complete = remaining == 0;
			MatrixBlock value = _merge.apply((MatrixBlock) left.getValue(), (MatrixBlock) right.getValue());
			if(value == null)
				throw new DMLRuntimeException("Grouped OOC reduction produced a null merged block.");
			merged = payload(new IndexedMatrixValue(outputIndexes(work._group), value), budget);
			work.releaseIncoming();
			released = work.closeExistingAsync();
		}
		catch(Throwable failure) {
			if(merged != null)
				merged.release();
			budget.close();
			fail(failure);
			completeOne();
			return;
		}

		ManagedPayload<IndexedMatrixValue> next = merged;
		released.whenComplete((ignored, error) -> {
			if(error != null) {
				next.release();
				budget.close();
				fail(error);
				completeOne();
			}
			else if(complete)
				finalizeGroup(work._group, next, budget);
			else
				reduce(work._group, next, budget);
		});
	}

	private void finalizeGroup(int group, ManagedPayload<IndexedMatrixValue> payload, ReservationBudget budget) {
		try {
			MatrixBlock outputBlock = _finish.apply((MatrixBlock) payload.value().getValue());
			if(outputBlock == null)
				throw new DMLRuntimeException("Grouped OOC reduction produced a null final block.");
			payload.release();
			payload = null;
			OOCUtils.enqueueExact(_outputStream, new IndexedMatrixValue(outputIndexes(group), outputBlock), budget);
			_finalizedGroups.incrementAndGet();
		}
		catch(Throwable failure) {
			if(payload != null)
				payload.release();
			budget.close();
			fail(failure);
		}
		completeOne();
	}

	private static ManagedPayload<IndexedMatrixValue> payload(IndexedMatrixValue value, ReservationBudget budget) {
		long bytes = OOCUtils.memoryCharge(value);
		budget.reserveBlocking(bytes);
		return new ManagedPayload<>(value, bytes, budget);
	}

	private int group(MatrixIndexes indexes) {
		long group = switch(_grouping) {
			case ROW_BLOCKS -> indexes.getRowIndex() - 1;
			case COL_BLOCKS -> indexes.getColumnIndex() - 1;
			case BLOCK_INDEX -> (indexes.getRowIndex() - 1) * _colBlocks + indexes.getColumnIndex() - 1;
		};
		if(group < 0 || group >= _numGroups)
			throw new DMLRuntimeException("Invalid grouped-reduce group index: " + group);
		return Math.toIntExact(group);
	}

	private MatrixIndexes outputIndexes(int group) {
		return switch(_grouping) {
			case ROW_BLOCKS -> new MatrixIndexes(group + 1L, 1);
			case COL_BLOCKS -> new MatrixIndexes(1, group + 1L);
			case BLOCK_INDEX -> new MatrixIndexes(group / _colBlocks + 1L, group % _colBlocks + 1L);
		};
	}

	private void finishSource(int source) {
		if(_sourceComplete[source].compareAndSet(false, true))
			completeOne();
	}

	private void completeOne() {
		int remaining = _active.decrementAndGet();
		if(remaining != 0)
			return;
		if(!hasFailed() && _finalizedGroups.get() != _numGroups)
			fail(new DMLRuntimeException(
				"Grouped reduction completed " + _finalizedGroups.get() + " of " + _numGroups + " groups."));
		try {
			_ready.closeInput();
		}
		catch(IllegalStateException ignored) {
			// Failure propagation may already have closed the ready stream.
		}
	}

	private void cleanup() {
		if(!_cleaned.compareAndSet(false, true))
			return;
		try {
			if(_table != null)
				_table.close();
		}
		finally {
			onComplete();
		}
	}

	private static final class MergeWork implements AutoCloseable {
		private final int _group;
		private ManagedPayload<IndexedMatrixValue> _incoming;
		private StoreLease<IndexedMatrixValue> _existing;
		private ReservationBudget _budget;

		private MergeWork(int group, ManagedPayload<IndexedMatrixValue> incoming,
			StoreLease<IndexedMatrixValue> existing, ReservationBudget budget) {
			_group = group;
			_incoming = incoming;
			_existing = existing;
			_budget = budget;
		}

		private ReservationBudget takeBudget() {
			ReservationBudget budget = _budget;
			_budget = null;
			return budget;
		}

		private void releaseIncoming() {
			if(_incoming != null) {
				_incoming.release();
				_incoming = null;
			}
		}

		private OOCFuture<Void> closeExistingAsync() {
			StoreLease<IndexedMatrixValue> existing = _existing;
			_existing = null;
			return existing.closeAsync();
		}

		@Override
		public void close() {
			releaseIncoming();
			if(_existing != null) {
				_existing.close();
				_existing = null;
			}
			if(_budget != null)
				_budget.close();
		}
	}
}
