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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.ooc.util.StateTableUtils;

/** Matrix multiplication which consumes both inputs as live, spillable indexed streams. */
public final class StreamingMMultOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final AggregateBinaryOperator _multiply;
	private final BinaryOperator _plus;
	private final Object _matchingLock = new Object();
	private final AtomicInteger _active = new AtomicInteger(1);
	private StateTable<IndexedMatrixValue> _left;
	private StateTable<IndexedMatrixValue> _right;
	private StateTable<IndexedMatrixValue> _accumulators;
	private OOCStream<AutoCloseable> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private int _rowBlocks;
	private int _innerBlocks;
	private int _colBlocks;
	private int[] _leftRemaining;
	private int[] _rightRemaining;
	private long _taskBytes;

	public StreamingMMultOOCPrimitive(OOCStreamable<IndexedMatrixValue> left, OOCStreamable<IndexedMatrixValue> right,
		OOCStreamable<IndexedMatrixValue> output, AggregateBinaryOperator multiply, BinaryOperator plus,
		StreamContext context) {
		super(context, left, right);
		_output = output;
		_multiply = multiply;
		_plus = plus;
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.ANY;
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = _pattern.preferred(accessPattern);
		OOCPrimitive left = getInputDependency(0);
		OOCPrimitive right = getInputDependency(1);
		if(left != null)
			left.requestPattern(OOCAccessPattern.ROW_MAJOR);
		if(right != null)
			right.requestPattern(OOCAccessPattern.COL_MAJOR);
	}

	@Override
	protected long getMaxTaskReservationBytes() {
		long left = OOCUtils.estimateFullTileBytes(getInput(0).getDataCharacteristics());
		long right = OOCUtils.estimateFullTileBytes(getInput(1).getDataCharacteristics());
		long output = OOCUtils.estimateFullTileBytes(_output.getDataCharacteristics());
		return OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(left) +
			OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(right) + output * 4;
	}

	@Override
	protected void startExecution() {
		_rowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(getInput(0).getDataCharacteristics()));
		_innerBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(getInput(0).getDataCharacteristics()));
		_colBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(getInput(1).getDataCharacteristics()));
		_taskBytes = getMaxTaskReservationBytes();
		_left = new StateTable<>(_rowBlocks * _innerBlocks);
		_right = new StateTable<>(_innerBlocks * _colBlocks);
		_leftRemaining = new int[_rowBlocks * _innerBlocks];
		_rightRemaining = new int[_innerBlocks * _colBlocks];
		_accumulators = new StateTable<>(_rowBlocks * _colBlocks);
		long accumulatorPriorityOffset = (long) _rowBlocks * _colBlocks;
		_accumulators.addEvictionPolicy(slot -> slot - accumulatorPriorityOffset);
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

		List<OOCStream<IndexedMatrixValue>> inputs = List.of(getInputReadStream(0), getInputReadStream(1));
		OOCInstructionUtils.submitOOCTasks(inputs, this::accept, getContext()).whenComplete((ignored, error) -> {
			if(error != null)
				fail(error);
			completeOne();
		});
	}

	private void accept(int input, OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		IndexedMatrixValue value = callback.get();
		int row = Math.toIntExact(value.getIndexes().getRowIndex() - 1);
		int col = Math.toIntExact(value.getIndexes().getColumnIndex() - 1);
		synchronized(_matchingLock) {
			if(input == 0) {
				int leftSlot = row * _innerBlocks + col;
				_leftRemaining[leftSlot] = _colBlocks;
				StateTableUtils.put(_left, leftSlot, callback, _allowance);
				for(int outputCol = 0; outputCol < _colBlocks; outputCol++) {
					int rightSlot = col * _colBlocks + outputCol;
					if(_right.contains(rightSlot))
						schedule(leftSlot, rightSlot, row * _colBlocks + outputCol);
				}
			}
			else {
				int rightSlot = row * _colBlocks + col;
				_rightRemaining[rightSlot] = _rowBlocks;
				StateTableUtils.put(_right, rightSlot, callback, _allowance);
				for(int outputRow = 0; outputRow < _rowBlocks; outputRow++) {
					int leftSlot = outputRow * _innerBlocks + row;
					if(_left.contains(leftSlot))
						schedule(leftSlot, rightSlot, outputRow * _colBlocks + col);
				}
			}
		}
	}

	private void schedule(int leftSlot, int rightSlot, int outputSlot) {
		_active.incrementAndGet();
		_allowance.reserveAsync(_taskBytes).whenComplete((ignored, error) -> {
			if(error != null) {
				fail(error);
				completeOne();
				return;
			}
			ReservationBudget budget = new ReservationBudget(_allowance, _taskBytes).enableReuse();
			OOCFuture
				.allOf(List.of(_left.acquire(leftSlot, budget), _right.acquire(rightSlot, budget)), StoreLease::close)
				.whenComplete((inputs, inputError) -> {
					if(inputError != null || inputs == null || inputs.get(0) == null || inputs.get(1) == null) {
						if(inputs != null)
							inputs.forEach(StreamingMMultOOCPrimitive::closeLease);
						budget.close();
						fail(inputError != null ? inputError : new IllegalStateException(
							"Missing streaming multiply tile."));
						completeOne();
						return;
					}
					try {
						_ready.enqueue(
							new MultiplyWork(inputs.get(0), inputs.get(1), leftSlot, rightSlot, outputSlot, budget));
					}
					catch(Throwable failure) {
						inputs.forEach(StreamingMMultOOCPrimitive::closeLease);
						budget.close();
						fail(failure);
						completeOne();
					}
				});
		});
	}

	private void process(AutoCloseable work) {
		if(work instanceof MultiplyWork multiply)
			multiply(multiply);
		else
			merge((MergeWork) work);
	}

	private void multiply(MultiplyWork work) {
		ReservationBudget budget = work.takeBudget();
		ManagedPayload<IndexedMatrixValue> partial = null;
		try {
			MatrixBlock left = (MatrixBlock) work._left.value().getValue();
			MatrixBlock right = (MatrixBlock) work._right.value().getValue();
			MatrixBlock block = left.aggregateBinaryOperations(left, right, new MatrixBlock(), _multiply);
			partial = payload(work._outputSlot, 1, block, budget);
			OOCFuture<List<Void>> released = work.releaseInputsAsync();
			ManagedPayload<IndexedMatrixValue> result = partial;
			partial = null;
			released.whenComplete((ignored, error) -> {
				if(error != null) {
					result.release();
					budget.close();
					fail(error);
					completeOne();
				}
				else {
					consumed(work._leftSlot, work._rightSlot);
					reduce(work._outputSlot, result, budget);
				}
			});
		}
		catch(Throwable failure) {
			if(partial != null)
				partial.release();
			budget.close();
			fail(failure);
			completeOne();
		}
	}

	private void consumed(int leftSlot, int rightSlot) {
		synchronized(_matchingLock) {
			if(--_leftRemaining[leftSlot] == 0)
				_left.clear(leftSlot);
			if(--_rightRemaining[rightSlot] == 0)
				_right.clear(rightSlot);
		}
	}

	private void reduce(int slot, ManagedPayload<IndexedMatrixValue> incoming, ReservationBudget budget) {
		if(count(incoming.value()) == _innerBlocks) {
			finalizeOutput(slot, incoming, budget);
			return;
		}
		OOCFuture<StoreLease<IndexedMatrixValue>> match;
		try {
			match = _accumulators.putOrTake(slot, incoming, budget);
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
				try {
					_ready.enqueue(new MergeWork(slot, incoming, existing, budget));
				}
				catch(Throwable failure) {
					incoming.release();
					existing.close();
					budget.close();
					fail(failure);
					completeOne();
				}
			}
		});
	}

	private void merge(MergeWork work) {
		ReservationBudget budget = work.takeBudget();
		ManagedPayload<IndexedMatrixValue> merged = null;
		try {
			IndexedMatrixValue existing = work._existing.value();
			IndexedMatrixValue incoming = work._incoming.value();
			MatrixBlock block = ((MatrixBlock) existing.getValue()).binaryOperations(_plus, incoming.getValue(),
				new MatrixBlock());
			merged = payload(work._slot, count(existing) + count(incoming), block, budget);
			work.releaseIncoming();
			OOCFuture<Void> released = work.closeExistingAsync();
			ManagedPayload<IndexedMatrixValue> result = merged;
			merged = null;
			released.whenComplete((ignored, error) -> {
				if(error != null) {
					result.release();
					budget.close();
					fail(error);
					completeOne();
				}
				else
					reduce(work._slot, result, budget);
			});
		}
		catch(Throwable failure) {
			if(merged != null)
				merged.release();
			budget.close();
			fail(failure);
			completeOne();
		}
	}

	private void finalizeOutput(int slot, ManagedPayload<IndexedMatrixValue> payload, ReservationBudget budget) {
		try {
			MatrixBlock block = (MatrixBlock) payload.value().getValue();
			payload.release();
			OOCUtils.enqueueExact(_outputStream,
				new IndexedMatrixValue(new MatrixIndexes(slot / _colBlocks + 1L, slot % _colBlocks + 1L), block),
				budget);
		}
		catch(Throwable failure) {
			payload.release();
			budget.close();
			fail(failure);
		}
		completeOne();
	}

	private static ManagedPayload<IndexedMatrixValue> payload(int slot, int count, MatrixBlock block,
		ReservationBudget budget) {
		long bytes = OOCUtils.memoryCharge(block);
		budget.reserveBlocking(bytes);
		return new ManagedPayload<>(new IndexedMatrixValue(new MatrixIndexes(slot + 1L, count), block), bytes, budget);
	}

	private static int count(IndexedMatrixValue value) {
		return Math.toIntExact(value.getIndexes().getColumnIndex());
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
		_left.close();
		_right.close();
		_accumulators.close();
		onComplete();
	}

	private static void closeLease(StoreLease<IndexedMatrixValue> lease) {
		if(lease != null)
			lease.close();
	}

	private static final class MultiplyWork implements AutoCloseable {
		private final int _leftSlot;
		private final int _rightSlot;
		private final int _outputSlot;
		private StoreLease<IndexedMatrixValue> _left;
		private StoreLease<IndexedMatrixValue> _right;
		private ReservationBudget _budget;

		private MultiplyWork(StoreLease<IndexedMatrixValue> left, StoreLease<IndexedMatrixValue> right, int leftSlot,
			int rightSlot, int outputSlot, ReservationBudget budget) {
			_left = left;
			_right = right;
			_leftSlot = leftSlot;
			_rightSlot = rightSlot;
			_outputSlot = outputSlot;
			_budget = budget;
		}

		private ReservationBudget takeBudget() {
			ReservationBudget budget = _budget;
			_budget = null;
			return budget;
		}

		private OOCFuture<List<Void>> releaseInputsAsync() {
			OOCFuture<Void> left = _left.closeAsync();
			OOCFuture<Void> right = _right.closeAsync();
			_left = null;
			_right = null;
			return OOCFuture.allOf(List.of(left, right));
		}

		@Override
		public void close() {
			closeLease(_left);
			closeLease(_right);
			if(_budget != null)
				_budget.close();
		}
	}

	private static final class MergeWork implements AutoCloseable {
		private final int _slot;
		private ManagedPayload<IndexedMatrixValue> _incoming;
		private StoreLease<IndexedMatrixValue> _existing;
		private ReservationBudget _budget;

		private MergeWork(int slot, ManagedPayload<IndexedMatrixValue> incoming,
			StoreLease<IndexedMatrixValue> existing, ReservationBudget budget) {
			_slot = slot;
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
			_incoming.release();
			_incoming = null;
		}

		private OOCFuture<Void> closeExistingAsync() {
			OOCFuture<Void> released = _existing.closeAsync();
			_existing = null;
			return released;
		}

		@Override
		public void close() {
			if(_incoming != null)
				_incoming.release();
			closeLease(_existing);
			if(_budget != null)
				_budget.close();
		}
	}
}
