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

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable.InputView;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.ooc.util.StateTableUtils;

public final class GeneralMMultOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final AggregateBinaryOperator _multiply;
	private final BinaryOperator _plus;
	private final boolean _streaming;
	private final Object _arrivalLock = new Object();
	private final BitSet _leftArrived = new BitSet();
	private final BitSet _rightArrived = new BitSet();
	private final AtomicInteger _arrivals = new AtomicInteger(2);
	private StateTable<IndexedMatrixValue> _leftTiles;
	private StateTable<IndexedMatrixValue> _rightTiles;
	private InputView _leftView;
	private InputView _rightView;
	private AtomicIntegerArray _leftUses;
	private AtomicIntegerArray _rightUses;
	private OOCStream<Integer> _matches;
	private final AtomicBoolean _sourceComplete = new AtomicBoolean();
	private final AtomicInteger _active = new AtomicInteger(1);
	private MaterializedStore<IndexedMatrixValue> _leftStore;
	private MaterializedStore<IndexedMatrixValue> _rightStore;
	private IndexedMaterializedStoreReader<IndexedMatrixValue> _leftReader;
	private IndexedMaterializedStoreReader<IndexedMatrixValue> _rightReader;
	private StateTable<IndexedMatrixValue> _accumulators;
	private OOCStream<AutoCloseable> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private int _rowBlocks;
	private int _innerBlocks;
	private int _colBlocks;
	private int _nextTask;
	private int _numTasks;
	private long _taskBytes;

	public GeneralMMultOOCPrimitive(OOCStreamable<IndexedMatrixValue> left, OOCStreamable<IndexedMatrixValue> right,
		OOCStreamable<IndexedMatrixValue> output, AggregateBinaryOperator multiply, BinaryOperator plus,
		boolean requireStreaming, StreamContext context) {
		super(context, left, right);
		_output = output;
		_multiply = multiply;
		_plus = plus;
		_streaming = requireStreaming;
	}

	public static boolean shouldStream(DataCharacteristics left, DataCharacteristics right) {
		if(!left.dimsKnown() || !right.dimsKnown() || left.getRows() <= 0 || left.getCols() <= 0 ||
			right.getCols() <= 0 || left.getCols() != right.getRows() || left.getBlocksize() <= 0 ||
			left.getBlocksize() != right.getBlocksize())
			return false;
		double outputBytes = 8d * left.getRows() * right.getCols();
		double counterpartBytes = 8d * Math.min((double) left.getRows() * left.getCols(),
			(double) right.getRows() * right.getCols());
			return outputBytes + counterpartBytes <= GlobalMemoryBroker.get().getAllowedMemory() / 4d;
	}

	@Override
	protected boolean isStreamingInput(int index) {
		return _streaming;
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		if(_streaming)
			return List.of();
		long innerBlocks = OOCUtils.getNumColBlocks(getInput(0).getDataCharacteristics());
		return List.of(new OOCMaterializedInputRequest(0, OOCStoreLayout.ROW_MAJOR, 1,
			(row, col) -> (row - 1) * innerBlocks + col - 1),
			new OOCMaterializedInputRequest(1, OOCStoreLayout.COL_MAJOR, 1,
				(row, col) -> (col - 1) * innerBlocks + row - 1));
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
			left.requestPattern(_streaming ? _pattern : OOCAccessPattern.ROW_MAJOR);
		if(right != null)
			right.requestPattern(_streaming ? _pattern : OOCAccessPattern.COL_MAJOR);
	}

	@Override
	protected void startExecution() {
		DataCharacteristics left = getInput(0).getDataCharacteristics();
		DataCharacteristics right = getInput(1).getDataCharacteristics();
		_rowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(left));
		_innerBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(left));
		_colBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(right));
		_numTasks = _rowBlocks * _innerBlocks * _colBlocks;
		long leftBytes = OOCUtils.estimateFullTileBytes(left);
		long rightBytes = OOCUtils.estimateFullTileBytes(right);
		long outputBytes = OOCUtils.estimateFullTileBytes(_output.getDataCharacteristics());
		_taskBytes = OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(leftBytes) +
			OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(rightBytes) +
			OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(outputBytes) + outputBytes * 3;

		_outputStream = _output.getWriteStream();
		_ready = new SubscribableTaskQueue<>();
		_accumulators = new StateTable<>();
		long accumulatorPriorityOffset = (long) _rowBlocks * _colBlocks;
		_accumulators.addEvictionPolicy(slot -> slot - accumulatorPriorityOffset);
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

		if(_streaming) {
			_leftTiles = new StateTable<>();
			_rightTiles = new StateTable<>();
			_leftUses = new AtomicIntegerArray(_rowBlocks * _innerBlocks);
			_rightUses = new AtomicIntegerArray(_innerBlocks * _colBlocks);
			_matches = new SubscribableTaskQueue<>();
			AllocatedOOCStream<Integer> admitted = new AllocatedOOCStream<>(_matches, _allowance,
				index -> _taskBytes, true);
			getContext().addInStream(_matches, admitted);
			admitted.setSubscriber(this::admitMatch);
			for(int input = 0; input < 2; input++) {
				boolean isLeft = input == 0;
				if(getInput(input) instanceof MaterializedStoreStreamable materialized) {
					consumeInputHandle(input);
					InputView view = materialized.getReservedInputView();
					if(isLeft)
						_leftView = view;
					else
						_rightView = view;
					view.start(index -> {
						MatrixIndexes indexes = view.indexes(index);
						match((int) indexes.getRowIndex() - 1, (int) indexes.getColumnIndex() - 1, isLeft);
					}, error -> {
						if(error != null)
							fail(error);
						if(_arrivals.decrementAndGet() == 0)
							_matches.closeInput();
					});
				}
				else {
					OOCStream<IndexedMatrixValue> stream = getInputReadStream(input);
					getContext().addInStream(stream);
					stream.setSubscriber(callback -> accept(callback, isLeft));
				}
			}
		}
		else {
			OOCFuture.allOf(List.of(getMaterializedInput(0), getMaterializedInput(1)), MaterializedStore::close)
				.whenComplete(this::storesReady);
		}
	}

	private void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback, boolean left) {
		if(callback.isEos() || callback.isFailure()) {
			try(callback) {
				if(callback.isFailure())
					callback.get();
			}
			catch(Throwable error) {
				fail(error);
			}
			finally {
				if(_arrivals.decrementAndGet() == 0)
					_matches.closeInput();
			}
			return;
		}
		_arrivals.incrementAndGet();
		try(callback) {
			IndexedMatrixValue tile = callback.get();
			int row = (int) tile.getIndexes().getRowIndex() - 1;
			int col = (int) tile.getIndexes().getColumnIndex() - 1;
			int slot = left ? row * _innerBlocks + col : row * _colBlocks + col;
			StateTableUtils.put(left ? _leftTiles : _rightTiles, slot, callback, _allowance);
			match(row, col, left);
		}
		catch(Throwable error) {
			fail(error);
		}
		finally {
			if(_arrivals.decrementAndGet() == 0)
				_matches.closeInput();
		}
	}

	private void match(int row, int col, boolean left) {
		synchronized(_arrivalLock) {
			(left ? _leftArrived : _rightArrived).set(left ? row * _innerBlocks + col : row * _colBlocks + col);
			if(left) {
				for(int j = 0; j < _colBlocks; j++)
					if(_rightArrived.get(col * _colBlocks + j))
						_matches.enqueue((row * _colBlocks + j) * _innerBlocks + col);
			}
			else {
				for(int i = 0; i < _rowBlocks; i++)
					if(_leftArrived.get(i * _innerBlocks + row))
						_matches.enqueue((i * _colBlocks + col) * _innerBlocks + row);
			}
		}
	}

	private void admitMatch(OOCStream.QueueCallback<Integer> callback) {
		if(callback.isEos() || callback.isFailure()) {
			try(callback) {
				if(callback.isFailure())
					callback.get();
			}
			catch(Throwable error) {
				fail(error);
			}
			finally {
				finishSource();
			}
			return;
		}
		ReservationBudget budget = AllocatedOOCStream.detachBudget(callback).enableReuse();
		_active.incrementAndGet();
		try(callback) {
			int task = callback.get();
			int inner = task % _innerBlocks;
			int outputSlot = task / _innerBlocks;
			int row = outputSlot / _colBlocks;
			int col = outputSlot % _colBlocks;
			OOCFuture.allOf(List.of(_leftView == null ? _leftTiles.acquire(row * _innerBlocks + inner, budget)
				: _leftView.acquire(row + 1L, inner + 1L, budget),
				_rightView == null ? _rightTiles.acquire(inner * _colBlocks + col, budget)
				: _rightView.acquire(inner + 1L, col + 1L, budget)), StoreLease::close)
				.whenComplete((inputs, error) -> {
					try {
						if(error != null)
							throw new CompletionException(error);
						_ready.enqueue(new MultiplyWork(inputs.get(0), inputs.get(1), outputSlot, budget,
							row * _innerBlocks + inner, inner * _colBlocks + col));
					}
					catch(Throwable failure) {
						if(inputs != null)
							inputs.forEach(StoreLease::close);
						budget.close();
						fail(failure);
						completeOne();
					}
				});
		}
		catch(Throwable error) {
			budget.close();
			fail(error);
			completeOne();
		}
	}

	private void storesReady(List<MaterializedStore<IndexedMatrixValue>> stores, Throwable error) {
		if(error != null) {
			fail(error);
			finishSource();
			return;
		}
		_leftStore = stores.get(0);
		_rightStore = stores.get(1);
		OOCFuture.allOf(List.of(_leftStore.completion(), _rightStore.completion()))
			.whenComplete((ignored, completionError) -> {
				if(completionError != null) {
					fail(completionError);
					finishSource();
					return;
				}
				_leftReader = _leftStore.openIndexedReader(new CountingLiveness(_leftStore.size(), _colBlocks));
				_rightReader = _rightStore.openIndexedReader(new CountingLiveness(_rightStore.size(), _rowBlocks));
				scheduleNext();
			});
	}

	private void scheduleNext() {
		while(true) {
			if(hasFailed() || _nextTask == _numTasks) {
				finishSource();
				return;
			}

			OOCFuture<?> reservation = _allowance.reserveAsync(_taskBytes);

			if(!reservation.isDone()) {
				// _nextTask ctr cannot be stale due to OOCFuture synchronization barrier
				reservation.whenComplete((ignored, error) -> {
					if(error != null) {
						fail(error);
						finishSource();
						return;
					}

					startTask();
					scheduleNext();
				});
				return;
			}

			try {
				reservation.getNow(null);
			}
			catch(CompletionException ex) {
				fail(ex.getCause());
				finishSource();
				return;
			}

			startTask();
		}
	}

	private void startTask() {
		ReservationBudget budget = new ReservationBudget(_allowance, _taskBytes).enableReuse();

		int task = _nextTask++;
		_active.incrementAndGet();
		requestInputs(task, budget);
	}

	private void requestInputs(int task, ReservationBudget budget) {
		int inner = task % _innerBlocks;
		int row;
		int col;
		if(_pattern == OOCAccessPattern.COL_MAJOR) {
			row = task / _innerBlocks % _rowBlocks;
			col = task / (_innerBlocks * _rowBlocks);
		}
		else {
			col = task / _innerBlocks % _colBlocks;
			row = task / (_innerBlocks * _colBlocks);
		}
		int outputSlot = row * _colBlocks + col;
		try {
			OOCFuture
				.allOf(List.of(_leftReader.request(row + 1L, inner + 1L, budget),
					_rightReader.request(inner + 1L, col + 1L, budget)), StoreLease::close)
				.whenComplete((inputs, error) -> {
					if(error != null) {
						budget.close();
						fail(error);
						completeOne();
						return;
					}
					try {
						_ready.enqueue(new MultiplyWork(inputs.get(0), inputs.get(1), outputSlot, budget, -1, -1));
					}
					catch(Throwable failure) {
						inputs.forEach(StoreLease::close);
						budget.close();
						fail(failure);
						completeOne();
					}
				});
		}
		catch(Throwable failure) {
			budget.close();
			fail(failure);
			completeOne();
		}
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
			if(_streaming) {
				if(_leftUses.incrementAndGet(work._leftSlot) == _colBlocks) {
					if(_leftView == null)
						_leftTiles.clear(work._leftSlot);
					else
						_leftView.clear(work._leftSlot / _innerBlocks + 1L, work._leftSlot % _innerBlocks + 1L);
				}
				if(_rightUses.incrementAndGet(work._rightSlot) == _rowBlocks) {
					if(_rightView == null)
						_rightTiles.clear(work._rightSlot);
					else
						_rightView.clear(work._rightSlot / _colBlocks + 1L, work._rightSlot % _colBlocks + 1L);
				}
			}
			partial = payload(work._outputSlot, 1, block, budget);
			OOCFuture<List<Void>> released = work.releaseInputsAsync();
			ManagedPayload<IndexedMatrixValue> result = partial;
			partial = null;
			// wait for closure to not exceed reserved budget
			released.whenComplete((ignored, error) -> {
				if(error != null) {
					result.release();
					budget.close();
					fail(error);
					completeOne();
				}
				else
					reduce(work._outputSlot, result, budget);
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

	private void finishSource() {
		if(_sourceComplete.compareAndSet(false, true))
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
		if(_leftView != null)
			_leftView.close();
		if(_rightView != null)
			_rightView.close();
		if(_leftTiles != null)
			_leftTiles.close();
		if(_rightTiles != null)
			_rightTiles.close();
		if(_accumulators != null)
			_accumulators.close();
		if(_leftReader != null)
			_leftReader.close();
		if(_rightReader != null)
			_rightReader.close();
		if(_leftStore != null)
			_leftStore.close();
		if(_rightStore != null)
			_rightStore.close();
		onComplete();
	}

	private static final class MultiplyWork implements AutoCloseable {
		private final int _outputSlot;
		private final int _leftSlot;
		private final int _rightSlot;
		private StoreLease<IndexedMatrixValue> _left;
		private StoreLease<IndexedMatrixValue> _right;
		private ReservationBudget _budget;

		private MultiplyWork(StoreLease<IndexedMatrixValue> left, StoreLease<IndexedMatrixValue> right, int outputSlot,
			ReservationBudget budget, int leftSlot, int rightSlot) {
			_left = left;
			_right = right;
			_outputSlot = outputSlot;
			_budget = budget;
			_leftSlot = leftSlot;
			_rightSlot = rightSlot;
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
			if(_left != null)
				_left.close();
			if(_right != null)
				_right.close();
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
			if(_existing != null)
				_existing.close();
			if(_budget != null)
				_budget.close();
		}
	}
}
