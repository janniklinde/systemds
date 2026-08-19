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
import java.util.function.BiConsumer;
import java.util.function.ToIntFunction;

import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
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
import org.apache.sysds.runtime.util.CommonThreadPool;

public final class RepartitionOOCPrimitive extends OOCPrimitive {
	private static final BinaryOperator PLUS = new BinaryOperator(Plus.getPlusFnObject());

	private final OOCStreamable<IndexedMatrixValue> _output;
	private final ToIntFunction<MatrixIndexes> _expectedFragments;
	private final List<BiConsumer<IndexedMatrixValue, FragmentEmitter>> _routers;
	private final AtomicInteger _active;
	private StateTable<IndexedMatrixValue> _accumulators;
	private OOCStream<ComputeWork> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private int _outputColBlocks;
	private long _taskBytes;

	public RepartitionOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<IndexedMatrixValue> output,
		ToIntFunction<MatrixIndexes> expectedFragments, BiConsumer<IndexedMatrixValue, FragmentEmitter> router,
		StreamContext context) {
		this(List.of(input), output, expectedFragments, List.of(router), context);
	}

	public RepartitionOOCPrimitive(List<? extends OOCStreamable<IndexedMatrixValue>> inputs,
		OOCStreamable<IndexedMatrixValue> output, ToIntFunction<MatrixIndexes> expectedFragments,
		List<BiConsumer<IndexedMatrixValue, FragmentEmitter>> routers, StreamContext context) {
		super(context, inputs.toArray(OOCStreamable<?>[]::new));
		if(inputs.isEmpty() || inputs.size() != routers.size())
			throw new IllegalArgumentException("Repartition requires one router for each input");
		_output = output;
		_expectedFragments = expectedFragments;
		_routers = List.copyOf(routers);
		_active = new AtomicInteger(inputs.size());
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.ANY;
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = OOCAccessPattern.ANY;
	}

	@Override
	protected void startExecution() {
		DataCharacteristics outputDC = _output.getDataCharacteristics();
		_outputColBlocks = Math.toIntExact(outputDC.getNumColBlocks());
		long outputBytes = OOCUtils.estimateOutputTileBytes(outputDC);
		_taskBytes = OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(outputBytes) + 4 * outputBytes;
		_accumulators = new StateTable<>(OOCCacheManager.getGlobalCache(), CachingStream._streamSeq.getNextID());
		_accumulators.addEvictionPolicy(slot -> slot - outputDC.getNumBlocks());
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
					_accumulators.close();
					onComplete();
				}
			});

		for(int i = 0; i < _routers.size(); i++) {
			OOCStream<IndexedMatrixValue> input = getInputReadStream(i);
			BiConsumer<IndexedMatrixValue, FragmentEmitter> router = _routers.get(i);
			CommonThreadPool.getDynamicPool().submit(() -> {
				try {
					drive(input, router);
				}
				catch(Throwable error) {
					fail(error);
				}
				finally {
					completeOne();
				}
			});
		}
	}

	private void drive(OOCStream<IndexedMatrixValue> input, BiConsumer<IndexedMatrixValue, FragmentEmitter> router) {
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while(!hasFailed() && (callback = input.dequeueCB()) != null)
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				router.accept(current.get(), new FragmentEmitter() {
					@Override
					public void copy(MatrixIndexes outputIndex, int srcRow, int srcCol, int rows, int cols, int dstRow,
						int dstCol) {
						admit(current, outputIndex, srcRow, srcCol, rows, cols, dstRow, dstCol, false, 1);
					}

					@Override
					public void copyRowToColumn(MatrixIndexes outputIndex, int srcRow, int srcCol, int length,
						int dstRow) {
						copyToColumn(outputIndex, srcRow, srcCol, 1, length, dstRow);
					}

					@Override
					public void copyToColumn(MatrixIndexes outputIndex, int srcRow, int srcCol, int rows, int cols,
						int dstRow) {
						admit(current, outputIndex, srcRow, srcCol, rows, cols, dstRow, 0, true, rows);
					}
				});
			}
	}

	private void admit(OOCStream.QueueCallback<IndexedMatrixValue> input, MatrixIndexes outputIndex, int srcRow,
		int srcCol, int rows, int cols, int dstRow, int dstCol, boolean rowToColumn, int count) {
		OOCStream.QueueCallback<IndexedMatrixValue> retained = input.keepOpen();
		ReservationBudget budget = null;
		boolean active = false;
		try {
			_allowance.reserveAsync(_taskBytes).get();
			budget = new ReservationBudget(_allowance, _taskBytes).enableReuse();
			int slot = Math
				.toIntExact((outputIndex.getRowIndex() - 1) * _outputColBlocks + outputIndex.getColumnIndex() - 1);
			_active.incrementAndGet();
			active = true;
			_ready.enqueue(new FragmentWork(retained, slot, srcRow, srcCol, rows, cols, dstRow, dstCol, rowToColumn,
				count, budget));
		}
		catch(Throwable failure) {
			retained.close();
			if(budget != null)
				budget.close();
			if(active)
				completeOne();
			fail(failure);
			throw new RuntimeException(failure);
		}
	}

	private void process(ComputeWork work) {
		if(work instanceof FragmentWork fragment)
			copy(fragment);
		else
			merge((MergeWork) work);
	}

	private void copy(FragmentWork work) {
		ReservationBudget budget = work.takeBudget();
		try {
			MatrixBlock source = (MatrixBlock) work._input.get().getValue();
			MatrixBlock slice = source.slice(work._srcRow, work._srcRow + work._rows - 1, work._srcCol,
				work._srcCol + work._cols - 1);
			int fragmentRows = work._rows;
			int fragmentCols = work._cols;
			if(work._rowToColumn) {
				slice = slice.reshape(work._rows * work._cols, 1, true);
				fragmentRows = work._rows * work._cols;
				fragmentCols = 1;
			}
			DataCharacteristics outputDC = _output.getDataCharacteristics();
			int row = work._slot / _outputColBlocks;
			int col = work._slot % _outputColBlocks;
			int outputRows = (int) Math.min(outputDC.getBlocksize(),
				outputDC.getRows() - (long) row * outputDC.getBlocksize());
			int outputCols = (int) Math.min(outputDC.getBlocksize(),
				outputDC.getCols() - (long) col * outputDC.getBlocksize());
			boolean sparse = source.isInSparseFormat() &&
				MatrixBlock.evalSparseFormatInMemory(outputRows, outputCols, slice.getNonZeros());
			MatrixBlock block = new MatrixBlock(outputRows, outputCols, sparse);
			block.copy(work._dstRow, work._dstRow + fragmentRows - 1, work._dstCol, work._dstCol + fragmentCols - 1,
				slice, false);
			block.recomputeNonZeros();
			block.examSparsity();
			work.releaseInput();
			ManagedPayload<IndexedMatrixValue> result = payload(work._slot, work._count, block, budget);
			reduce(work._slot, result, budget);
		}
		catch(Throwable failure) {
			budget.close();
			fail(failure);
			completeOne();
		}
	}

	private void reduce(int slot, ManagedPayload<IndexedMatrixValue> incoming, ReservationBudget budget) {
		MatrixIndexes outputIndex = new MatrixIndexes(slot / _outputColBlocks + 1L, slot % _outputColBlocks + 1L);
		int expected = _expectedFragments.applyAsInt(outputIndex);
		if(expected <= 0 || count(incoming.value()) > expected) {
			incoming.release();
			budget.close();
			fail(new IllegalStateException("Invalid fragment count for output block " + outputIndex));
			completeOne();
			return;
		}
		if(count(incoming.value()) == expected) {
			try {
				MatrixBlock block = (MatrixBlock) incoming.value().getValue();
				incoming.release();
				OOCUtils.enqueueExact(_outputStream, new IndexedMatrixValue(outputIndex, block), budget);
			}
			catch(Throwable failure) {
				incoming.release();
				budget.close();
				fail(failure);
			}
			completeOne();
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
			MatrixBlock block = ((MatrixBlock) existing.getValue()).binaryOperations(PLUS, incoming.getValue(),
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
			// Failure propagation may already have closed the work stream.
		}
	}

	public interface FragmentEmitter {
		void copy(MatrixIndexes outputIndex, int srcRow, int srcCol, int rows, int cols, int dstRow, int dstCol);

		void copyRowToColumn(MatrixIndexes outputIndex, int srcRow, int srcCol, int length, int dstRow);

		void copyToColumn(MatrixIndexes outputIndex, int srcRow, int srcCol, int rows, int cols, int dstRow);
	}

	private interface ComputeWork extends AutoCloseable {
	}

	private static final class FragmentWork implements ComputeWork {
		private OOCStream.QueueCallback<IndexedMatrixValue> _input;
		private final int _slot;
		private final int _srcRow;
		private final int _srcCol;
		private final int _rows;
		private final int _cols;
		private final int _dstRow;
		private final int _dstCol;
		private final boolean _rowToColumn;
		private final int _count;
		private ReservationBudget _budget;

		private FragmentWork(OOCStream.QueueCallback<IndexedMatrixValue> input, int slot, int srcRow, int srcCol,
			int rows, int cols, int dstRow, int dstCol, boolean rowToColumn, int count, ReservationBudget budget) {
			_input = input;
			_slot = slot;
			_srcRow = srcRow;
			_srcCol = srcCol;
			_rows = rows;
			_cols = cols;
			_dstRow = dstRow;
			_dstCol = dstCol;
			_rowToColumn = rowToColumn;
			_count = count;
			_budget = budget;
		}

		private ReservationBudget takeBudget() {
			ReservationBudget budget = _budget;
			_budget = null;
			return budget;
		}

		private void releaseInput() {
			if(_input != null) {
				_input.close();
				_input = null;
			}
		}

		@Override
		public void close() {
			releaseInput();
			if(_budget != null) {
				_budget.close();
				_budget = null;
			}
		}
	}

	private static final class MergeWork implements ComputeWork {
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
			if(_budget != null) {
				_budget.close();
				_budget = null;
			}
		}
	}
}
