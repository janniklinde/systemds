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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.IJV;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
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
import org.apache.sysds.utils.stats.InfrastructureAnalyzer;

public final class SparseMatrixVectorOOCPrimitive extends OOCPrimitive {
	private static final long BATCH_BYTES = 16L * 1024 * 1024;
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final long _rowBudget;
	private final AtomicBoolean _cleaned = new AtomicBoolean();
	private final AtomicInteger _activeRows = new AtomicInteger();
	private final AtomicInteger _nextRow = new AtomicInteger();
	private MaterializedStore<IndexedMatrixValue> _matrixStore;
	private MaterializedStore<IndexedMatrixValue> _vectorStore;
	private IndexedMaterializedStoreReader<IndexedMatrixValue> _matrixReader;
	private IndexedMaterializedStoreReader<IndexedMatrixValue> _vectorReader;
	private OOCStream<RowState> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private int _rowBlocks;
	private int _colBlocks;
	private int _blocksize;

	public SparseMatrixVectorOOCPrimitive(OOCStreamable<IndexedMatrixValue> matrix,
		OOCStreamable<IndexedMatrixValue> vector, OOCStreamable<IndexedMatrixValue> output, StreamContext context) {
		super(context, matrix, vector);
		_output = output;
		_rowBudget = MatrixBlock.estimateSizeDenseInMemory(matrix.getDataCharacteristics().getBlocksize(), 1) * 3;
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		return List.of(new OOCMaterializedInputRequest(0, OOCStoreLayout.ROW_MAJOR, 1),
			new OOCMaterializedInputRequest(1, OOCStoreLayout.ROW_MAJOR, 1));
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
		int blocksize = getInput(0).getDataCharacteristics().getBlocksize();
		return _rowBudget + MatrixBlock.estimateSizeDenseInMemory(blocksize, blocksize);
	}

	@Override
	protected void startExecution() {
		DataCharacteristics matrix = getInput(0).getDataCharacteristics();
		_rowBlocks = (int) matrix.getNumRowBlocks();
		_colBlocks = (int) matrix.getNumColBlocks();
		_blocksize = matrix.getBlocksize();
		_outputStream = _output.getWriteStream();
		_ready = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _ready);
		OOCInstructionUtils.submitOOCTasks(_ready, callback -> process(callback.get()), getContext())
			.whenComplete((ignored, error) -> {
				if(error != null)
					fail(error);
				cleanup();
			});
		OOCFuture.allOf(List.of(getMaterializedInput(0), getMaterializedInput(1)), MaterializedStore::close)
			.whenComplete((stores, error) -> {
				if(error != null) {
					fail(error);
					closeReady();
					return;
				}
				_matrixStore = stores.get(0);
				_vectorStore = stores.get(1);
				OOCFuture.allOf(List.of(_matrixStore.completion(), _vectorStore.completion()))
					.whenComplete((ignored, failure) -> storesReady(failure));
			});
	}

	private void storesReady(Throwable error) {
		if(error != null) {
			fail(error);
			closeReady();
			return;
		}
		try {
			_matrixReader = _matrixStore.openIndexedReader(new CountingLiveness(_matrixStore.size(), 1));
			_vectorReader = _vectorStore.openIndexedReader(new CountingLiveness(_vectorStore.size(), _rowBlocks));
			int parallel = Math.min(_rowBlocks, Math.max(1, InfrastructureAnalyzer.getLocalParallelism()));
			_activeRows.set(parallel);
			if(parallel == 0)
				closeReady();
			for(int i = 0; i < parallel; i++)
				startRow();
		}
		catch(Throwable failure) {
			fail(failure);
			closeReady();
		}
	}

	private void startRow() {
		int row = _nextRow.getAndIncrement();
		if(row >= _rowBlocks || hasFailed()) {
			finishRow();
			return;
		}
		_allowance.reserveTaskAsync(_rowBudget).whenComplete((ignored, error) -> {
			if(error != null) {
				fail(error);
				finishRow();
				return;
			}
			ReservationBudget budget = new ReservationBudget(_allowance, _rowBudget).enableReuse().enableGrowth();
			try {
				int rows = (int) Math.min(_blocksize,
					getInput(0).getDataCharacteristics().getRows() - (long) row * _blocksize);
				MatrixBlock accumulator = new MatrixBlock(rows, 1, false);
				accumulator.allocateDenseBlock();
				advance(new RowState(row, accumulator, budget));
			}
			catch(Throwable failure) {
				budget.close();
				fail(failure);
				finishRow();
			}
		});
	}

	private void advance(RowState state) {
		if(hasFailed()) {
			state.close();
			finishRow();
			return;
		}
		if(state._column == _colBlocks) {
			if(!state._batch.isEmpty()) {
				_ready.enqueue(state);
				return;
			}
			try {
				state._accumulator.recomputeNonZeros();
				OOCUtils.enqueueExact(_outputStream,
					new IndexedMatrixValue(new MatrixIndexes(state._row + 1L, 1), state._accumulator), state._budget);
				state._budget = null;
				startRow();
			}
			catch(Throwable failure) {
				state.close();
				fail(failure);
				finishRow();
			}
			return;
		}
		if(state._batchBytes >= BATCH_BYTES) {
			_ready.enqueue(state);
			return;
		}
		boolean admitted = state._batch.isEmpty();
		OOCFuture<StoreLease<IndexedMatrixValue>> matrix = state._pendingMatrix != null ?
			OOCFuture.completed(state._pendingMatrix) : admitted ?
			_matrixReader.request(state._row + 1L, state._column + 1L, state._budget) :
			_matrixReader.requestAvailable(state._row + 1L, state._column + 1L, state._budget);
		OOCFuture<StoreLease<IndexedMatrixValue>> vector = state._pendingVector != null ?
			OOCFuture.completed(state._pendingVector) : admitted ?
			_vectorReader.request(state._column + 1L, 1, state._budget) :
			_vectorReader.requestAvailable(state._column + 1L, 1, state._budget);
		OOCFuture.allOf(List.of(matrix, vector), StoreLease::close)
			.whenComplete((leases, error) -> {
				if(error != null) {
					state.close();
					fail(error);
					finishRow();
					return;
				}
				StoreLease<IndexedMatrixValue> matrixLease = leases.get(0);
				StoreLease<IndexedMatrixValue> vectorLease = leases.get(1);
				if(!admitted && (matrixLease == null || vectorLease == null)) {
					state._pendingMatrix = matrixLease;
					state._pendingVector = vectorLease;
					_ready.enqueue(state);
					return;
				}
				state._column++;
				if(matrixLease != null && vectorLease != null) {
					state._pendingMatrix = null;
					state._pendingVector = null;
					state._batch.add(new BatchItem(matrixLease, vectorLease));
					state._batchBytes += OOCUtils.memoryCharge(matrixLease.value()) +
						OOCUtils.memoryCharge(vectorLease.value());
				}
				else {
					List<OOCFuture<Void>> closed = new ArrayList<>(2);
					if(matrixLease != null)
						closed.add(matrixLease.closeAsync());
					if(vectorLease != null)
						closed.add(vectorLease.closeAsync());
					OOCFuture.allOf(closed).whenComplete((ignored, closeError) -> {
						if(closeError != null) {
							state.close();
							fail(closeError);
							finishRow();
						}
						else
							advance(state);
					});
					return;
				}
				advance(state);
			});
	}

	private void process(RowState state) {
		try {
			for(BatchItem item : state._batch) {
				MatrixBlock matrix = (MatrixBlock) item._matrix.value().getValue();
				MatrixBlock vector = (MatrixBlock) item._vector.value().getValue();
				if(!matrix.isEmptyBlock(false) && !vector.isEmptyBlock(false)) {
					double[] out = state._accumulator.getDenseBlockValues();
					if(matrix.isInSparseFormat()) {
						Iterator<IJV> entries = matrix.getSparseBlock().getIterator();
						while(entries.hasNext()) {
							IJV entry = entries.next();
							out[entry.getI()] += entry.getV() * vector.get(entry.getJ(), 0);
						}
					}
					else {
						DenseBlock dense = matrix.getDenseBlock();
						for(int row = 0; row < matrix.getNumRows(); row++) {
							double[] values = dense.values(row);
							int offset = dense.pos(row);
							for(int col = 0; col < matrix.getNumColumns(); col++)
								out[row] += values[offset + col] * vector.get(col, 0);
						}
					}
				}
			}
			List<OOCFuture<Void>> closed = new ArrayList<>(state._batch.size() * 2);
			for(BatchItem item : state._batch) {
				closed.add(item._matrix.closeAsync());
				closed.add(item._vector.closeAsync());
			}
			state._batch.clear();
			state._batchBytes = 0;
			OOCFuture.allOf(closed).whenComplete((ignored, error) -> {
				if(error != null) {
					state.close();
					fail(error);
					finishRow();
				}
				else
					advance(state);
			});
		}
		catch(Throwable failure) {
			state.close();
			fail(failure);
			finishRow();
		}
	}

	private void finishRow() {
		if(_activeRows.decrementAndGet() == 0)
			closeReady();
	}

	private void closeReady() {
		try {
			_ready.closeInput();
		}
		catch(IllegalStateException ignored) {
		}
	}

	private void cleanup() {
		if(!_cleaned.compareAndSet(false, true))
			return;
		if(_matrixReader != null)
			_matrixReader.close();
		if(_vectorReader != null)
			_vectorReader.close();
		if(_matrixStore != null)
			_matrixStore.close();
		if(_vectorStore != null)
			_vectorStore.close();
		_outputStream.closeInput();
		onComplete();
	}

	private static final class RowState implements AutoCloseable {
		private final int _row;
		private final MatrixBlock _accumulator;
		private int _column;
		private ReservationBudget _budget;
		private final List<BatchItem> _batch = new ArrayList<>();
		private long _batchBytes;
		private StoreLease<IndexedMatrixValue> _pendingMatrix;
		private StoreLease<IndexedMatrixValue> _pendingVector;

		private RowState(int row, MatrixBlock accumulator, ReservationBudget budget) {
			_row = row;
			_accumulator = accumulator;
			_budget = budget;
		}

		@Override
		public void close() {
			if(_pendingMatrix != null)
				_pendingMatrix.close();
			if(_pendingVector != null)
				_pendingVector.close();
			for(BatchItem item : _batch) {
				item._matrix.close();
				item._vector.close();
			}
			if(_budget != null)
				_budget.close();
		}
	}

	private record BatchItem(StoreLease<IndexedMatrixValue> _matrix, StoreLease<IndexedMatrixValue> _vector) {
	}
}
