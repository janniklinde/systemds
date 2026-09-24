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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
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
import org.apache.sysds.runtime.ooc.cache.packed.PackedBlock;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.PartitionedStoreStreamable;
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
	private final AtomicInteger _partitionNext = new AtomicInteger();
	private final AtomicInteger _partitionActive = new AtomicInteger();
	private final AtomicBoolean _partitionSourceDone = new AtomicBoolean();
	private final List<OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>>> _partitionVectorBlocks;
	private final Semaphore _partitionSlots =
		new Semaphore(Math.max(2, 2 * InfrastructureAnalyzer.getLocalParallelism()));
	private volatile boolean _partitionLiveVector;
	private boolean _partitionLiveMatrix;
	private MaterializedStore<PackedBlock> _partitionMatrixStore;
	private IndexedMaterializedStoreReader<PackedBlock> _partitionMatrixReader;
	private MatrixBlock[] _partitionAccumulators;
	private ReservationBudget _partitionOutputBudget;
	private OOCStream<StoreLease<PackedBlock>> _partitionReady;

	public SparseMatrixVectorOOCPrimitive(OOCStreamable<IndexedMatrixValue> matrix,
		OOCStreamable<IndexedMatrixValue> vector, OOCStreamable<IndexedMatrixValue> output, StreamContext context) {
		super(context, matrix, vector);
		_output = output;
		_rowBudget = MatrixBlock.estimateSizeDenseInMemory(matrix.getDataCharacteristics().getBlocksize(), 1) * 3;
		_partitionVectorBlocks = new ArrayList<>((int) matrix.getDataCharacteristics().getNumColBlocks());
		for(int col = 0; col < matrix.getDataCharacteristics().getNumColBlocks(); col++)
			_partitionVectorBlocks.add(new OOCFuture<>());
	}

	@Override
	public boolean supportsPartitionedInput(int index) {
		return index == 0;
	}

	private boolean usePartitions() {
		return getInput(0) instanceof PartitionedStoreStreamable source && source.partitionsSelected();
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		if(usePartitions())
			return List.of(new OOCMaterializedInputRequest(1, OOCStoreLayout.ROW_MAJOR, 1,
				this::acceptPartitionVector, live -> _partitionLiveVector = live));
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
		if(usePartitions())
			return 0;
		int blocksize = getInput(0).getDataCharacteristics().getBlocksize();
		return _rowBudget + MatrixBlock.estimateSizeDenseInMemory(blocksize, blocksize);
	}

	@Override
	protected long getAllowanceLimit(GlobalMemoryBroker broker) {
		return usePartitions() ? broker.getAllowedMemory() * 2 / 3 : super.getAllowanceLimit(broker);
	}

	@Override
	protected void startExecution() {
		if(usePartitions()) {
			startPartitionedExecution();
			return;
		}
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

	private void startPartitionedExecution() {
		_outputStream = _output.getWriteStream();
		_partitionReady = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _partitionReady);
		OOCInstructionUtils.submitOOCTasks(_partitionReady, callback -> processPartition(callback.get()), getContext())
			.whenComplete((ignored, error) -> {
				if(error != null)
					fail(error);
				cleanupPartitioned();
			});
		getMaterializedInput(1).whenComplete((store, error) -> {
			if(error != null) {
				fail(error);
				completePartitionVector(error);
			}
			else {
				_vectorStore = store;
				store.completion().whenComplete((ignored, completionError) ->
					completePartitionVector(completionError));
			}
		});
		preparePartitions();
	}

	private void preparePartitions() {
		DataCharacteristics dc = getInput(0).getDataCharacteristics();
		long bytes = MatrixBlock.estimateSizeDenseInMemory(dc.getBlocksize(), 1) * dc.getNumRowBlocks();
		_allowance.reserveAsync(bytes).whenComplete((ignored, reservationError) -> {
			if(reservationError != null) {
				fail(reservationError);
				_partitionReady.closeInput();
				return;
			}
			_partitionOutputBudget = new ReservationBudget(_allowance, bytes);
			try {
				_partitionAccumulators = new MatrixBlock[(int) dc.getNumRowBlocks()];
				for(int i = 0; i < _partitionAccumulators.length; i++) {
					int rows = (int) Math.min(dc.getBlocksize(), dc.getRows() - (long) i * dc.getBlocksize());
					_partitionAccumulators[i] = new MatrixBlock(rows, 1, false);
					_partitionAccumulators[i].allocateDenseBlock();
				}
				consumeInputHandle(0);
				PartitionedStoreStreamable matrix = (PartitionedStoreStreamable) getInput(0);
				_partitionLiveMatrix = matrix.registerLiveConsumer(this::receivePartition);
				matrix.acquirePartitions().whenComplete((store, error) -> {
					if(error != null) {
						fail(error);
						_partitionReady.closeInput();
						return;
					}
					_partitionMatrixStore = store;
					store.completion().whenComplete((completed, completionError) -> {
						if(completionError != null)
							fail(completionError);
						if(_partitionLiveMatrix) {
							_partitionSourceDone.set(true);
							if(_partitionActive.get() == 0)
								emitPartitions();
						}
						else if(completionError == null)
							startPartitionReplay();
						else
							_partitionReady.closeInput();
					});
				});
			}
			catch(Throwable failure) {
				fail(failure);
				_partitionReady.closeInput();
			}
		});
	}

	private void acceptPartitionVector(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		try(callback) {
			if(callback.isFailure()) {
				callback.get();
				return;
			}
			if(callback.isEos() || _cleaned.get())
				return;
			int col = (int) callback.get().getIndexes().getRowIndex() - 1;
			OOCStream.QueueCallback<IndexedMatrixValue> held = callback.keepOpen();
			if(!_partitionVectorBlocks.get(col).complete(held))
				held.close();
		}
		catch(Throwable error) {
			fail(error);
			completePartitionVector(error);
		}
	}

	private void completePartitionVector(Throwable error) {
		if(_cleaned.get())
			return;
		if(error != null) {
			fail(error);
			for(OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future : _partitionVectorBlocks)
				future.completeExceptionally(error);
			return;
		}
		if(_partitionLiveVector) {
			for(OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future : _partitionVectorBlocks)
				future.complete(null);
			return;
		}
		_vectorReader = _vectorStore.openIndexedReader(new CountingLiveness(_vectorStore.size(), 1));
		for(int col = 0; col < _partitionVectorBlocks.size(); col++) {
			int index = col;
			_vectorReader.request(col + 1L, 1, _allowance).whenComplete((lease, readError) -> {
				if(readError != null) {
					fail(readError);
					_partitionVectorBlocks.get(index).completeExceptionally(readError);
				}
				else
					_partitionVectorBlocks.get(index).complete(lease == null ? null : new MaterializedCallback<>(lease));
			});
		}
	}

	private void receivePartition(StoreLease<PackedBlock> lease) {
		if(hasFailed()) {
			lease.close();
			return;
		}
		_partitionSlots.acquireUninterruptibly();
		if(hasFailed()) {
			_partitionSlots.release();
			lease.close();
			return;
		}
		_partitionActive.incrementAndGet();
		schedulePartition(lease);
	}

	private void schedulePartition(StoreLease<PackedBlock> lease) {
		try {
			Set<Integer> columns = new HashSet<>();
			PackedBlock pack = lease.value();
			for(int i = 0; i < pack.count(); i++)
				columns.add((int) ((IndexedMatrixValue) pack.value(i)).getIndexes().getColumnIndex() - 1);
			List<OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>>> required = new ArrayList<>();
			for(int col : columns)
				required.add(_partitionVectorBlocks.get(col));
			OOCFuture.allOf(required).whenComplete((ignored, error) -> {
				if(error != null || hasFailed()) {
					if(error != null)
						fail(error);
					lease.close();
					completePartition();
				}
				else
					_partitionReady.enqueue(lease);
			});
		}
		catch(Throwable error) {
			fail(error);
			lease.close();
			completePartition();
		}
	}

	private void startPartitionReplay() {
		_partitionMatrixReader = _partitionMatrixStore.openIndexedReader(
			new CountingLiveness(_partitionMatrixStore.size(), 1));
		((PartitionedStoreStreamable) getInput(0)).partitionReaderOpened();
		int parallel = Math.min(_partitionMatrixStore.size(), InfrastructureAnalyzer.getLocalParallelism());
		_partitionActive.set(parallel);
		if(parallel == 0)
			emitPartitions();
		for(int i = 0; i < parallel; i++)
			nextPartition();
	}

	private void nextPartition() {
		int index = _partitionNext.getAndIncrement();
		if(hasFailed() || index >= _partitionMatrixStore.size()) {
			if(_partitionActive.decrementAndGet() == 0)
				emitPartitions();
			return;
		}
		_partitionMatrixReader.request(index, _allowance).whenComplete((lease, error) -> {
			if(error != null) {
				fail(error);
				nextPartition();
			}
			else
				schedulePartition(lease);
		});
	}

	private void processPartition(StoreLease<PackedBlock> lease) {
		try {
			PackedBlock pack = lease.value();
			for(int i = 0; i < pack.count(); i++) {
				IndexedMatrixValue indexed = (IndexedMatrixValue) pack.value(i);
				MatrixBlock matrix = (MatrixBlock) indexed.getValue();
				if(matrix.isEmptyBlock(false))
					continue;
				OOCStream.QueueCallback<IndexedMatrixValue> rightCallback = _partitionVectorBlocks
					.get((int) indexed.getIndexes().getColumnIndex() - 1).getNow(null);
				if(rightCallback == null)
					continue;
				MatrixBlock right = (MatrixBlock) rightCallback.get().getValue();
				MatrixBlock accumulator = _partitionAccumulators[(int) indexed.getIndexes().getRowIndex() - 1];
				synchronized(accumulator) {
					double[] out = accumulator.getDenseBlockValues();
					if(matrix.isInSparseFormat()) {
						Iterator<IJV> entries = matrix.getSparseBlock().getIterator();
						while(entries.hasNext()) {
							IJV entry = entries.next();
							out[entry.getI()] += entry.getV() * right.get(entry.getJ(), 0);
						}
					}
					else
						for(int row = 0; row < matrix.getNumRows(); row++)
							for(int col = 0; col < matrix.getNumColumns(); col++)
								out[row] += matrix.get(row, col) * right.get(col, 0);
				}
			}
		}
		catch(Throwable error) {
			fail(error);
		}
		finally {
			lease.closeAsync().whenComplete((ignored, error) -> {
				if(error != null)
					fail(error);
				completePartition();
			});
		}
	}

	private void completePartition() {
		if(_partitionLiveMatrix) {
			_partitionSlots.release();
			if(_partitionActive.decrementAndGet() == 0 && _partitionSourceDone.get())
				emitPartitions();
		}
		else
			nextPartition();
	}

	private void emitPartitions() {
		try {
			if(!hasFailed())
				for(int row = 0; row < _partitionAccumulators.length; row++) {
					MatrixBlock block = _partitionAccumulators[row];
					block.recomputeNonZeros();
					IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(row + 1L, 1), block);
					_partitionOutputBudget.reserveBlocking(value.size());
					OOCUtils.enqueueExact(_outputStream, value,
						new ReservationBudget(_partitionOutputBudget, value.size()));
				}
		}
		catch(Throwable error) {
			fail(error);
		}
		finally {
			_partitionReady.closeInput();
		}
	}

	private void cleanupPartitioned() {
		if(!_cleaned.compareAndSet(false, true))
			return;
		for(OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future : _partitionVectorBlocks)
			future.whenComplete((vector, error) -> {
				if(vector != null)
					vector.close();
			});
		if(_vectorReader != null)
			_vectorReader.close();
		if(_partitionMatrixReader != null)
			_partitionMatrixReader.close();
		if(_partitionMatrixReader != null)
			((PartitionedStoreStreamable) getInput(0)).partitionReaderClosed();
		if(_vectorStore != null)
			_vectorStore.close();
		if(_partitionOutputBudget != null)
			_partitionOutputBudget.close();
		((PartitionedStoreStreamable) getInput(0)).releasePartitions();
		_outputStream.closeInput();
		onComplete();
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
		long maxTileCharge = 0;
		for(int col = 0; col < _colBlocks; col++)
			maxTileCharge = Math.max(maxTileCharge, _matrixReader.getPinCharge(row + 1L, col + 1L));
		long rowReservation = _rowBudget + maxTileCharge;
		_allowance.reserveTaskAsync(rowReservation).whenComplete((ignored, error) -> {
			if(error != null) {
				fail(error);
				finishRow();
				return;
			}
			ReservationBudget budget = new ReservationBudget(_allowance, rowReservation).enableReuse().enableGrowth();
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
