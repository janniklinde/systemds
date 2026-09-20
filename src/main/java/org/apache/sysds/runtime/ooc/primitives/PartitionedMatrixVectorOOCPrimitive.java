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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.PartitionedStoreStreamable;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.utils.stats.InfrastructureAnalyzer;

public final class PartitionedMatrixVectorOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final AtomicBoolean _cleaned = new AtomicBoolean();
	private final AtomicInteger _next = new AtomicInteger();
	private final AtomicInteger _active = new AtomicInteger();
	private final AtomicBoolean _sourceDone = new AtomicBoolean();
	private final List<OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>>> _vectorBlocks;
	private final Semaphore _packSlots = new Semaphore(Math.max(2, 2 * InfrastructureAnalyzer.getLocalParallelism()));
	private volatile boolean _liveVector;
	private boolean _liveMatrix;
	private MaterializedStore<PackedBlock> _matrixStore;
	private MaterializedStore<IndexedMatrixValue> _vectorStore;
	private IndexedMaterializedStoreReader<PackedBlock> _matrixReader;
	private IndexedMaterializedStoreReader<IndexedMatrixValue> _vectorReader;
	private MatrixBlock[] _accumulators;
	private ReservationBudget _outputBudget;
	private OOCStream<StoreLease<PackedBlock>> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;

	public PartitionedMatrixVectorOOCPrimitive(OOCStreamable<IndexedMatrixValue> matrix,
		OOCStreamable<IndexedMatrixValue> vector, OOCStreamable<IndexedMatrixValue> output, StreamContext context) {
		super(context, matrix, vector);
		_output = output;
		_vectorBlocks = new ArrayList<>((int) matrix.getDataCharacteristics().getNumColBlocks());
		for(int col = 0; col < matrix.getDataCharacteristics().getNumColBlocks(); col++)
			_vectorBlocks.add(new OOCFuture<>());
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		return List.of(new OOCMaterializedInputRequest(1, OOCStoreLayout.ROW_MAJOR, 1,
			this::acceptVector, live -> _liveVector = live));
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern pattern) {
		_pattern = OOCAccessPattern.ROW_MAJOR;
	}

	@Override
	protected long getAllowanceLimit(GlobalMemoryBroker broker) {
		return broker.getAllowedMemory() * 2 / 3;
	}

	@Override
	protected void startExecution() {
		_outputStream = _output.getWriteStream();
		_ready = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _ready);
		OOCInstructionUtils.submitOOCTasks(_ready, callback -> process(callback.get()), getContext())
			.whenComplete((ignored, error) -> {
				if(error != null)
					fail(error);
				cleanup();
			});
		getMaterializedInput(1).whenComplete((store, error) -> {
			if(error != null) {
				fail(error);
				completeVector(error);
			}
			else {
				_vectorStore = store;
				store.completion().whenComplete((ignored, completionError) -> completeVector(completionError));
			}
		});
		prepare();
	}

	private void prepare() {
		DataCharacteristics dc = getInput(0).getDataCharacteristics();
		long bytes = MatrixBlock.estimateSizeDenseInMemory(dc.getBlocksize(), 1) * dc.getNumRowBlocks();
		_allowance.reserveAsync(bytes).whenComplete((ignored, reservationError) -> {
			if(reservationError != null) {
				fail(reservationError);
				_ready.closeInput();
				return;
			}
			_outputBudget = new ReservationBudget(_allowance, bytes);
			try {
				_accumulators = new MatrixBlock[(int) dc.getNumRowBlocks()];
				for(int i = 0; i < _accumulators.length; i++) {
					int rows = (int) Math.min(dc.getBlocksize(), dc.getRows() - (long) i * dc.getBlocksize());
					_accumulators[i] = new MatrixBlock(rows, 1, false);
					_accumulators[i].allocateDenseBlock();
				}
				consumeInputHandle(0);
				PartitionedStoreStreamable matrix = (PartitionedStoreStreamable) getInput(0);
				_liveMatrix = matrix.registerLiveConsumer(this::receivePack);
				matrix.acquirePartitions().whenComplete((store, error) -> {
					if(error != null) {
						fail(error);
						_ready.closeInput();
						return;
					}
					_matrixStore = store;
					store.completion().whenComplete((completed, completionError) -> {
						if(completionError != null)
							fail(completionError);
						if(_liveMatrix) {
							_sourceDone.set(true);
							if(_active.get() == 0)
								emit();
						}
						else if(completionError == null)
							startReplay();
						else
							_ready.closeInput();
					});
				});
			}
			catch(Throwable failure) {
				fail(failure);
				_ready.closeInput();
			}
		});
	}

	private void acceptVector(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		try(callback) {
			if(callback.isFailure()) {
				callback.get();
				return;
			}
			if(callback.isEos())
				return;
			if(_cleaned.get())
				return;
			int col = (int) callback.get().getIndexes().getRowIndex() - 1;
			OOCStream.QueueCallback<IndexedMatrixValue> held = callback.keepOpen();
			if(!_vectorBlocks.get(col).complete(held))
				held.close();
		}
		catch(Throwable error) {
			fail(error);
			completeVector(error);
		}
	}

	private void completeVector(Throwable error) {
		if(_cleaned.get())
			return;
		if(error != null) {
			fail(error);
			for(OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future : _vectorBlocks)
				future.completeExceptionally(error);
			return;
		}
		if(_liveVector) {
			for(OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future : _vectorBlocks)
				future.complete(null);
			return;
		}
		_vectorReader = _vectorStore.openIndexedReader(new CountingLiveness(_vectorStore.size(), 1));
		for(int col = 0; col < _vectorBlocks.size(); col++) {
			int index = col;
			_vectorReader.request(col + 1L, 1, _allowance).whenComplete((lease, readError) -> {
				if(readError != null) {
					fail(readError);
					_vectorBlocks.get(index).completeExceptionally(readError);
				}
				else
					_vectorBlocks.get(index).complete(lease == null ? null : new MaterializedCallback<>(lease));
			});
		}
	}

	private void receivePack(StoreLease<PackedBlock> lease) {
		if(hasFailed()) {
			lease.close();
			return;
		}
		_packSlots.acquireUninterruptibly();
		if(hasFailed()) {
			_packSlots.release();
			lease.close();
			return;
		}
		_active.incrementAndGet();
		schedulePack(lease);
	}

	private void schedulePack(StoreLease<PackedBlock> lease) {
		try {
			Set<Integer> columns = new HashSet<>();
			PackedBlock pack = lease.value();
			for(int i = 0; i < pack.count(); i++)
				columns.add((int) ((IndexedMatrixValue) pack.value(i)).getIndexes().getColumnIndex() - 1);
			List<OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>>> required = new ArrayList<>();
			for(int col : columns)
				required.add(_vectorBlocks.get(col));
			OOCFuture.allOf(required).whenComplete((ignored, error) -> {
				if(error != null || hasFailed()) {
					if(error != null)
						fail(error);
					lease.close();
					completePack();
				}
				else
					_ready.enqueue(lease);
			});
		}
		catch(Throwable error) {
			fail(error);
			lease.close();
			completePack();
		}
	}

	private void startReplay() {
		_matrixReader = _matrixStore.openIndexedReader(new CountingLiveness(_matrixStore.size(), 1));
		int parallel = Math.min(_matrixStore.size(), InfrastructureAnalyzer.getLocalParallelism());
		_active.set(parallel);
		if(parallel == 0)
			emit();
		for(int i = 0; i < parallel; i++)
			next();
	}

	private void next() {
		int index = _next.getAndIncrement();
		if(hasFailed() || index >= _matrixStore.size()) {
			if(_active.decrementAndGet() == 0)
				emit();
			return;
		}
		_matrixReader.request(index, _allowance).whenComplete((lease, error) -> {
			if(error != null) {
				fail(error);
				next();
			}
			else
				schedulePack(lease);
		});
	}

	private void process(StoreLease<PackedBlock> lease) {
		try {
			PackedBlock pack = lease.value();
			for(int i = 0; i < pack.count(); i++) {
				IndexedMatrixValue indexed = (IndexedMatrixValue) pack.value(i);
				MatrixBlock matrix = (MatrixBlock) indexed.getValue();
				if(matrix.isEmptyBlock(false))
					continue;
				OOCStream.QueueCallback<IndexedMatrixValue> rightCallback = _vectorBlocks
					.get((int) indexed.getIndexes().getColumnIndex() - 1).getNow(null);
				if(rightCallback == null)
					continue;
				MatrixBlock right = (MatrixBlock) rightCallback.get().getValue();
				MatrixBlock accumulator = _accumulators[(int) indexed.getIndexes().getRowIndex() - 1];
				synchronized(accumulator) {
					double[] out = accumulator.getDenseBlockValues();
					if(matrix.isInSparseFormat()) {
						Iterator<IJV> entries = matrix.getSparseBlock().getIterator();
						while(entries.hasNext()) {
							IJV entry = entries.next();
							out[entry.getI()] += entry.getV() * right.get(entry.getJ(), 0);
						}
					}
					else {
						for(int row = 0; row < matrix.getNumRows(); row++)
							for(int col = 0; col < matrix.getNumColumns(); col++)
								out[row] += matrix.get(row, col) * right.get(col, 0);
					}
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
				completePack();
			});
		}
	}

	private void completePack() {
		if(_liveMatrix) {
			_packSlots.release();
			if(_active.decrementAndGet() == 0 && _sourceDone.get())
				emit();
		}
		else
			next();
	}

	private void emit() {
		try {
			if(!hasFailed()) {
				for(int row = 0; row < _accumulators.length; row++) {
					MatrixBlock block = _accumulators[row];
					block.recomputeNonZeros();
					IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(row + 1L, 1), block);
					_outputBudget.reserveBlocking(value.size());
					OOCUtils.enqueueExact(_outputStream, value, new ReservationBudget(_outputBudget, value.size()));
				}
			}
		}
		catch(Throwable error) {
			fail(error);
		}
		finally {
			_ready.closeInput();
		}
	}

	private void cleanup() {
		if(!_cleaned.compareAndSet(false, true))
			return;
		for(OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future : _vectorBlocks) {
			future.whenComplete((vector, error) -> {
				if(vector != null)
					vector.close();
			});
		}
		if(_vectorReader != null)
			_vectorReader.close();
		if(_matrixReader != null)
			_matrixReader.close();
		if(_vectorStore != null)
			_vectorStore.close();
		if(_outputBudget != null)
			_outputBudget.close();
		((PartitionedStoreStreamable) getInput(0)).releasePartitions();
		_outputStream.closeInput();
		onComplete();
	}
}
