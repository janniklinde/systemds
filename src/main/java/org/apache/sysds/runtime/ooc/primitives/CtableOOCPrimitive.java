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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public final class CtableOOCPrimitive extends OOCPrimitive {
	private static final BinaryOperator PLUS = new BinaryOperator(Plus.getPlusFnObject());

	private final List<OOCStreamable<IndexedMatrixValue>> _inputs;
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final double _secondScalar;
	private final double _weight;
	private final AtomicInteger _active = new AtomicInteger(1);
	private final AtomicBoolean _flushing = new AtomicBoolean();
	private final AtomicBoolean _flushPending = new AtomicBoolean();
	private final AtomicBoolean _schedulePending = new AtomicBoolean();
	private final AtomicBoolean _finished = new AtomicBoolean();
	private List<MaterializedStore<IndexedMatrixValue>> _stores;
	private List<IndexedMaterializedStoreReader<IndexedMatrixValue>> _readers;
	private StateTable<IndexedMatrixValue> _accumulators;
	private OOCStream<ComputeWork> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private int _inputColBlocks;
	private int _outputColBlocks;
	private int _outputBlocks;
	private volatile int _nextTile;
	private volatile int _flushSlot;
	private int _inputTiles;
	private long _taskBytes;
	private long _outputBytes;

	public CtableOOCPrimitive(List<OOCStreamable<IndexedMatrixValue>> inputs, OOCStreamable<IndexedMatrixValue> output,
		double secondScalar, double weight, StreamContext context) {
		super(context, inputs.toArray(OOCStreamable<?>[]::new));
		if(inputs.isEmpty() || inputs.size() > 2)
			throw new IllegalArgumentException("SliceLine ctable requires one or two matrix inputs.");
		_inputs = List.copyOf(inputs);
		_output = output;
		_secondScalar = secondScalar;
		_weight = weight;
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		List<OOCMaterializedInputRequest> requests = new ArrayList<>(_inputs.size());
		for(int i = 0; i < _inputs.size(); i++)
			requests.add(new OOCMaterializedInputRequest(i, OOCStoreLayout.ROW_MAJOR, 1));
		return requests;
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
	}

	@Override
	protected void startExecution() {
		DataCharacteristics input = _inputs.get(0).getDataCharacteristics();
		DataCharacteristics output = _output.getDataCharacteristics();
		_inputColBlocks = Math.toIntExact(input.getNumColBlocks());
		_inputTiles = Math.toIntExact(input.getNumBlocks());
		_outputColBlocks = Math.toIntExact(output.getNumColBlocks());
		_outputBlocks = Math.toIntExact(output.getNumBlocks());
		long inputBytes = OOCUtils.estimateFullTileBytes(input);
		_outputBytes = OOCUtils.estimateFullTileBytes(output);
		_taskBytes = _inputs.size() * OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(inputBytes) +
			4 * OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(_outputBytes);
		_accumulators = new StateTable<>(OOCCacheManager.getGlobalCache(), CachingStream._streamSeq.getNextID(),
			_outputBlocks);
		_outputStream = _output.getWriteStream();
		_ready = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _ready);
		OOCInstructionUtils.submitCloseableOOCTasks(_ready, this::process, getContext())
			.whenComplete((ignored, error) -> {
				if(error != null) {
					fail(error);
					finish();
				}
				else
					flush(0);
			});

		List<OOCFuture<MaterializedStore<IndexedMatrixValue>>> futures = new ArrayList<>(_inputs.size());
		for(int i = 0; i < _inputs.size(); i++)
			futures.add(getMaterializedInput(i));
		OOCFuture.allOf(futures, MaterializedStore::close).whenComplete(this::storesReady);
	}

	private void storesReady(List<MaterializedStore<IndexedMatrixValue>> stores, Throwable error) {
		if(error != null) {
			fail(error);
			completeOne();
			return;
		}
		_stores = stores;
		List<OOCFuture<Void>> completions = stores.stream().map(MaterializedStore::completion).toList();
		OOCFuture.allOf(completions, ignored -> {
		}).whenComplete((ignored, completionError) -> {
			if(completionError != null) {
				fail(completionError);
				completeOne();
				return;
			}
			_readers = new ArrayList<>(stores.size());
			for(MaterializedStore<IndexedMatrixValue> store : stores)
				_readers.add(store.openIndexedReader(new CountingLiveness(store.size(), 1)));
			scheduleNext();
		});
	}

	private void scheduleNext() {
		while(true) {
			if(hasFailed() || _nextTile == _inputTiles) {
				completeOne();
				return;
			}
			_schedulePending.set(true);
			scheduleTile();
			if(_schedulePending.compareAndSet(true, false))
				return;
		}
	}

	private void resumeSchedule() {
		if(!_schedulePending.compareAndSet(true, false))
			scheduleNext();
	}

	private void scheduleTile() {
		_allowance.reserveAsync(_taskBytes).whenComplete((ignored, error) -> {
			if(error != null) {
				fail(error);
				completeOne();
				return;
			}
			ReservationBudget budget = new ReservationBudget(_allowance, _taskBytes).enableReuse();
			int tile = _nextTile++;
			_active.incrementAndGet();
			requestInputs(tile, budget);
			resumeSchedule();
		});
	}

	private void requestInputs(int tile, ReservationBudget budget) {
		long row = tile / _inputColBlocks + 1L;
		long col = tile % _inputColBlocks + 1L;
		List<OOCFuture<StoreLease<IndexedMatrixValue>>> requests = new ArrayList<>(_readers.size());
		for(IndexedMaterializedStoreReader<IndexedMatrixValue> reader : _readers)
			requests.add(reader.request(row, col, budget));
		OOCFuture.allOf(requests, StoreLease::close).whenComplete((leases, error) -> {
			if(error != null) {
				budget.close();
				fail(error);
				completeOne();
				return;
			}
			try {
				_ready.enqueue(new BuildWork(leases, budget));
			}
			catch(Throwable failure) {
				leases.forEach(StoreLease::close);
				budget.close();
				fail(failure);
				completeOne();
			}
		});
	}

	private void process(ComputeWork work) {
		if(work instanceof BuildWork build)
			build(build);
		else if(work instanceof NextWork next)
			next(next._batch);
		else
			merge((MergeWork) work);
	}

	private void build(BuildWork work) {
		ReservationBudget budget = work._budget;
		try {
			MatrixBlock first = (MatrixBlock) work._inputs.get(0).value().getValue();
			MatrixBlock second = work._inputs.size() == 2 ? (MatrixBlock) work._inputs.get(1).value().getValue() : null;
			if(second != null &&
				(first.getNumRows() != second.getNumRows() || first.getNumColumns() != second.getNumColumns()))
				throw new IllegalArgumentException("Ctable input blocks are not aligned.");
			Map<Integer, MatrixBlock> partials = new HashMap<>();
			DataCharacteristics output = _output.getDataCharacteristics();
			int blocksize = output.getBlocksize();
			for(int row = 0; row < first.getNumRows(); row++)
				for(int col = 0; col < first.getNumColumns(); col++) {
					long outputRow = (long) first.get(row, col) - 1;
					long outputCol = (long) (second != null ? second.get(row, col) : _secondScalar) - 1;
					if(outputRow < 0 || outputCol < 0 || outputRow >= output.getRows() || outputCol >= output.getCols())
						continue;
					int blockRow = (int) (outputRow / blocksize);
					int blockCol = (int) (outputCol / blocksize);
					int slot = blockRow * _outputColBlocks + blockCol;
					MatrixBlock partial = partials.computeIfAbsent(slot,
						ignored -> new MatrixBlock(
							(int) Math.min(blocksize, output.getRows() - (long) blockRow * blocksize),
							(int) Math.min(blocksize, output.getCols() - (long) blockCol * blocksize), true));
					int localRow = (int) (outputRow % blocksize);
					int localCol = (int) (outputCol % blocksize);
					partial.set(localRow, localCol, partial.get(localRow, localCol) + _weight);
				}
			for(MatrixBlock partial : partials.values()) {
				partial.recomputeNonZeros();
				partial.examSparsity();
			}
			OOCFuture.allOf(work._inputs.stream().map(StoreLease::closeAsync).toList(), ignored -> {
			}).whenComplete((ignored, error) -> {
				if(error != null) {
					budget.close();
					fail(error);
					completeOne();
					return;
				}
				work._inputs.clear();
				Batch batch = new Batch(partials.entrySet().iterator(), budget);
				enqueue(new NextWork(batch), batch);
			});
		}
		catch(Throwable failure) {
			budget.close();
			fail(failure);
			completeOne();
		}
	}

	private void next(Batch batch) {
		if(!batch._partials.hasNext()) {
			batch._budget.close();
			completeOne();
			return;
		}
		Map.Entry<Integer, MatrixBlock> entry = batch._partials.next();
		long bytes = OOCUtils.memoryCharge(entry.getValue());
		try {
			batch._budget.reserveBlocking(bytes);
			reduce(entry.getKey(), new ManagedPayload<>(
				new IndexedMatrixValue(outputIndex(entry.getKey()), entry.getValue()), bytes, batch._budget), batch);
		}
		catch(Throwable failure) {
			batch._budget.close();
			fail(failure);
			completeOne();
		}
	}

	private void reduce(int slot, ManagedPayload<IndexedMatrixValue> incoming, Batch batch) {
		OOCFuture<StoreLease<IndexedMatrixValue>> match;
		try {
			match = _accumulators.putOrTake(slot, incoming, batch._budget);
		}
		catch(Throwable failure) {
			incoming.release();
			batch._budget.close();
			fail(failure);
			completeOne();
			return;
		}
		match.whenComplete((existing, error) -> {
			if(error != null) {
				incoming.release();
				batch._budget.close();
				fail(error);
				completeOne();
			}
			else if(existing == null)
				enqueue(new NextWork(batch), batch);
			else
				enqueue(new MergeWork(slot, incoming, existing, batch), batch);
		});
	}

	private void merge(MergeWork work) {
		ManagedPayload<IndexedMatrixValue> incoming = work.takeIncoming();
		StoreLease<IndexedMatrixValue> existing = work.takeExisting();
		ManagedPayload<IndexedMatrixValue> merged = null;
		try {
			MatrixBlock left = (MatrixBlock) incoming.value().getValue();
			MatrixBlock right = (MatrixBlock) existing.value().getValue();
			MatrixBlock block = left.binaryOperations(PLUS, right, new MatrixBlock());
			long bytes = OOCUtils.memoryCharge(block);
			work._batch._budget.reserveBlocking(bytes);
			merged = new ManagedPayload<>(new IndexedMatrixValue(outputIndex(work._slot), block), bytes,
				work._batch._budget);
			incoming.release();
			incoming = null;
			OOCFuture<Void> released = existing.closeAsync();
			existing = null;
			ManagedPayload<IndexedMatrixValue> result = merged;
			merged = null;
			released.whenComplete((ignored, error) -> {
				if(error != null) {
					result.release();
					work._batch._budget.close();
					fail(error);
					completeOne();
				}
				else
					reduce(work._slot, result, work._batch);
			});
		}
		catch(Throwable failure) {
			if(merged != null)
				merged.release();
			if(incoming != null)
				incoming.release();
			if(existing != null)
				existing.close();
			work._batch._budget.close();
			fail(failure);
			completeOne();
		}
	}

	private void enqueue(ComputeWork work, Batch batch) {
		try {
			_ready.enqueue(work);
		}
		catch(Throwable failure) {
			work.close();
			batch._budget.close();
			fail(failure);
			completeOne();
		}
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

	private void flush(int slot) {
		if(!_flushing.compareAndSet(false, true))
			return;
		_flushSlot = slot;
		driveFlush();
	}

	private void driveFlush() {
		while(true) {
			int slot = _flushSlot;
			if(slot == _outputBlocks) {
				finish();
				return;
			}
			_flushPending.set(true);
			flushSlot(slot);
			if(_flushPending.compareAndSet(true, false))
				return;
		}
	}

	private void resumeFlush() {
		if(!_flushPending.compareAndSet(true, false))
			driveFlush();
	}

	private void flushSlot(int slot) {
		long bytes = OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(_outputBytes) + _outputBytes;
		_allowance.reserveAsync(bytes).whenComplete((ignored, admissionError) -> {
			if(admissionError != null) {
				fail(admissionError);
				finish();
				return;
			}
			ReservationBudget budget = new ReservationBudget(_allowance, bytes);
			_accumulators.take(slot, budget).whenComplete((lease, error) -> {
				try {
					if(error != null)
						throw new RuntimeException(error);
					if(lease != null) {
						OOCStream.QueueCallback<IndexedMatrixValue> callback = new MaterializedCallback<>(lease);
						try {
							_outputStream.enqueue(callback);
							callback = null;
						}
						finally {
							if(callback != null)
								callback.close();
						}
					}
					else {
						DataCharacteristics output = _output.getDataCharacteristics();
						int blocksize = output.getBlocksize();
						int blockRow = slot / _outputColBlocks;
						int blockCol = slot % _outputColBlocks;
						MatrixBlock empty = new MatrixBlock(
							(int) Math.min(blocksize, output.getRows() - (long) blockRow * blocksize),
							(int) Math.min(blocksize, output.getCols() - (long) blockCol * blocksize), true);
						OOCUtils.enqueueExact(_outputStream, new IndexedMatrixValue(outputIndex(slot), empty), budget);
					}
					budget.close();
					_flushSlot = slot + 1;
					resumeFlush();
				}
				catch(Throwable failure) {
					budget.close();
					fail(failure);
					finish();
				}
			});
		});
	}

	private MatrixIndexes outputIndex(int slot) {
		return new MatrixIndexes(slot / _outputColBlocks + 1L, slot % _outputColBlocks + 1L);
	}

	private void finish() {
		if(!_finished.compareAndSet(false, true))
			return;
		try {
			_outputStream.closeInput();
		}
		catch(Throwable failure) {
			fail(failure);
		}
		finally {
			if(_readers != null)
				_readers.forEach(IndexedMaterializedStoreReader::close);
			if(_stores != null)
				_stores.forEach(MaterializedStore::close);
			if(_accumulators != null)
				_accumulators.close();
			onComplete();
		}
	}

	private interface ComputeWork extends AutoCloseable {
		@Override
		default void close() {
		}
	}

	private static final class BuildWork implements ComputeWork {
		private final List<StoreLease<IndexedMatrixValue>> _inputs;
		private final ReservationBudget _budget;

		private BuildWork(List<StoreLease<IndexedMatrixValue>> inputs, ReservationBudget budget) {
			_inputs = new ArrayList<>(inputs);
			_budget = budget;
		}

		@Override
		public void close() {
			_inputs.forEach(StoreLease::close);
			_inputs.clear();
		}
	}

	private record NextWork(Batch _batch) implements ComputeWork {
	}

	private static final class MergeWork implements ComputeWork {
		private final int _slot;
		private ManagedPayload<IndexedMatrixValue> _incoming;
		private StoreLease<IndexedMatrixValue> _existing;
		private final Batch _batch;

		private MergeWork(int slot, ManagedPayload<IndexedMatrixValue> incoming,
			StoreLease<IndexedMatrixValue> existing, Batch batch) {
			_slot = slot;
			_incoming = incoming;
			_existing = existing;
			_batch = batch;
		}

		private ManagedPayload<IndexedMatrixValue> takeIncoming() {
			ManagedPayload<IndexedMatrixValue> incoming = _incoming;
			_incoming = null;
			return incoming;
		}

		private StoreLease<IndexedMatrixValue> takeExisting() {
			StoreLease<IndexedMatrixValue> existing = _existing;
			_existing = null;
			return existing;
		}

		@Override
		public void close() {
			if(_incoming != null)
				_incoming.release();
			if(_existing != null)
				_existing.close();
		}
	}

	private static final class Batch {
		private final Iterator<Map.Entry<Integer, MatrixBlock>> _partials;
		private final ReservationBudget _budget;

		private Batch(Iterator<Map.Entry<Integer, MatrixBlock>> partials, ReservationBudget budget) {
			_partials = partials;
			_budget = budget;
		}
	}
}
