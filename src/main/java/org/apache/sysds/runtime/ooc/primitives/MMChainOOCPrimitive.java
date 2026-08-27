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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.sysds.lops.MapMultChain.ChainType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.matrix.operators.ScalarOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
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

public final class MMChainOOCPrimitive extends OOCPrimitive {
	private static final int VECTOR = 0;
	private static final int WEIGHT = 1;
	private static final ScalarOperator NEGATE = new RightScalarOperator(Multiply.getMultiplyFnObject(), -1);

	private final OOCStreamable<IndexedMatrixValue> _x;
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final ChainType _type;
	private final AggregateBinaryOperator _multiply;
	private final BinaryOperator _plus;
	private final BinaryOperator _weight;
	private final int[] _sideInputs;
	private final MaterializedStore<IndexedMatrixValue>[] _sideStores;
	private final IndexedMaterializedStoreReader<IndexedMatrixValue>[] _sideReaders;
	private final AtomicBoolean _inputComplete = new AtomicBoolean();
	private final AtomicBoolean _liveInputEnded = new AtomicBoolean();
	private final AtomicBoolean _liveSchedulingReady = new AtomicBoolean();
	private final AtomicBoolean _cleaned = new AtomicBoolean();
	private final AtomicInteger _active = new AtomicInteger(1);
	private final AtomicInteger _pendingSides = new AtomicInteger();
	private boolean _liveInput;
	private MaterializedStore<IndexedMatrixValue> _xStore;
	private volatile IndexedMaterializedStoreReader<IndexedMatrixValue> _xReader;
	private AtomicIntegerArray _tilesSeen;
	private AtomicIntegerArray _bandsScheduled;
	private OOCStream<BandWork> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private int _rowBlocks;
	private int _colBlocks;
	private long _taskBytes;

	@SuppressWarnings("unchecked")
	public MMChainOOCPrimitive(OOCStreamable<IndexedMatrixValue> x, OOCStreamable<IndexedMatrixValue> v,
		OOCStreamable<IndexedMatrixValue> w, OOCStreamable<IndexedMatrixValue> output, ChainType type,
		AggregateBinaryOperator multiply, BinaryOperator plus, BinaryOperator weight, StreamContext context) {
		super(context, inputs(x, v, w));
		if(v == null && w == null)
			throw new DMLRuntimeException("MMChain OOC requires a vector or a weight input.");
		if(w != null && weight == null && v != null)
			throw new DMLRuntimeException("A weighted MMChain requires the operator combining it with X %*% v.");
		_x = x;
		_output = output;
		_type = type;
		_multiply = multiply;
		_plus = plus;
		_weight = weight;
		_sideInputs = new int[] {v != null ? 1 : -1, w != null ? (v != null ? 2 : 1) : -1};
		_sideStores = new MaterializedStore[2];
		_sideReaders = new IndexedMaterializedStoreReader[2];
	}

	private static OOCStreamable<?>[] inputs(OOCStreamable<IndexedMatrixValue> x, OOCStreamable<IndexedMatrixValue> v,
		OOCStreamable<IndexedMatrixValue> w) {
		List<OOCStreamable<?>> inputs = new ArrayList<>(3);
		inputs.add(x);
		if(v != null)
			inputs.add(v);
		if(w != null)
			inputs.add(w);
		return inputs.toArray(OOCStreamable<?>[]::new);
	}

	private boolean has(int side) {
		return _sideInputs[side] >= 0;
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		List<OOCMaterializedInputRequest> requests = new ArrayList<>(3);
		requests.add(
			new OOCMaterializedInputRequest(0, OOCStoreLayout.ROW_MAJOR, 1, this::accept, live -> _liveInput = live));
		for(int side = VECTOR; side <= WEIGHT; side++)
			if(has(side))
				requests.add(new OOCMaterializedInputRequest(_sideInputs[side], OOCStoreLayout.ROW_MAJOR, 1));
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
		for(OOCPrimitive child : getChildren())
			child.requestPattern(OOCAccessPattern.ROW_MAJOR);
	}

	@Override
	protected long getMaxTaskReservationBytes() {
		DataCharacteristics xDc = _x.getDataCharacteristics();
		return xDc == null || !xDc.dimsKnown() || xDc.getBlocksize() <= 0 ? 0 : taskBytes(xDc);
	}

	private long taskBytes(DataCharacteristics xDc) {
		long colBlocks = xDc.getNumColBlocks();
		long vectorBytes = OOCUtils.estimateOutputTileBytes(_output.getDataCharacteristics());
		long pinned = OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(OOCUtils.estimateFullTileBytes(xDc)) *
			colBlocks;
		if(has(VECTOR))
			pinned += OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(vectorBytes) * colBlocks;
		if(has(WEIGHT))
			pinned += OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(vectorBytes);
		// an accumulator for u and one output partial per column tile, each with room for a working copy
		return pinned + vectorBytes * (2 * colBlocks + 2);
	}

	@Override
	protected void startExecution() {
		DataCharacteristics xDc = _x.getDataCharacteristics();
		if(xDc == null || !xDc.dimsKnown() || xDc.getBlocksize() <= 0)
			throw new DMLRuntimeException("MMChain OOC requires known input dimensions and block size.");
		_rowBlocks = Math.toIntExact(xDc.getNumRowBlocks());
		_colBlocks = Math.toIntExact(xDc.getNumColBlocks());
		if(_rowBlocks <= 0 || _colBlocks <= 0)
			throw new DMLRuntimeException("MMChain OOC requires non-empty input block geometry.");
		_tilesSeen = new AtomicIntegerArray(_rowBlocks);
		_bandsScheduled = new AtomicIntegerArray(_rowBlocks);
		_taskBytes = taskBytes(xDc);
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

		_pendingSides.set(1 + (has(VECTOR) ? 1 : 0) + (has(WEIGHT) ? 1 : 0));
		for(int side = VECTOR; side <= WEIGHT; side++) {
			if(!has(side))
				continue;
			final int current = side;
			MaterializedStore.Liveness liveness = side == VECTOR ? new CountingLiveness(_colBlocks,
				_rowBlocks) : new CountingLiveness(_rowBlocks, 1);
			getMaterializedInput(_sideInputs[side]).whenComplete((store, error) -> {
				if(error != null) {
					fail(error);
					finishInput();
					return;
				}
				_sideStores[current] = store;
				store.completion().whenComplete((ignored, completionError) -> {
					if(completionError != null) {
						fail(completionError);
						finishInput();
						return;
					}
					try {
						_sideReaders[current] = store.openIndexedReader(liveness);
						sideReady();
					}
					catch(Throwable failure) {
						fail(failure);
						finishInput();
					}
				});
			});
		}

		getMaterializedInput(0).whenComplete((store, error) -> {
			if(error != null) {
				fail(error);
				finishInput();
				return;
			}
			_xStore = store;
			CountingLiveness liveness = new CountingLiveness(_rowBlocks * _colBlocks, 1);
			if(_liveInput) {
				_xReader = store.openLiveIndexedReader(liveness);
				sideReady();
				_liveSchedulingReady.set(true);
				finishLiveInput();
			}
			else
				store.completion().whenComplete((ignored, completionError) -> {
					if(completionError != null) {
						fail(completionError);
						finishInput();
						return;
					}
					_xReader = store.openIndexedReader(liveness);
					sideReady();
				});
		});
	}

	private void sideReady() {
		if(_pendingSides.decrementAndGet() != 0)
			return;
		if(_liveInput)
			for(int band = 0; band < _rowBlocks; band++)
				tryScheduleBand(band);
		else
			OOCInstructionUtils.submitOOCTask(this::drain, new StreamContext().addOutStream(_outputStream));
	}

	private void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback.isEos() || callback.isFailure()) {
			try(callback) {
				if(callback.isFailure())
					callback.get();
			}
			catch(Throwable failure) {
				fail(failure);
			}
			_liveInputEnded.set(true);
			finishLiveInput();
			return;
		}

		try(callback) {
			int band = Math.toIntExact(callback.get().getIndexes().getRowIndex() - 1);
			_tilesSeen.incrementAndGet(band);
			tryScheduleBand(band);
		}
		catch(Throwable failure) {
			fail(failure);
			finishInput();
		}
	}

	private void finishLiveInput() {
		if(_liveInputEnded.get() && _liveSchedulingReady.get())
			finishInput();
	}

	private void drain() {
		try {
			for(int band = 0; band < _rowBlocks && !hasFailed(); band++)
				if(_bandsScheduled.compareAndSet(band, 0, 1))
					scheduleBand(band);
		}
		catch(Throwable failure) {
			fail(failure);
		}
		finally {
			finishInput();
		}
	}

	private void tryScheduleBand(int band) {
		if(_pendingSides.get() == 0 && _tilesSeen.get(band) == _colBlocks && _bandsScheduled.compareAndSet(band, 0, 1))
			scheduleBand(band);
	}

	private void scheduleBand(int band) {
		_active.incrementAndGet();
		_allowance.reserveAsync(_taskBytes).whenComplete((ignored, admissionError) -> {
			if(admissionError != null) {
				fail(admissionError);
				completeOne();
				return;
			}
			ReservationBudget budget = new ReservationBudget(_allowance, _taskBytes).enableReuse();
			List<OOCFuture<StoreLease<IndexedMatrixValue>>> requests = new ArrayList<>(2 * _colBlocks + 1);
			try {
				for(int tile = 0; tile < _colBlocks; tile++)
					requests.add(_xReader.request(band + 1L, tile + 1L, budget));
				if(has(VECTOR))
					for(int tile = 0; tile < _colBlocks; tile++)
						requests.add(_sideReaders[VECTOR].request(tile + 1L, 1L, budget));
				if(has(WEIGHT))
					requests.add(_sideReaders[WEIGHT].request(band + 1L, 1L, budget));
			}
			catch(Throwable failure) {
				for(OOCFuture<StoreLease<IndexedMatrixValue>> issued : requests)
					issued.whenComplete((lease, error) -> closeLease(lease));
				budget.close();
				fail(failure);
				completeOne();
				return;
			}
			OOCFuture.allOf(requests, MMChainOOCPrimitive::closeLease).whenComplete((leases, error) -> {
				if(error != null) {
					budget.close();
					fail(error);
					completeOne();
					return;
				}
				try {
					for(StoreLease<IndexedMatrixValue> lease : leases)
						if(lease == null)
							throw new DMLRuntimeException("Missing MMChain tile for block row " + (band + 1));
					_ready.enqueue(new BandWork(band, leases, budget));
				}
				catch(Throwable failure) {
					leases.forEach(MMChainOOCPrimitive::closeLease);
					budget.close();
					fail(failure);
					completeOne();
				}
			});
		});
	}

	private void process(BandWork work) {
		ReservationBudget budget = work.takeBudget();
		List<OOCStream.QueueCallback<IndexedMatrixValue>> outputs = new ArrayList<>(_colBlocks);
		try {
			List<StoreLease<IndexedMatrixValue>> leases = work._leases;
			MatrixBlock u = null;
			for(int tile = 0; has(VECTOR) && tile < _colBlocks; tile++) {
				MatrixBlock x = block(leases, tile);
				MatrixBlock partial = x.aggregateBinaryOperations(x, block(leases, _colBlocks + tile),
					new MatrixBlock(), _multiply);
				u = u == null ? partial : u.binaryOperations(_plus, partial, new MatrixBlock());
			}
			if(has(WEIGHT)) {
				MatrixBlock w = block(leases, leases.size() - 1);
				u = u == null ? w.scalarOperations(NEGATE, new MatrixBlock()) : u.binaryOperations(_weight, w,
					new MatrixBlock());
			}
			if(u == null)
				throw new DMLRuntimeException("MMChain OOC produced no intermediate for chain type " + _type);

			for(int tile = 0; tile < _colBlocks; tile++) {
				MatrixBlock partial = multTransposeVector(block(leases, tile), u);
				long bytes = OOCUtils.memoryCharge(partial);
				budget.reserveBlocking(bytes);
				outputs.add(new InMemoryQueueCallback<>(
					new IndexedMatrixValue(new MatrixIndexes(tile + 1L, work._band + 1L), partial), null, budget,
					bytes));
			}
			work.releaseLeases();
			budget.close();
			for(OOCStream.QueueCallback<IndexedMatrixValue> output : outputs)
				_outputStream.enqueue(output);
			outputs.clear();
		}
		catch(Throwable failure) {
			budget.close();
			fail(failure);
		}
		finally {
			for(OOCStream.QueueCallback<IndexedMatrixValue> output : outputs)
				output.close();
			work.close();
			completeOne();
		}
	}

	private static MatrixBlock block(List<StoreLease<IndexedMatrixValue>> leases, int position) {
		return (MatrixBlock) leases.get(position).value().getValue();
	}

	private static MatrixBlock multTransposeVector(MatrixBlock x, MatrixBlock u) {
		int rows = x.getNumRows();
		int cols = x.getNumColumns();
		MatrixBlock out = new MatrixBlock(cols, 1, false);
		out.allocateDenseBlock();
		double[] outVals = out.getDenseBlockValues();

		if(x.isInSparseFormat()) {
			SparseBlock a = x.getSparseBlock();
			if(a != null)
				for(int i = 0; i < rows; i++) {
					if(a.isEmpty(i))
						continue;
					double uval = u.get(i, 0);
					if(uval == 0)
						continue;
					int apos = a.pos(i);
					int alen = a.size(i);
					int[] aix = a.indexes(i);
					double[] avals = a.values(i);
					for(int k = apos; k < apos + alen; k++)
						outVals[aix[k]] += uval * avals[k];
				}
		}
		else {
			DenseBlock a = x.getDenseBlock();
			for(int i = 0; i < rows; i++) {
				double uval = u.get(i, 0);
				if(uval == 0)
					continue;
				double[] avals = a.values(i);
				int apos = a.pos(i);
				for(int j = 0; j < cols; j++)
					outVals[j] += uval * avals[apos + j];
			}
		}

		out.recomputeNonZeros();
		out.examSparsity();
		return out;
	}

	private void finishInput() {
		if(_inputComplete.compareAndSet(false, true))
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

	private static void closeLease(StoreLease<IndexedMatrixValue> lease) {
		if(lease != null)
			lease.close();
	}

	private void cleanup() {
		if(!_cleaned.compareAndSet(false, true))
			return;
		try {
			if(_xReader != null)
				_xReader.close();
			for(IndexedMaterializedStoreReader<IndexedMatrixValue> reader : _sideReaders)
				if(reader != null)
					reader.close();
		}
		finally {
			try {
				if(_xStore != null)
					_xStore.close();
				for(MaterializedStore<IndexedMatrixValue> store : _sideStores)
					if(store != null)
						store.close();
			}
			finally {
				onComplete();
			}
		}
	}

	private static final class BandWork implements AutoCloseable {
		private final int _band;
		private List<StoreLease<IndexedMatrixValue>> _leases;
		private ReservationBudget _budget;

		private BandWork(int band, List<StoreLease<IndexedMatrixValue>> leases, ReservationBudget budget) {
			_band = band;
			_leases = leases;
			_budget = budget;
		}

		private ReservationBudget takeBudget() {
			ReservationBudget budget = _budget;
			_budget = null;
			return budget;
		}

		private void releaseLeases() {
			if(_leases == null)
				return;
			_leases.forEach(MMChainOOCPrimitive::closeLease);
			_leases = null;
		}

		@Override
		public void close() {
			releaseLeases();
			if(_budget != null) {
				_budget.close();
				_budget = null;
			}
		}
	}
}
