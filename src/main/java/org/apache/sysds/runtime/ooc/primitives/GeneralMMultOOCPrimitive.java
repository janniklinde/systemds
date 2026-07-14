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
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedInputRequest;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MultiplicityLiveness;
import org.apache.sysds.runtime.ooc.store.OOCMaterializedView;
import org.apache.sysds.runtime.ooc.store.StateLease;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * First general out-of-core matrix multiplication skeleton ({@code C = A %*% B}). B is fully materialized while A
 * is consumed in row-major order. An arriving A tile is transferred into a cache-backed state table and replaced by
 * lightweight multiply-work descriptors, so the input callback does not remain open for the complete one-to-many
 * join.
 *
 * There is deliberately exactly one {@link AllocatedOOCStream}: the stream of multiply-work descriptors. Parking A
 * is an ownership transfer, not a second admission phase, and the task that installs the last partial for a C tile
 * also emits that tile from the same reservation budget. The conservative implementation admits only one A tile's
 * fan-out at a time. That makes this a reliable baseline and leaves scheduling, batching, and eviction policy as
 * explicit extension points for subsequent work.
 */
public class GeneralMMultOOCPrimitive extends PlannableOOCPrimitive {
	private final AggregateBinaryOperator _mmOperator;
	private final BinaryOperator _plus;
	private final StreamContext _sc;
	private final AtomicBoolean _terminated = new AtomicBoolean(false);
	private final AtomicBoolean _completionSignalled = new AtomicBoolean(false);
	private final AtomicBoolean _storeReleased = new AtomicBoolean(false);
	private StateTable<IndexedMatrixValue> _retainedA;
	private StateTable<IndexedMatrixValue> _accumulators;
	private OOCMaterializedView _bView;
	private volatile MaterializedStore.IndexedReader<IndexedMatrixValue> _bReader;
	private volatile ABatch _activeABatch;

	public GeneralMMultOOCPrimitive(OOCStreamable<IndexedMatrixValue> a,
		OOCStreamable<IndexedMatrixValue> b, OOCStreamable<IndexedMatrixValue> out,
		AggregateBinaryOperator mmOperator, BinaryOperator plus, StreamContext sc) {
		super(childrenOf(a, b), List.of(a, b), List.of(out));
		_mmOperator = mmOperator;
		_plus = plus;
		_sc = sc;
	}

	private static List<OOCPrimitive> childrenOf(OOCStreamable<?> a, OOCStreamable<?> b) {
		ArrayList<OOCPrimitive> children = new ArrayList<>(2);
		OOCPrimitive aPrimitive = safePrimitive(a);
		OOCPrimitive bPrimitive = safePrimitive(b);
		if(aPrimitive != null)
			children.add(aPrimitive);
		if(bPrimitive != null)
			children.add(bPrimitive);
		return children;
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
	public OOCMaterializedInputRequest requiresMaterializedInput() {
		return new OOCMaterializedInputRequest(1,
			OOCStoreLayout.of(this::bTileIndex, this::bTileIndexes), 1, 1, this::bEvictionScore);
	}

	/**
	 * Initial eviction seam for experiments. Larger scores are evicted first. With row-major A, larger B row
	 * indexes are normally needed farther in the future.
	 */
	protected long bEvictionScore(MatrixIndexes indexes) {
		return bTileIndex(indexes);
	}

	/** State-table eviction seam; relevant once the conservative one-A-tile throttle is relaxed. */
	protected long retainedAEvictionScore(int slot) {
		return slot;
	}

	/** Output-accumulator eviction seam. Larger row-major slots are farther from baseline completion. */
	protected long accumulatorEvictionScore(int slot) {
		return slot;
	}

	@Override
	public long getMinimumOperatingMemoryBytes() {
		return saturatingAdd(taskBudgetBytes(),
			OOCInstructionUtils.estimateFullTileBytes(getAStreamable().getDataCharacteristics()));
	}

	@Override
	public void inferPatterns() {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		for(OOCPrimitive child : getChildren()) {
			if(!child.hasStartedExecution())
				child.requestPattern(OOCAccessPattern.ROW_MAJOR);
		}
		inferPatterns(getParents());
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == OOCAccessPattern.ROW_MAJOR)
			return;
		_pattern = OOCAccessPattern.ROW_MAJOR;
		for(OOCPrimitive child : getChildren()) {
			if(!child.hasStartedExecution())
				child.requestPattern(OOCAccessPattern.ROW_MAJOR);
		}
	}

	@Override
	public void startExecution() {
		final OOCStreamable<IndexedMatrixValue> aStreamable = getAStreamable();
		final OOCStreamable<IndexedMatrixValue> bStreamable = getBStreamable();
		final OOCStream<IndexedMatrixValue> a = aStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> out = getOutputStreamable().getWriteStream();
		final int aTiles = tileCount(aStreamable.getDataCharacteristics());
		final int cTiles = outputTileCount();
		_retainedA = new StateTable<>(OOCCacheManager.getGlobalCache(),
			CachingStream._streamSeq.getNextID(), aTiles);
		_accumulators = new StateTable<>(OOCCacheManager.getGlobalCache(),
			CachingStream._streamSeq.getNextID(), cTiles);
		_retainedA.addEvictionPolicy(this::retainedAEvictionScore);
		_accumulators.addEvictionPolicy(this::accumulatorEvictionScore);
		_bView = bStreamable.materializedView();

		_bView.completion().whenComplete((ignored, buildError) -> {
			if(buildError != null) {
				fail(buildError, out);
				finishExecution(out);
				return;
			}
			try {
				int bTiles = tileCount(bStreamable.getDataCharacteristics());
				int aRowBlocks = numRowBlocks(aStreamable.getDataCharacteristics());
				_bReader = _bView.openIndexedReader(new MultiplicityLiveness(bTiles, aRowBlocks), _allowance);
			}
			catch(Throwable t) {
				fail(t, out);
				finishExecution(out);
				return;
			}
			_bView.readersSealed().whenComplete((ignore, sealError) -> {
				if(sealError != null) {
					fail(sealError, out);
					finishExecution(out);
				}
				else
					startMultiply(a, out);
			});
		});
	}

	private void startMultiply(OOCStream<IndexedMatrixValue> a, OOCStream<IndexedMatrixValue> out) {
		final int nBlocks = numColBlocks(getBStreamable().getDataCharacteristics());
		final int kBlocks = numColBlocks(getAStreamable().getDataCharacteristics());
		final int cTiles = outputTileCount();
		final AtomicIntegerArray contributions = new AtomicIntegerArray(cTiles);
		final Object[] accumulatorLocks = new Object[cTiles];
		for(int i = 0; i < cTiles; i++)
			accumulatorLocks[i] = new Object();

		final SubscribableTaskQueue<MultiplyWork> work = new SubscribableTaskQueue<>();
		final long taskBudget = taskBudgetBytes();
		// This is the primitive's one and only budget-allocation stream. In particular, neither A retention
		// nor final C emission is wrapped in another AllocatedOOCStream.
		final OOCStream<MultiplyWork> admittedWork = new AllocatedOOCStream<>(work, _allowance,
			ignored -> taskBudget, taskBudget > 0, ReservationBudget::admitted);

		OOCInstructionUtils.submitOOCTasks(admittedWork, cb -> {
			ReservationBudget budget = OOCInstructionUtils.detachBudget(cb);
			MultiplyWork multiplyWork = null;
			try {
				multiplyWork = cb.get();
				if(budget == null)
					throw new DMLRuntimeException("Missing admitted general matrix-multiply task budget.");
				boolean emitted = multiply(multiplyWork, out, contributions, accumulatorLocks, kBlocks, budget);
				if(emitted)
					budget = null; // The output callback now owns the task budget.
			}
			catch(Throwable t) {
				fail(t, out);
				throw DMLRuntimeException.of(t);
			}
			finally {
				if(multiplyWork != null)
					multiplyWork._batch.taskFinished(_retainedA);
				if(budget != null)
					budget.close();
			}
		}, _sc).whenComplete((ignored, taskError) -> {
			if(taskError != null)
				fail(taskError, out);
			finishExecution(out);
		});

		runCoordinator("ooc-general-mm-a-driver", () -> driveA(a, work, nBlocks, out));
	}

	private void driveA(OOCStream<IndexedMatrixValue> a, OOCStream<MultiplyWork> work, int nBlocks,
		OOCStream<IndexedMatrixValue> out) {
		try {
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = a.dequeueCB()) != null) {
				try {
					if(callback.isEos()) {
						callback.close();
						break;
					}
					IndexedMatrixValue tile = callback.get();
					int row = Math.toIntExact(tile.getIndexes().getRowIndex() - 1);
					int inner = Math.toIntExact(tile.getIndexes().getColumnIndex() - 1);
					int aSlot = aTileIndex(tile.getIndexes());
					ABatch batch = new ABatch(aSlot, nBlocks);
					_activeABatch = batch;
					installRetainedA(aSlot, callback);
					for(int col = 0; col < nBlocks; col++) {
						int bIndex = Math.toIntExact((long)inner * nBlocks + col);
						int cSlot = Math.toIntExact((long)row * nBlocks + col);
						work.enqueue(new MultiplyWork(batch, bIndex, cSlot,
							new MatrixIndexes(row + 1L, col + 1L)));
					}
					awaitBatch(batch);
					if(_activeABatch == batch)
						_activeABatch = null;
				}
				catch(Throwable t) {
					callback.close();
					throw DMLRuntimeException.of(t);
				}
			}
		}
		catch(Throwable t) {
			fail(t, out);
		}
		finally {
			try {
				work.closeInput();
			}
			catch(IllegalStateException ignored) {
				// A concurrent failure may already have closed the work source.
			}
		}
	}

	private boolean multiply(MultiplyWork work, OOCStream<IndexedMatrixValue> out,
		AtomicIntegerArray contributions, Object[] accumulatorLocks, int kBlocks, ReservationBudget budget) {
		StateLease<IndexedMatrixValue> aLease = null;
		MaterializedStore.Lease<IndexedMatrixValue> bLease = null;
		ManagedPayload<IndexedMatrixValue> partial = null;
		try {
			try {
				aLease = await(_retainedA.lease(work._batch._aSlot, budget));
				if(aLease == null)
					throw new IllegalStateException("Missing retained A tile " + work._batch._aSlot);
				bLease = await(_bReader.request(work._bIndex, budget));
				if(bLease == null)
					throw new IllegalStateException("Missing materialized B tile " + work._bIndex);
				MatrixBlock aBlock = (MatrixBlock)aLease.value().getValue();
				MatrixBlock bBlock = (MatrixBlock)bLease.value().getValue();
				MatrixBlock block = aBlock.aggregateBinaryOperations(aBlock, bBlock,
					new MatrixBlock(), _mmOperator);
				partial = payload(new IndexedMatrixValue(work._outputIndex, block), budget);
			}
			finally {
				if(bLease != null)
					bLease.close();
				if(aLease != null)
					aLease.close();
			}

			IndexedMatrixValue completed = null;
			synchronized(accumulatorLocks[work._cSlot]) {
				accumulate(work._cSlot, partial, budget);
				partial = null;
				if(contributions.incrementAndGet(work._cSlot) == kBlocks) {
					StateLease<IndexedMatrixValue> result = await(_accumulators.take(work._cSlot, budget));
					if(result == null)
						throw new IllegalStateException("Missing completed C accumulator " + work._cSlot);
					try(result) {
						completed = new IndexedMatrixValue(work._outputIndex,
							(MatrixBlock)result.value().getValue());
					}
				}
			}
			if(completed == null)
				return false;
			OOCInstructionUtils.enqueueExact(out, completed, budget);
			return true;
		}
		catch(Throwable t) {
			if(partial != null)
				partial.release();
			throw DMLRuntimeException.of(t);
		}
	}

	private void accumulate(int slot, ManagedPayload<IndexedMatrixValue> payload, ReservationBudget budget) {
		while(true) {
			StateLease<IndexedMatrixValue> existing;
			try {
				existing = await(_accumulators.installOrTake(slot, payload, budget));
			}
			catch(RuntimeException ex) {
				payload.release();
				throw ex;
			}
			if(existing == null)
				return;
			IndexedMatrixValue merged;
			try(existing) {
				MatrixBlock block = ((MatrixBlock)existing.value().getValue()).binaryOperations(_plus,
					payload.value().getValue(), new MatrixBlock());
				merged = new IndexedMatrixValue(existing.value().getIndexes(), block);
			}
			finally {
				payload.release();
			}
			payload = payload(merged, budget);
		}
	}

	private ManagedPayload<IndexedMatrixValue> payload(IndexedMatrixValue value, MemoryAllowance owner) {
		long bytes = ((MatrixBlock)value.getValue()).getExactSerializedSize();
		owner.reserveBlocking(bytes);
		return new ManagedPayload<>(value, bytes, owner);
	}

	private void installRetainedA(int slot, OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback instanceof MaterializedCallback pinned) {
			try {
				_retainedA.installReference(slot, pinned.pinnedEntry());
			}
			finally {
				pinned.close();
			}
			return;
		}
		ManagedPayload<IndexedMatrixValue> payload = null;
		try {
			if(callback instanceof InMemoryQueueCallback managed && managed.getManagedBytes() > 0)
				payload = managed.extractManagedPayload();
			else {
				IndexedMatrixValue value = callback.get();
				long bytes = ((MatrixBlock)value.getValue()).getExactSerializedSize();
				_allowance.reserveBlocking(bytes);
				payload = new ManagedPayload<>(value, bytes, _allowance);
			}
			_retainedA.install(slot, payload);
			payload = null;
		}
		finally {
			callback.close();
			if(payload != null)
				payload.release();
		}
	}

	private long taskBudgetBytes() {
		DataCharacteristics a = getAStreamable().getDataCharacteristics();
		DataCharacteristics b = getBStreamable().getDataCharacteristics();
		DataCharacteristics c = getOutputStreamable().getDataCharacteristics();
		long aLogical = OOCInstructionUtils.estimateFullTileBytes(a);
		long bLogical = OOCInstructionUtils.estimateFullTileBytes(b);
		long cLogical = OOCInstructionUtils.estimateFullTileBytes(c);
		OOCCache cache = OOCCacheManager.getGlobalCache();
		long aPin = cache.maxPhysicalPinBytes(aLogical);
		long bPin = cache.maxPhysicalPinBytes(bLogical);
		long cPin = cache.maxPhysicalPinBytes(cLogical);
		// Input pins may be released asynchronously; accumulation can simultaneously hold a packed C pin,
		// an incoming partial, a replacement, and the final output callback.
		return saturatingAdd(aPin, bPin, saturatingMultiply(cPin, 2), saturatingMultiply(cLogical, 4));
	}

	private int aTileIndex(MatrixIndexes indexes) {
		int kBlocks = numColBlocks(getAStreamable().getDataCharacteristics());
		return Math.toIntExact((indexes.getRowIndex() - 1) * kBlocks + indexes.getColumnIndex() - 1);
	}

	private int bTileIndex(MatrixIndexes indexes) {
		int nBlocks = numColBlocks(getBStreamable().getDataCharacteristics());
		return Math.toIntExact((indexes.getRowIndex() - 1) * nBlocks + indexes.getColumnIndex() - 1);
	}

	private MatrixIndexes bTileIndexes(int index) {
		int nBlocks = numColBlocks(getBStreamable().getDataCharacteristics());
		return new MatrixIndexes(index / nBlocks + 1L, index % nBlocks + 1L);
	}

	private int outputTileCount() {
		return Math.toIntExact((long)numRowBlocks(getAStreamable().getDataCharacteristics()) *
			numColBlocks(getBStreamable().getDataCharacteristics()));
	}

	private static int tileCount(DataCharacteristics dc) {
		return Math.toIntExact(OOCUtils.getNumBlocks(dc));
	}

	private static int numRowBlocks(DataCharacteristics dc) {
		return Math.toIntExact(OOCUtils.getNumRowBlocks(dc));
	}

	private static int numColBlocks(DataCharacteristics dc) {
		return Math.toIntExact(OOCUtils.getNumColBlocks(dc));
	}

	private void fail(Throwable error, OOCStream<IndexedMatrixValue> out) {
		if(!_terminated.compareAndSet(false, true))
			return;
		DMLRuntimeException failure = DMLRuntimeException.of(error);
		ABatch active = _activeABatch;
		if(active != null)
			active.fail(failure);
		try {
			out.propagateFailure(failure);
		}
		finally {
			if(_sc != null)
				_sc.failAll(failure);
		}
	}

	private void finishExecution(OOCStream<IndexedMatrixValue> out) {
		if(!_terminated.get()) {
			try {
				out.closeInput();
			}
			catch(Throwable t) {
				fail(t, out);
			}
			_terminated.compareAndSet(false, true);
		}
		if(_completionSignalled.compareAndSet(false, true))
			onComplete();
	}

	@Override
	public void onComplete() {
		try {
			if(_bView != null && _storeReleased.compareAndSet(false, true)) {
				if(_bReader != null)
					_bReader.close();
				_bView.close();
			}
			if(_retainedA != null)
				_retainedA.close();
			if(_accumulators != null)
				_accumulators.close();
		}
		finally {
			super.onComplete();
		}
	}

	private static void awaitBatch(ABatch batch) {
		try {
			batch._done.join();
		}
		catch(CompletionException ex) {
			throw DMLRuntimeException.of(ex);
		}
	}

	private static <T> T await(OOCFuture<T> future) {
		try {
			return future.get();
		}
		catch(InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new DMLRuntimeException(ex);
		}
		catch(ExecutionException ex) {
			throw DMLRuntimeException.of(ex);
		}
	}

	private static long saturatingMultiply(long value, long factor) {
		return value > Long.MAX_VALUE / factor ? Long.MAX_VALUE : value * factor;
	}

	private static long saturatingAdd(long... values) {
		long result = 0;
		for(long value : values) {
			if(Long.MAX_VALUE - result < value)
				return Long.MAX_VALUE;
			result += value;
		}
		return result;
	}

	public OOCStreamable<IndexedMatrixValue> getAStreamable() {
		return getInputStream(0);
	}

	public OOCStreamable<IndexedMatrixValue> getBStreamable() {
		return getInputStream(1);
	}

	public OOCStreamable<IndexedMatrixValue> getOutputStreamable() {
		return getOutputStream(0);
	}

	private static final class ABatch {
		private final int _aSlot;
		private final AtomicInteger _remaining;
		private final CompletableFuture<Void> _done = new CompletableFuture<>();

		private ABatch(int aSlot, int tasks) {
			_aSlot = aSlot;
			_remaining = new AtomicInteger(tasks);
		}

		private void taskFinished(StateTable<IndexedMatrixValue> retainedA) {
			if(_remaining.decrementAndGet() == 0) {
				retainedA.clear(_aSlot);
				_done.complete(null);
			}
		}

		private void fail(Throwable error) {
			_done.completeExceptionally(error);
		}
	}

	private static final class MultiplyWork {
		private final ABatch _batch;
		private final int _bIndex;
		private final int _cSlot;
		private final MatrixIndexes _outputIndex;

		private MultiplyWork(ABatch batch, int bIndex, int cSlot, MatrixIndexes outputIndex) {
			_batch = batch;
			_bIndex = bIndex;
			_cSlot = cSlot;
			_outputIndex = outputIndex;
		}
	}
}
