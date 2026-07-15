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

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.lops.MapMultChain.ChainType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.io.CloseableQueue;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedInputRequest;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedView;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.LeaseQueueCallbacks;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MultiplicityLiveness;
import org.apache.sysds.runtime.ooc.store.StateLease;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Consumer;

public class MapMMChainOOCPrimitive extends PlannableOOCPrimitive {
	private final ChainType _type;
	private final StreamContext _sc;
	private final AtomicBoolean _terminated = new AtomicBoolean(false);
	private final AtomicBoolean _storeReleased = new AtomicBoolean(false);
	private StateTable<IndexedMatrixValue> _accumulators;
	private StateTable<IndexedMatrixValue> _retainedTiles;
	private OOCMaterializedView _vView;
	private volatile IndexedMaterializedStoreReader<IndexedMatrixValue> _vReader;

	private MapMMChainOOCPrimitive(List<OOCPrimitive> children, OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> wStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, ChainType type, StreamContext sc) {
		super(children, inputsOf(xStreamable, vStreamable, wStreamable), List.of(outputStreamable));
		_type = type;
		_sc = sc;
	}

	public MapMMChainOOCPrimitive(OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> outputStreamable,
		ChainType type, StreamContext sc) {
		this(childrenOf(xStreamable, vStreamable, null), xStreamable, vStreamable, null, outputStreamable, type, sc);
	}

	public MapMMChainOOCPrimitive(OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> wStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, ChainType type, StreamContext sc) {
		this(childrenOf(xStreamable, vStreamable, wStreamable), xStreamable, vStreamable, wStreamable,
			outputStreamable, type, sc);
	}

	private static List<OOCPrimitive> childrenOf(OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> wStreamable) {
		ArrayList<OOCPrimitive> children = new ArrayList<>(3);
		addPrimitive(children, xStreamable);
		addPrimitive(children, vStreamable);
		addPrimitive(children, wStreamable);
		return children;
	}

	private static List<OOCStreamable<IndexedMatrixValue>> inputsOf(OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> wStreamable) {
		ArrayList<OOCStreamable<IndexedMatrixValue>> inputs = new ArrayList<>(3);
		inputs.add(xStreamable);
		inputs.add(vStreamable);
		if(wStreamable != null)
			inputs.add(wStreamable);
		return inputs;
	}

	private static void addPrimitive(List<OOCPrimitive> children, OOCStreamable<?> streamable) {
		if(streamable == null)
			return;
		try {
			OOCPrimitive primitive = streamable.getPrimitive();
			if(primitive != null)
				children.add(primitive);
		}
		catch(RuntimeException ignored) {
		}
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
			OOCStoreLayout.of(ix -> Math.toIntExact(ix.getRowIndex() - 1),
				index -> new MatrixIndexes(index + 1L, 1)), 1, 1,
			ix -> ix.getRowIndex() - 1);
	}

	@Override
	public void onComplete() {
		try {
				releaseStore();
				if(_accumulators != null)
					_accumulators.close();
				if(_retainedTiles != null)
					_retainedTiles.close();
		}
		finally {
			super.onComplete();
		}
	}

	private void releaseStore() {
		if(_vView == null || !_storeReleased.compareAndSet(false, true))
			return;
		if(_vReader != null)
			_vReader.close();
		_vView.close();
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
		if(_pattern == accessPattern)
			return;
		_pattern = accessPattern;
		for(OOCPrimitive child : getChildren()) {
			if(!child.hasStartedExecution())
				child.requestPattern(OOCAccessPattern.ROW_MAJOR);
		}
	}

	@Override
	public void startExecution() {
		if(_type != ChainType.XtXv || getWStreamable() != null)
			throw new UnsupportedOperationException("MapMMChainOOCPrimitive currently only supports XtXv.");
		_accumulators = new StateTable<>(OOCCacheManager.getGlobalCache(),
			CachingStream._streamSeq.getNextID());
		_retainedTiles = new StateTable<>(OOCCacheManager.getGlobalCache(),
			CachingStream._streamSeq.getNextID());

		_vView = getVStreamable().materializedView();
		final OOCStream<IndexedMatrixValue> x = getXStreamable().getReadStream();
		final OOCStream<IndexedMatrixValue> out = getOutputStreamable().getWriteStream();
		final int numVBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(getVStreamable().getDataCharacteristics()));
		final int numColBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(x.getDataCharacteristics()));
		final int numRowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(x.getDataCharacteristics()));
		final int uBase = numVBlocks;
		final int qBase = uBase + numRowBlocks;
		final int xBase = qBase + numColBlocks;
		final AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		final AggregateBinaryOperator mmOp = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		final BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		final AtomicIntegerArray seenPerRow = new AtomicIntegerArray(numRowBlocks);
		final long xBytes = OOCInstructionUtils.estimateOutputTileBytes(x.getDataCharacteristics());
		final long vBytes = OOCInstructionUtils.estimateOutputTileBytes(getVStreamable().getDataCharacteristics());
		final long qBytes = OOCInstructionUtils.estimateOutputTileBytes(getOutputStreamable().getDataCharacteristics());
		final long vectorBytes = Math.max(vBytes, qBytes);
		final long phase1BudgetBytes = saturatingAdd(xBytes, vBytes, saturatingMultiply(vectorBytes, 3));
		final long phase2BudgetBytes = saturatingAdd(xBytes, vectorBytes, saturatingMultiply(qBytes, 3));
		final long outputBudgetBytes = saturatingAdd(qBytes, qBytes);

		_vView.completion().whenComplete((ignored, error) -> {
			if(error != null) {
				fail(error, out, null);
				return;
			}
			try {
				_vReader = _vView.openIndexedReader(
					new MultiplicityLiveness(numVBlocks, numRowBlocks));
				startXtXv(x, out, numColBlocks, uBase, qBase, xBase, mmOp, plus, seenPerRow,
					phase1BudgetBytes, phase2BudgetBytes, outputBudgetBytes);
			}
			catch(Throwable t) {
				fail(t, out, null);
			}
		});
	}

	private void startXtXv(OOCStream<IndexedMatrixValue> x, OOCStream<IndexedMatrixValue> out,
		int numColBlocks, int uBase, int qBase, int xBase, AggregateBinaryOperator mmOp, BinaryOperator plus,
		AtomicIntegerArray seenPerRow, long phase1BudgetBytes, long phase2BudgetBytes, long outputBudgetBytes) {
		final OOCStream<MMChainWorkload> phase1Stream = new SubscribableTaskQueue<>();
		final OOCStream<Phase2Request> phase2Requests = new SubscribableTaskQueue<>();
		final OOCStream<Phase2Request> admittedPhase2 = new AllocatedOOCStream<>(phase2Requests, _allowance,
			request -> phase2BudgetBytes, phase2BudgetBytes > 0, ReservationBudget::admitted);
		final OOCStream<IndexedMatrixValue> admittedX = new AllocatedOOCStream<>(x, _allowance,
			input -> phase1BudgetBytes, phase1BudgetBytes > 0, ReservationBudget::admitted);
		final CloseableQueue<Phase1Result> phase1Results = new CloseableQueue<>();
		final CloseableQueue<Phase2Result> phase2Results = new CloseableQueue<>();
		final AtomicBoolean phase1Closed = new AtomicBoolean(false);
		final AtomicBoolean phase2Closed = new AtomicBoolean(false);

		CompletableFuture<Void> phase1Future = OOCInstructionUtils.submitOOCTasks(phase1Stream, wl -> {
			MMChainWorkload work = wl.get();
			var vCb = work.cb1;
			var xCb = work.cb2;
			ReservationBudget budget = work.detachBudget();
			OOCStream.QueueCallback<IndexedMatrixValue> retainedX = null;
			boolean resultQueued = false;
			try(vCb; xCb) {
				if(budget == null)
					throw new DMLRuntimeException("Missing admitted MapMMChain phase-1 budget.");
				MatrixIndexes xIx = xCb.get().getIndexes();
				int row = Math.toIntExact(xIx.getRowIndex() - 1);
				int col = Math.toIntExact(xIx.getColumnIndex() - 1);
				MatrixBlock xb = (MatrixBlock)xCb.get().getValue();
				MatrixBlock vb = (MatrixBlock)vCb.get().getValue();
				MatrixBlock ub = xb.aggregateBinaryOperations(xb, vb, new MatrixBlock(), mmOp);
				retainedX = xCb.keepOpen();
				Phase1Result result = new Phase1Result(row, col, ub, retainedX, budget);
				if(!phase1Results.enqueueIfOpen(result)) {
					result.close();
					throw new DMLRuntimeException("MapMMChain phase-1 result queue closed before task completion.");
				}
				retainedX = null;
				budget = null;
				resultQueued = true;
			}
			catch(InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new DMLRuntimeException(e);
			}
			finally {
				if(!resultQueued && retainedX != null)
					retainedX.close();
				if(budget != null)
					budget.close();
			}
		}, cb -> true, (i, cb) -> cb.get().close(), _sc);
		phase1Future.whenComplete((ignored, error) -> {
			try {
				phase1Results.close();
			}
			catch(InterruptedException e) {
				Thread.currentThread().interrupt();
				fail(e, out, phase2Requests);
				return;
			}
			if(error != null)
				fail(error, out, phase2Requests);
		});

		CompletableFuture<Void> phase2Future = OOCInstructionUtils.submitOOCTasks(admittedPhase2, cb -> {
			ReservationBudget budget = OOCInstructionUtils.detachBudget(cb);
			StateLease<IndexedMatrixValue> uLease = null;
			StateLease<IndexedMatrixValue> xLease = null;
			try {
				if(budget == null)
					throw new DMLRuntimeException("Missing admitted MapMMChain phase-2 budget.");
				Phase2Request request = cb.get();
				uLease = await(_accumulators.lease(uBase + request.row, budget));
				if(uLease == null)
					throw new IllegalStateException("Missing finalized XtXv row accumulator " + request.row);
				xLease = await(_retainedTiles.take(xBase + request.row * numColBlocks + request.col, budget));
				if(xLease == null)
					throw new IllegalStateException("Missing retained XtXv input tile for row=" + request.row
						+ ", col=" + request.col);
				MatrixBlock xb = (MatrixBlock)xLease.value().getValue();
				MatrixBlock ub = (MatrixBlock)uLease.value().getValue();
				MatrixBlock qb = multTransposeVector(xb, ub);
				uLease.close();
				uLease = null;
				xLease.close();
				xLease = null;
				Phase2Result result = new Phase2Result(request.col, qb, budget);
				if(!phase2Results.enqueueIfOpen(result)) {
					result.close();
					throw new DMLRuntimeException("MapMMChain phase-2 result queue closed before task completion.");
				}
				budget = null;
			}
			catch(InterruptedException e) {
				throw DMLRuntimeException.of(e);
			}
			finally {
				if(uLease != null)
					uLease.close();
				if(xLease != null)
					xLease.close();
				if(budget != null)
					budget.close();
			}
		}, _sc);
		phase2Future.whenComplete((ignored, error) -> {
			try {
				phase2Results.close();
			}
			catch(InterruptedException e) {
				Thread.currentThread().interrupt();
				fail(e, out, null);
				return;
			}
			if(error != null)
				fail(error, out, null);
		});

		runCoordinator("ooc-mapmmchain-q-coordinator", () -> {
			try {
				Phase2Result result;
				while((result = phase2Results.take()) != null) {
					Phase2Result current = result;
					ReservationBudget budget = current.detachBudget();
					try(current) {
						accumulateTable(qBase + current.col, new MatrixIndexes(current.col + 1L, 1L),
							current.block, plus, budget);
					}
					finally {
						if(budget != null)
							budget.close();
					}
				}
				emitOutputs(out, numColBlocks, qBase, outputBudgetBytes);
				complete(out);
			}
			catch(Throwable t) {
				closeAndDrain(phase2Results, t);
				fail(t, out, null);
			}
		});

		runCoordinator("ooc-mapmmchain-u-coordinator", () -> {
			try {
				Phase1Result result;
				while((result = phase1Results.take()) != null) {
					Phase1Result current = result;
					ReservationBudget budget = current.detachBudget();
					boolean rowReady;
					try(current) {
						accumulateTable(uBase + current.row, new MatrixIndexes(current.row + 1L, 1L),
							current.block, plus, budget);
						installRetainedCallback(xBase + current.row * numColBlocks + current.col,
							current.xCb, budget);
						rowReady = seenPerRow.incrementAndGet(current.row) == numColBlocks;
					}
					finally {
						if(budget != null)
							budget.close();
					}
					if(rowReady)
						schedulePhase2Row(current.row, numColBlocks, phase2Requests);
				}
				closeOnce(phase2Requests, phase2Closed, out, phase2Requests);
			}
			catch(Throwable t) {
				closeAndDrain(phase1Results, t);
				fail(t, out, phase2Requests);
			}
		});

		final AtomicInteger inflightCtr = new AtomicInteger(1);
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> xSubscriber = xcb -> {
			ReservationBudget budget = OOCInstructionUtils.detachBudget(xcb);
			OOCStream.QueueCallback<IndexedMatrixValue> retainedX = null;
			boolean inflightRetained = false;
			try(xcb) {
				if(xcb.isEos()) {
					if(inflightCtr.decrementAndGet() == 0)
						closeOnce(phase1Stream, phase1Closed, out, phase1Stream);
					return;
				}
				if(budget == null)
					throw new DMLRuntimeException("Missing admitted MapMMChain input budget.");
				retainedX = xcb.keepOpen();
				inflightCtr.incrementAndGet();
				inflightRetained = true;
				int col = Math.toIntExact(xcb.get().getIndexes().getColumnIndex() - 1);
				OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> vFuture = vectorTile(col, budget);
				final var fXcb = retainedX;
				final var fBudget = budget;
				retainedX = null;
				budget = null;
				vFuture.whenComplete((vcb, error) -> {
					boolean enqueued = false;
					try {
						if(error != null)
							throw DMLRuntimeException.of(error);
						if(vcb == null)
							throw new IllegalStateException("Missing broadcast vector tile for column block " + col);
						phase1Stream.enqueue(new MMChainWorkload(vcb, fXcb, fBudget));
						enqueued = true;
					}
					catch(Throwable t) {
						fail(t, out, phase1Stream);
					}
					finally {
						if(!enqueued) {
							if(vcb != null)
								vcb.close();
							fXcb.close();
							fBudget.close();
						}
						if(inflightCtr.decrementAndGet() == 0)
							closeOnce(phase1Stream, phase1Closed, out, phase1Stream);
					}
				});
			}
			catch(Throwable t) {
				if(retainedX != null)
					retainedX.close();
				if(inflightRetained && inflightCtr.decrementAndGet() == 0)
					closeOnce(phase1Stream, phase1Closed, out, phase1Stream);
				fail(t, out, phase1Stream);
			}
			finally {
				if(budget != null)
					budget.close();
			}
		};
		admittedX.setSubscriber(xSubscriber);
	}

	private void closeOnce(OOCStream<?> stream, AtomicBoolean closed, OOCStream<?> out, OOCStream<?> workStream) {
		if(!closed.compareAndSet(false, true))
			return;
		try {
			stream.closeInput();
		}
		catch(Throwable t) {
			fail(t, out, workStream);
		}
	}

	private static <T extends AutoCloseable> void closeAndDrain(CloseableQueue<T> queue, Throwable failure) {
		try {
			queue.close();
			T value;
			while((value = queue.take()) != null)
				value.close();
		}
		catch(InterruptedException ex) {
			Thread.currentThread().interrupt();
			failure.addSuppressed(ex);
		}
		catch(Exception ex) {
			failure.addSuppressed(ex);
		}
	}

	private OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> vectorTile(int idx, MemoryAllowance allowance) {
		MaterializedStore.Lease<IndexedMatrixValue> live = _vReader.requestIfLive(idx, allowance);
		if(live != null)
			return OOCFuture.completed(LeaseQueueCallbacks.store(live));
		OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> pending = new OOCFuture<>();
		_vReader.request(idx, allowance).whenComplete((lease, error) -> {
			if(error != null)
				pending.completeExceptionally(error);
			else if(lease == null)
				pending.completeExceptionally(
					new DMLRuntimeException("MapMMChain v store reader closed before tile " + idx + " was served."));
			else
				pending.complete(LeaseQueueCallbacks.store(lease));
		});
		return pending;
	}

	private void accumulateTable(int slot, MatrixIndexes index, MatrixBlock block, BinaryOperator plus,
		ReservationBudget budget) {
		ManagedPayload<IndexedMatrixValue> payload = payload(index, block, budget);
		while(true) {
			StateLease<IndexedMatrixValue> existing;
			try {
				existing = await(_accumulators.putOrTake(slot, payload, budget));
			}
			catch(RuntimeException ex) {
				payload.release();
				throw ex;
			}
			if(existing == null)
				return;
			MatrixBlock merged;
			try(existing) {
				merged = ((MatrixBlock)existing.value().getValue())
					.binaryOperations(plus, payload.value().getValue(), new MatrixBlock());
			}
			finally {
				payload.release();
			}
			payload = payload(index, merged, budget);
		}
	}

	private ManagedPayload<IndexedMatrixValue> payload(MatrixIndexes index, MatrixBlock block,
		MemoryAllowance owner) {
		long bytes = block.getExactSerializedSize();
		owner.reserveBlocking(bytes);
		return new ManagedPayload<>(new IndexedMatrixValue(index, block), bytes, owner);
	}

	private void installRetainedCallback(int slot, OOCStream.QueueCallback<IndexedMatrixValue> callback,
		ReservationBudget budget) {
		if(callback instanceof MaterializedCallback pinned) {
			try {
				_retainedTiles.putReference(slot, pinned.pinnedEntry());
			}
			finally {
				pinned.close();
			}
			return;
		}
		ManagedPayload<IndexedMatrixValue> payload;
		if(callback instanceof InMemoryQueueCallback managed && managed.getManagedBytes() > 0) {
			payload = managed.extractManagedPayload();
			managed.close();
		}
		else {
			IndexedMatrixValue value = callback.get();
			long bytes = ((MatrixBlock)value.getValue()).getExactSerializedSize();
			budget.reserveBlocking(bytes);
			payload = new ManagedPayload<>(value, bytes, budget);
			callback.close();
		}
		try {
			_retainedTiles.put(slot, payload);
		}
		catch(RuntimeException ex) {
			payload.release();
			throw ex;
		}
	}

	private void schedulePhase2Row(int row, int numColBlocks, OOCStream<Phase2Request> phase2Requests) {
		for(int col = 0; col < numColBlocks; col++)
			phase2Requests.enqueue(new Phase2Request(row, col));
	}

	private void emitOutputs(OOCStream<IndexedMatrixValue> out, int numColBlocks, int qBase,
		long outputBudgetBytes) {
		for(int col = 0; col < numColBlocks; col++) {
			ReservationBudget budget = OOCInstructionUtils.reserveBudget(_allowance, outputBudgetBytes);
			try {
				StateLease<IndexedMatrixValue> qLease = await(_accumulators.take(qBase + col, budget));
				if(qLease == null)
					continue;
				IndexedMatrixValue output;
				try(qLease) {
					output = new IndexedMatrixValue(new MatrixIndexes(col + 1L, 1L),
						(MatrixBlock) qLease.value().getValue());
				}
				OOCInstructionUtils.enqueueExact(out, output, budget);
				budget = null;
			}
			finally {
				if(budget != null)
					budget.close();
			}
		}
	}

	private void fail(Throwable t, OOCStream<?> out, OOCStream<?> workStream) {
		if(!_terminated.compareAndSet(false, true))
			return;
		DMLRuntimeException re = DMLRuntimeException.of(t);
		try {
			if(workStream != null)
				workStream.propagateFailure(re);
		}
		catch(Throwable ignored) {
			// The externally visible output stream must still see the failure.
		}
		try {
			out.propagateFailure(re);
		}
		catch(Throwable ignored) {
		}
		try {
			if(_sc != null)
				_sc.failAll(re);
		}
		finally {
			onComplete();
		}
	}

	private void complete(OOCStream<IndexedMatrixValue> out) {
		if(_terminated.compareAndSet(false, true)) {
			try {
				out.closeInput();
			}
			finally {
				onComplete();
			}
		}
	}

	private <T> T await(OOCFuture<T> future) {
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

	private static MatrixBlock multTransposeVector(MatrixBlock x, MatrixBlock u) {
		int rows = x.getNumRows();
		int cols = x.getNumColumns();
		MatrixBlock out = new MatrixBlock(cols, 1, false);
		out.allocateDenseBlock();
		double[] outVals = out.getDenseBlockValues();

		if(x.isInSparseFormat()) {
			SparseBlock a = x.getSparseBlock();
			if(a != null) {
				if(u.isInSparseFormat()) {
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
					double[] uvals = u.getDenseBlockValues();
					for(int i = 0; i < rows; i++) {
						if(a.isEmpty(i))
							continue;
						double uval = uvals[i];
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
			}
		}
		else {
			DenseBlock a = x.getDenseBlock();
			if(u.isInSparseFormat()) {
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
			else {
				double[] uvals = u.getDenseBlockValues();
				for(int i = 0; i < rows; i++) {
					double uval = uvals[i];
					if(uval == 0)
						continue;
					double[] avals = a.values(i);
					int apos = a.pos(i);
					for(int j = 0; j < cols; j++)
						outVals[j] += uval * avals[apos + j];
				}
			}
		}

		out.recomputeNonZeros();
		out.examSparsity();
		return out;
	}

	private static long saturatingMultiply(long value, long factor) {
		if(value > Long.MAX_VALUE / factor)
			return Long.MAX_VALUE;
		return value * factor;
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

	public OOCStreamable<IndexedMatrixValue> getXStreamable() {
		return getInputStream(0);
	}

	public OOCStreamable<IndexedMatrixValue> getVStreamable() {
		return getInputStream(1);
	}

	public OOCStreamable<IndexedMatrixValue> getWStreamable() {
		return getInputStreams().size() > 2 ? getInputStream(2) : null;
	}

	public OOCStreamable<IndexedMatrixValue> getOutputStreamable() {
		return getOutputStream(0);
	}

	public ChainType getType() {
		return _type;
	}

	public StreamContext getContext() {
		return _sc;
	}

	private static final class MMChainWorkload implements AutoCloseable {
		private final OOCStream.QueueCallback<IndexedMatrixValue> cb1;
		private final OOCStream.QueueCallback<IndexedMatrixValue> cb2;
		private ReservationBudget budget;

		private MMChainWorkload(OOCStream.QueueCallback<IndexedMatrixValue> cb1,
			OOCStream.QueueCallback<IndexedMatrixValue> cb2, ReservationBudget budget) {
			this.cb1 = cb1;
			this.cb2 = cb2;
			this.budget = budget;
		}

		private synchronized ReservationBudget detachBudget() {
			ReservationBudget detached = budget;
			budget = null;
			return detached;
		}

		@Override
		public synchronized void close() {
			try {
				cb1.close();
			}
			finally {
				try {
					cb2.close();
				}
				finally {
					if(budget != null) {
						budget.close();
						budget = null;
					}
				}
			}
		}
	}

	private static final class Phase1Result implements AutoCloseable {
		private final int row;
		private final int col;
		private final MatrixBlock block;
		private final OOCStream.QueueCallback<IndexedMatrixValue> xCb;
		private ReservationBudget budget;

		private Phase1Result(int row, int col, MatrixBlock block,
			OOCStream.QueueCallback<IndexedMatrixValue> xCb, ReservationBudget budget) {
			this.row = row;
			this.col = col;
			this.block = block;
			this.xCb = xCb;
			this.budget = budget;
		}

		private synchronized ReservationBudget detachBudget() {
			ReservationBudget detached = budget;
			budget = null;
			return detached;
		}

		@Override
		public synchronized void close() {
			try {
				xCb.close();
			}
			finally {
				if(budget != null) {
					budget.close();
					budget = null;
				}
			}
		}
	}

	private static final class Phase2Result implements AutoCloseable {
		private final int col;
		private final MatrixBlock block;
		private ReservationBudget budget;

		private Phase2Result(int col, MatrixBlock block, ReservationBudget budget) {
			this.col = col;
			this.block = block;
			this.budget = budget;
		}

		private synchronized ReservationBudget detachBudget() {
			ReservationBudget detached = budget;
			budget = null;
			return detached;
		}

		@Override
		public synchronized void close() {
			if(budget != null) {
				budget.close();
				budget = null;
			}
		}
	}

	private record Phase2Request(int row, int col) {
	}
}
