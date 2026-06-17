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
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.PlaybackStream;
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
import org.apache.sysds.runtime.ooc.cache.io.CloseableQueue;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreBinding;
import org.apache.sysds.runtime.ooc.planning.OOCStoreRequest;
import org.apache.sysds.runtime.ooc.store.MaterializationSink;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MultiplicityLiveness;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;
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
	private final OOCStreamable<IndexedMatrixValue> _xStreamable;
	private final OOCStreamable<IndexedMatrixValue> _vStreamable;
	private final OOCStreamable<IndexedMatrixValue> _wStreamable;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final ChainType _type;
	private final StreamContext _sc;
	private final AtomicBoolean _terminated = new AtomicBoolean(false);
	private final AtomicBoolean _storeReleased = new AtomicBoolean(false);
	private OperatorStateTable<IndexedMatrixValue> _table;
	private OOCStoreBinding _storeBinding;
	private volatile MaterializedStore.IndexedReader<IndexedMatrixValue> _vReader;

	private MapMMChainOOCPrimitive(List<OOCPrimitive> children, OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> wStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, ChainType type, StreamContext sc) {
		super(children);
		_xStreamable = reserveLazyHandle(xStreamable);
		_vStreamable = reserveLazyHandle(vStreamable);
		_wStreamable = reserveLazyHandle(wStreamable);
		_outputStreamable = outputStreamable;
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
	public List<OOCStreamable<?>> getInputStreams() {
		ArrayList<OOCStreamable<?>> inputs = new ArrayList<>(3);
		inputs.add(_xStreamable);
		inputs.add(_vStreamable);
		if(_wStreamable != null)
			inputs.add(_wStreamable);
		return inputs;
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStreamable);
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
	public boolean requiresStateTable() {
		return true;
	}

	@Override
	public void bindStateTable(OperatorStateTable<IndexedMatrixValue> table) {
		_table = table;
	}

	@Override
	public OOCStoreRequest requiresStore() {
		return new OOCStoreRequest(_vStreamable, ix -> Math.toIntExact(ix.getRowIndex() - 1), 1, 1);
	}

	@Override
	public void bindStore(OOCStoreBinding store) {
		_storeBinding = store;
	}

	@Override
	public void onComplete() {
		try {
			releaseStore();
			if(_table != null)
				_table.close();
		}
		finally {
			super.onComplete();
		}
	}

	private void releaseStore() {
		if(_storeBinding == null || !_storeReleased.compareAndSet(false, true))
			return;
		if(_vReader != null)
			_vReader.close();
		_storeBinding.release();
	}

	@Override
	public long getDenseTileMemoryFactor() {
		return 2;
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
		if(_type != ChainType.XtXv || _wStreamable != null)
			throw new UnsupportedOperationException("MapMMChainOOCPrimitive currently only supports XtXv.");
		if(_table == null)
			throw new IllegalStateException("MapMMChain requires a bound OperatorStateTable.");
		if(_storeBinding == null)
			throw new IllegalStateException("MapMMChain requires a bound MaterializedStore for v.");

		final OOCStream<IndexedMatrixValue> x = _xStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		final int numVBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(_vStreamable.getDataCharacteristics()));
		final int numColBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(x.getDataCharacteristics()));
		final int numRowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(x.getDataCharacteristics()));
		final int uBase = numVBlocks;
		final int qBase = uBase + numRowBlocks;
		final int xBase = qBase + numColBlocks;
		final AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		final AggregateBinaryOperator mmOp = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		final BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		final AtomicIntegerArray seenPerRow = new AtomicIntegerArray(numRowBlocks);

		_storeBinding.completion().whenComplete((ignored, error) -> {
			if(error != null) {
				fail(error, out, null);
				return;
			}
			try {
				_vReader = _storeBinding.openIndexedReader(
					new MultiplicityLiveness(numVBlocks, numRowBlocks), _allowance);
				startXtXv(x, out, numColBlocks, uBase, qBase, xBase, mmOp, plus, seenPerRow);
			}
			catch(Throwable t) {
				fail(t, out, null);
			}
		});
		try {
			_storeBinding.attach(_vStreamable);
		}
		catch(Throwable t) {
			fail(t, out, null);
		}
	}

	private void startXtXv(OOCStream<IndexedMatrixValue> x, OOCStream<IndexedMatrixValue> out,
		int numColBlocks, int uBase, int qBase, int xBase, AggregateBinaryOperator mmOp, BinaryOperator plus,
		AtomicIntegerArray seenPerRow) {
		final OOCStream<MMChainWorkload> phase1Stream = new SubscribableTaskQueue<>();
		final OOCStream<MMChainWorkload> phase2Stream = new SubscribableTaskQueue<>();
		final CloseableQueue<Phase1Result> phase1Results = new CloseableQueue<>();
		final CloseableQueue<Phase2Result> phase2Results = new CloseableQueue<>();
		final AtomicBoolean phase1Closed = new AtomicBoolean(false);
		final AtomicBoolean phase2Closed = new AtomicBoolean(false);

		CompletableFuture<Void> phase1Future = OOCInstructionUtils.submitOOCTasks(phase1Stream, wl -> {
			var vCb = wl.get().cb1;
			var xCb = wl.get().cb2;
			OOCStream.QueueCallback<IndexedMatrixValue> retainedX = null;
			boolean resultQueued = false;
			try(vCb; xCb) {
				MatrixIndexes xIx = xCb.get().getIndexes();
				int row = Math.toIntExact(xIx.getRowIndex() - 1);
				int col = Math.toIntExact(xIx.getColumnIndex() - 1);
				MatrixBlock xb = (MatrixBlock)xCb.get().getValue();
				MatrixBlock vb = (MatrixBlock)vCb.get().getValue();
				MatrixBlock ub = xb.aggregateBinaryOperations(xb, vb, new MatrixBlock(), mmOp);
				retainedX = xCb.keepOpen();
				phase1Results.enqueueIfOpen(new Phase1Result(row, col, ub, retainedX));
				resultQueued = true;
			}
			catch(InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new DMLRuntimeException(e);
			}
			finally {
				if(!resultQueued && retainedX != null)
					retainedX.close();
			}
		}, _sc);
		phase1Future.whenComplete((ignored, error) -> {
			try {
				phase1Results.close();
			}
			catch(InterruptedException e) {
				Thread.currentThread().interrupt();
				fail(e, out, phase2Stream);
				return;
			}
			if(error != null)
				fail(error, out, phase2Stream);
		});

		CompletableFuture<Void> phase2Future = OOCInstructionUtils.submitOOCTasks(phase2Stream, wl -> {
			var uCb = wl.get().cb1;
			var xCb = wl.get().cb2;
			try(uCb; xCb) {
				MatrixIndexes xIx = xCb.get().getIndexes();
				int col = Math.toIntExact(xIx.getColumnIndex() - 1);
				MatrixBlock xb = (MatrixBlock)xCb.get().getValue();
				MatrixBlock ub = (MatrixBlock)uCb.get().getValue();
				MatrixBlock qb = multTransposeVector(xb, ub);
				phase2Results.enqueueIfOpen(new Phase2Result(col, qb));
			}
			catch(InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new DMLRuntimeException(e);
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

		new Thread(() -> {
			try {
				Phase2Result result;
				while((result = phase2Results.take()) != null)
					accumulateTable(qBase + result.col(), new MatrixIndexes(result.col() + 1L, 1L),
						result.block(), plus);
				emitOutputs(out, numColBlocks, qBase);
				complete(out);
			}
			catch(Throwable t) {
				fail(t, out, null);
			}
		}, "ooc-mapmmchain-q-coordinator").start();

		new Thread(() -> {
			try {
				Phase1Result result;
				while((result = phase1Results.take()) != null) {
					boolean retainedInstalled = false;
					try {
						accumulateTable(uBase + result.row(), new MatrixIndexes(result.row() + 1L, 1L),
							result.block(), plus);
						installRetainedCallback(xBase + result.row() * numColBlocks + result.col(), result.xCb());
						retainedInstalled = true;
						if(seenPerRow.incrementAndGet(result.row()) == numColBlocks)
							schedulePhase2Row(result.row(), numColBlocks, uBase, xBase, phase2Stream);
					}
					finally {
						if(!retainedInstalled)
							result.xCb().close();
					}
				}
				closeOnce(phase2Stream, phase2Closed, out, phase2Stream);
			}
			catch(Throwable t) {
				fail(t, out, phase2Stream);
			}
		}, "ooc-mapmmchain-u-coordinator").start();

		final AtomicInteger inflightCtr = new AtomicInteger(1);
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> xSubscriber = xcb -> {
			OOCStream.QueueCallback<IndexedMatrixValue> retainedX = null;
			boolean inflightRetained = false;
			try(xcb) {
				if(xcb.isEos()) {
					if(inflightCtr.decrementAndGet() == 0)
						closeOnce(phase1Stream, phase1Closed, out, phase1Stream);
					return;
				}
				retainedX = xcb.keepOpen();
				inflightCtr.incrementAndGet();
				inflightRetained = true;
				int col = Math.toIntExact(xcb.get().getIndexes().getColumnIndex() - 1);
				CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> vFuture = vectorTile(col);
				final var fXcb = retainedX;
				retainedX = null;
				vFuture.whenComplete((vcb, error) -> {
					boolean enqueued = false;
					try {
						if(error != null)
							throw DMLRuntimeException.of(error);
						if(vcb == null)
							throw new IllegalStateException("Missing broadcast vector tile for column block " + col);
						phase1Stream.enqueue(new MMChainWorkload(vcb, fXcb));
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
		};
		if(x instanceof PlaybackStream playback)
			playback.setSubscriber(xSubscriber, _allowance, _allocFn);
		else
			x.setSubscriber(xSubscriber);
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

	private CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> vectorTile(int idx) {
		MaterializedStore.Lease<IndexedMatrixValue> live = _vReader.requestIfLive(idx);
		if(live != null)
			return CompletableFuture.completedFuture(new StoreLeaseCallback(live));
		CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> pending = new CompletableFuture<>();
		_vReader.request(idx).whenComplete((lease, error) -> {
			if(error != null)
				pending.completeExceptionally(error);
			else if(lease == null)
				pending.completeExceptionally(
					new DMLRuntimeException("MapMMChain v store reader closed before tile " + idx + " was served."));
			else
				pending.complete(new StoreLeaseCallback(lease));
		});
		return pending;
	}

	private void accumulateTable(int slot, MatrixIndexes index, MatrixBlock block, BinaryOperator plus) {
		ManagedPayload<IndexedMatrixValue> payload = payload(index, block);
		while(true) {
			OperatorStateTable.StateLease<IndexedMatrixValue> existing;
			try {
				existing = await(_table.installOrTake(slot, payload));
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
			payload = payload(index, merged);
		}
	}

	private ManagedPayload<IndexedMatrixValue> payload(MatrixIndexes index, MatrixBlock block) {
		long bytes = block.getExactSerializedSize();
		_allowance.reserveBlocking(bytes);
		return new ManagedPayload<>(new IndexedMatrixValue(index, block), bytes, _allowance);
	}

	private void installRetainedCallback(int slot, OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback instanceof MaterializationSink.PinnedLeaseCallback pinned) {
			try {
				_table.installReference(slot, pinned.pinnedEntry());
			}
			finally {
				pinned.close();
			}
			return;
		}
		ManagedPayload<IndexedMatrixValue> payload;
		if(callback instanceof InMemoryQueueCallback managed) {
			payload = managed.extractManagedPayload();
			managed.close();
		}
		else {
			IndexedMatrixValue value = callback.get();
			long bytes = ((MatrixBlock)value.getValue()).getExactSerializedSize();
			_allowance.reserveBlocking(bytes);
			payload = new ManagedPayload<>(value, bytes, _allowance);
			callback.close();
		}
		try {
			_table.install(slot, payload);
		}
		catch(RuntimeException ex) {
			payload.release();
			throw ex;
		}
	}

	private void schedulePhase2Row(int row, int numColBlocks, int uBase, int xBase,
		OOCStream<MMChainWorkload> phase2Stream) {
		OperatorStateTable.StateLease<IndexedMatrixValue> uLease = await(_table.take(uBase + row));
		if(uLease == null)
			throw new IllegalStateException("Missing finalized XtXv row accumulator " + row);
		try(StateLeaseCallback uCb = new StateLeaseCallback(uLease)) {
			for(int col = 0; col < numColBlocks; col++) {
				OperatorStateTable.StateLease<IndexedMatrixValue> xLease =
					await(_table.take(xBase + row * numColBlocks + col));
				if(xLease == null)
					throw new IllegalStateException("Missing retained XtXv input tile for row=" + row
						+ ", col=" + col);
				OOCStream.QueueCallback<IndexedMatrixValue> uAlias = uCb.keepOpen();
				StateLeaseCallback xCb = new StateLeaseCallback(xLease);
				boolean enqueued = false;
				try {
					phase2Stream.enqueue(new MMChainWorkload(uAlias, xCb));
					enqueued = true;
				}
				finally {
					if(!enqueued) {
						uAlias.close();
						xCb.close();
					}
				}
			}
		}
	}

	private void emitOutputs(OOCStream<IndexedMatrixValue> out, int numColBlocks, int qBase) {
		for(int col = 0; col < numColBlocks; col++) {
			OperatorStateTable.StateLease<IndexedMatrixValue> qLease = await(_table.take(qBase + col));
			if(qLease == null)
				continue;
			try(qLease) {
				OOCStream.QueueCallback<IndexedMatrixValue> output = outputCallback(
					new MatrixIndexes(col + 1L, 1L), (MatrixBlock) qLease.value().getValue());
				boolean enqueued = false;
				try {
					out.enqueue(output);
					enqueued = true;
				}
				finally {
					if(!enqueued)
						output.close();
				}
			}
		}
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> outputCallback(MatrixIndexes index, MatrixBlock block) {
		IndexedMatrixValue imv = new IndexedMatrixValue(index, block);
		long bytes = _startsRegion ? _allocFn.applyAsLong(index) : 0;
		boolean reserved = false;
		if(bytes > 0) {
			_allowance.reserveBlocking(bytes);
			reserved = true;
		}
		if(_crossBoundaries)
			return new InMemoryQueueCallback(imv, null, _allowance, reserved ? bytes : 0);
		if(reserved)
			_allowance.release(bytes);
		return new OOCStream.SimpleQueueCallback<>(imv, null);
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

	public OOCStreamable<IndexedMatrixValue> getXStreamable() {
		return _xStreamable;
	}

	public OOCStreamable<IndexedMatrixValue> getVStreamable() {
		return _vStreamable;
	}

	public OOCStreamable<IndexedMatrixValue> getWStreamable() {
		return _wStreamable;
	}

	public OOCStreamable<IndexedMatrixValue> getOutputStreamable() {
		return _outputStreamable;
	}

	public ChainType getType() {
		return _type;
	}

	public StreamContext getContext() {
		return _sc;
	}

	private static final class StoreLeaseCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final MaterializedStore.Lease<IndexedMatrixValue> _lease;
		private DMLRuntimeException _failure;
		private boolean _closed;

		private StoreLeaseCallback(MaterializedStore.Lease<IndexedMatrixValue> lease) {
			_lease = lease;
		}

		@Override
		public IndexedMatrixValue get() {
			if(_failure != null)
				throw _failure;
			return _lease.value();
		}

		@Override
		public synchronized OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed callback");
			return new StoreLeaseCallback(_lease.retain());
		}

		@Override
		public synchronized void close() {
			if(_closed)
				return;
			_closed = true;
			_lease.close();
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_failure = failure;
		}

		@Override
		public boolean isEos() {
			return false;
		}

		@Override
		public boolean isFailure() {
			return _failure != null;
		}
	}

	private static final class StateLeaseCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final OperatorStateTable.StateLease<IndexedMatrixValue> _lease;
		private DMLRuntimeException _failure;
		private int _references = 1;
		private boolean _closed;

		private StateLeaseCallback(OperatorStateTable.StateLease<IndexedMatrixValue> lease) {
			_lease = lease;
		}

		@Override
		public IndexedMatrixValue get() {
			if(_failure != null)
				throw _failure;
			return _lease.value();
		}

		@Override
		public synchronized OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed callback");
			_references++;
			return this;
		}

		@Override
		public synchronized void close() {
			if(_closed || --_references > 0)
				return;
			_closed = true;
			_lease.close();
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_failure = failure;
		}

		@Override
		public boolean isEos() {
			return false;
		}

		@Override
		public boolean isFailure() {
			return _failure != null;
		}
	}

	private record MMChainWorkload(OOCStream.QueueCallback<IndexedMatrixValue> cb1, OOCStream.QueueCallback<IndexedMatrixValue> cb2){}
	private record Phase1Result(int row, int col, MatrixBlock block, OOCStream.QueueCallback<IndexedMatrixValue> xCb){}
	private record Phase2Result(int col, MatrixBlock block){}
}
