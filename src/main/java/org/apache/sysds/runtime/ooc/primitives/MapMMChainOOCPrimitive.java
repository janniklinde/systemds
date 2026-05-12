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
import org.apache.sysds.runtime.ooc.cache.CloseableQueue;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCPrimitiveUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
	private CachedAllowance _cache;

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
		return true;
	}

	@Override
	public void bindCache(CachedAllowance cache) {
		_cache = cache;
	}

	@Override
	public void onComplete() {
		try {
			if(_cache != null)
				_cache.shutdown();
		}
		finally {
			super.onComplete();
		}
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

		final OOCStream<IndexedMatrixValue> x = _xStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> v = _vStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		final int numVBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(v.getDataCharacteristics()));
		final int numColBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(x.getDataCharacteristics()));
		final int numRowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(x.getDataCharacteristics()));
		final int uBase = numVBlocks;
		final int qBase = uBase + numRowBlocks;
		final int xBase = qBase + numColBlocks;
		final AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		final AggregateBinaryOperator mmOp = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		final BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		final AtomicIntegerArray seenPerRow = new AtomicIntegerArray(numRowBlocks);

		OOCPrimitiveUtils.collect(v, _cache, idx -> Math.toIntExact(idx.getRowIndex() - 1))
			.thenRun(() -> {
				final OOCStream<MMChainWorkload> phase1Stream = new SubscribableTaskQueue<>();
				final OOCStream<MMChainWorkload> phase2Stream = new SubscribableTaskQueue<>();
				final CloseableQueue<Phase1Result> phase1Results = new CloseableQueue<>();
				final CloseableQueue<Phase2Result> phase2Results = new CloseableQueue<>();

				CompletableFuture<Void> phase1Future = OOCInstructionUtils.submitOOCTasks(phase1Stream, wl -> {
					var vCb = wl.get().cb1;
					var xCb = wl.get().cb2;
					try(vCb; xCb) {
						MatrixIndexes xIx = xCb.get().getIndexes();
						int row = Math.toIntExact(xIx.getRowIndex() - 1);
						int col = Math.toIntExact(xIx.getColumnIndex() - 1);
						MatrixBlock xb = (MatrixBlock)xCb.get().getValue();
						MatrixBlock vb = (MatrixBlock)vCb.get().getValue();
						MatrixBlock ub = xb.aggregateBinaryOperations(xb, vb, new MatrixBlock(), mmOp);
						try {
							phase1Results.enqueueIfOpen(new Phase1Result(row, col, ub, xCb.keepOpen()));
						}
						catch(InterruptedException e) {
							throw new DMLRuntimeException(e);
						}
					}
				}, _sc);
				phase1Future.exceptionally(t -> {
					fail(t, out, phase2Stream);
					return null;
				}).thenRun(() -> {
					try {
						phase1Results.close();
					}
					catch(InterruptedException e) {
						throw new DMLRuntimeException(e);
					}
				});

				CompletableFuture<Void> phase2Future = OOCInstructionUtils.submitOOCTasks(phase2Stream, wl -> {
					var uCb = wl.get().cb1;
					var xCb = wl.get().cb2;
					try(uCb; xCb) {
						MatrixIndexes xIx = xCb.get().getIndexes();
						int col = Math.toIntExact(xIx.getColumnIndex() - 1);
						MatrixBlock xb = (MatrixBlock) xCb.get().getValue();
						MatrixBlock ub = (MatrixBlock) uCb.get().getValue();
						MatrixBlock qb = multTransposeVector(xb, ub);
						try {
							phase2Results.enqueueIfOpen(new Phase2Result(col, qb));
						}
						catch(InterruptedException e) {
							throw new DMLRuntimeException(e);
						}
					}
				}, _sc);
				phase2Future.thenRun(() -> {
					try {
						phase2Results.close();
					}
					catch(InterruptedException e) {
						throw new DMLRuntimeException(e);
					}
				}).exceptionally(t -> {
					fail(t, out, null);
					return null;
				});

				new Thread(() -> {
					try {
						Phase2Result result;
						while((result = phase2Results.take()) != null) {
							OOCPrimitiveUtils.accumulate(
								trackedCallback(new MatrixIndexes(result.col() + 1L, 1L), result.block()),
								(left, right) -> mergeCallbacks(left, right, plus), _cache, qBase + result.col());
						}
						for(int col = 0; col < numColBlocks; col++) {
							OOCStream.QueueCallback<IndexedMatrixValue> qcb = _cache.take(qBase + col).join();
							if(qcb == null)
								continue;
							try(qcb) {
								out.enqueue(outputCallback(new MatrixIndexes(col + 1L, 1L),
									(MatrixBlock) qcb.get().getValue()));
							}
						}
						for(int col = 0; col < numVBlocks; col++)
							_cache.clear(col);
						complete(out);
					}
					catch(Throwable t) {
						fail(t, out, null);
					}
				}).start();

				new Thread(() -> {
					try {
						Phase1Result result;
						while((result = phase1Results.take()) != null) {
							try {
								OOCPrimitiveUtils.accumulate(
									trackedCallback(new MatrixIndexes(result.row() + 1L, 1L), result.block()),
									(left, right) -> mergeCallbacks(left, right, plus), _cache, uBase + result.row());
								_cache.handover(result.xCb(), xBase + result.row() * numColBlocks + result.col());
								if(seenPerRow.incrementAndGet(result.row()) == numColBlocks) {
									OOCStream.QueueCallback<IndexedMatrixValue> ucb = _cache.take(uBase + result.row()).join();
									if(ucb == null) {
										throw new IllegalStateException(
											"Missing finalized XtXv row accumulator " + result.row());
									}
									try {
										for(int col = 0; col < numColBlocks; col++) {
											OOCStream.QueueCallback<IndexedMatrixValue> xcb =
												_cache.take(xBase + result.row() * numColBlocks + col).join();
											if(xcb == null) {
												throw new IllegalStateException(
													"Missing retained XtXv input tile for row=" + result.row()
														+ ", col=" + col);
											}
											phase2Stream.enqueue(new MMChainWorkload(ucb.keepOpen(), xcb));
										}
									}
									finally {
										ucb.close();
									}
								}
							}
							catch(Throwable t) {
								result.xCb().close();
								throw t;
							}
						}
						phase2Stream.closeInput();
					}
					catch(Throwable t) {
						fail(t, out, phase2Stream);
					}
				}).start();

				final AtomicInteger inflightCtr = new AtomicInteger(1);
				Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> xSubscriber = xcb -> {
					try(xcb) {
						if(xcb.isEos()) {
							if(inflightCtr.decrementAndGet() == 0)
								phase1Stream.closeInput();
							return;
						}
						final var fXcb = xcb.keepOpen();
						inflightCtr.incrementAndGet();
							int col = Math.toIntExact(xcb.get().getIndexes().getColumnIndex() - 1);
							_cache.get(col).whenComplete((vcb, err) -> {
								try {
									if(err != null)
										throw DMLRuntimeException.of(err);
									if(vcb == null)
										throw new IllegalStateException("Missing broadcast vector tile for column block " + col);
									phase1Stream.enqueue(new MMChainWorkload(vcb.keepOpen(), fXcb.keepOpen()));
								}
								catch(Throwable t) {
									fail(t, out, phase1Stream);
								}
								finally {
									if(vcb != null)
										vcb.close();
									fXcb.close();
									if(inflightCtr.decrementAndGet() == 0)
										phase1Stream.closeInput();
								}
							});
					}
				};
				if(x instanceof PlaybackStream playback)
					playback.setSubscriber(xSubscriber, _allowance, _allocFn);
				else
					x.setSubscriber(xSubscriber);
			}).exceptionally(t -> {
				fail(t, out, null);
				return null;
			});
	}

	private void processRows(OOCStream<IndexedMatrixValue> x, OOCStream<IndexedMatrixValue> out,
		OOCStream.QueueCallback<IndexedMatrixValue>[] vTiles, int numColBlocks, int qBase,
		AggregateBinaryOperator mmOp, BinaryOperator plus) {
		OOCStream.QueueCallback<IndexedMatrixValue>[] rowTiles = newCallbackArray(numColBlocks);
		int currentRow = -1;
		OOCStream.QueueCallback<IndexedMatrixValue> cb = null;
		try {
			while((cb = x.dequeueCB()) != null && !cb.isEos()) {
				try {
					IndexedMatrixValue ximv = cb.get();
					int row = Math.toIntExact(ximv.getIndexes().getRowIndex() - 1);
					int col = Math.toIntExact(ximv.getIndexes().getColumnIndex() - 1);
					if(currentRow >= 0 && row != currentRow) {
						processRow(currentRow, rowTiles, vTiles, numColBlocks, qBase, mmOp, plus);
						closeCallbacks(rowTiles);
						rowTiles = newCallbackArray(numColBlocks);
					}
					currentRow = row;
					if(rowTiles[col] != null)
						throw new IllegalStateException("Duplicate XtXv input tile for row=" + row + ", col=" + col);
					rowTiles[col] = cb.keepOpen();
				}
				finally {
					cb.close();
				}
			}
			if(cb != null)
				cb.close();
			if(currentRow >= 0) {
				processRow(currentRow, rowTiles, vTiles, numColBlocks, qBase, mmOp, plus);
				closeCallbacks(rowTiles);
			}
			emitOutputs(out, numColBlocks, qBase);
			closeCallbacks(vTiles);
			complete(out);
		}
		catch(Throwable t) {
			if(cb != null)
				cb.close();
			closeCallbacks(rowTiles);
			closeCallbacks(vTiles);
			fail(t, out, null);
		}
	}

	private void processRow(int row, OOCStream.QueueCallback<IndexedMatrixValue>[] rowTiles,
		OOCStream.QueueCallback<IndexedMatrixValue>[] vTiles, int numColBlocks, int qBase,
		AggregateBinaryOperator mmOp, BinaryOperator plus) {
		for(int col = 0; col < numColBlocks; col++) {
			if(rowTiles[col] == null)
				throw new IllegalStateException("Missing XtXv input tile for row=" + row + ", col=" + col);
			if(vTiles[col] == null)
				throw new IllegalStateException("Missing XtXv vector tile for column block " + col);
		}

		MatrixBlock uBlock = finalizePartialU(rowTiles, vTiles, numColBlocks, mmOp, plus);
		CompletableFuture<MatrixBlock>[] qFutures = newFutureArray(numColBlocks);
		for(int col = 0; col < numColBlocks; col++) {
			final OOCStream.QueueCallback<IndexedMatrixValue> xcb = rowTiles[col].keepOpen();
			final MatrixBlock finalUBlock = uBlock;
			qFutures[col] = submitComputation(() -> {
				try(xcb) {
					return multTransposeVector((MatrixBlock) xcb.get().getValue(), finalUBlock);
				}
			});
		}

		for(int col = 0; col < numColBlocks; col++) {
			MatrixBlock partialQ = await(qFutures[col]);
			OOCPrimitiveUtils.accumulate(trackedCallback(new MatrixIndexes(col + 1L, 1L), partialQ),
				(left, right) -> mergeCallbacks(left, right, plus), _cache, qBase + col);
		}
	}

	private MatrixBlock finalizePartialU(OOCStream.QueueCallback<IndexedMatrixValue>[] rowTiles,
		OOCStream.QueueCallback<IndexedMatrixValue>[] vTiles, int numColBlocks, AggregateBinaryOperator mmOp,
		BinaryOperator plus) {
		CompletableFuture<MatrixBlock>[] uFutures = newFutureArray(numColBlocks);
		for(int col = 0; col < numColBlocks; col++) {
			final OOCStream.QueueCallback<IndexedMatrixValue> xcb = rowTiles[col].keepOpen();
			final OOCStream.QueueCallback<IndexedMatrixValue> vcb = vTiles[col].keepOpen();
			uFutures[col] = submitComputation(() -> {
				try(xcb; vcb) {
					MatrixBlock xBlock = (MatrixBlock) xcb.get().getValue();
					MatrixBlock vBlock = (MatrixBlock) vcb.get().getValue();
					return xBlock.aggregateBinaryOperations(xBlock, vBlock, new MatrixBlock(), mmOp);
				}
			});
		}

		MatrixBlock uBlock = null;
		for(int col = 0; col < numColBlocks; col++) {
			MatrixBlock partialU = await(uFutures[col]);
			if(uBlock == null)
				uBlock = partialU;
			else
				uBlock.binaryOperationsInPlace(plus, partialU);
		}
		return uBlock;
	}

	private void emitOutputs(OOCStream<IndexedMatrixValue> out, int numColBlocks, int qBase) {
		for(int col = 0; col < numColBlocks; col++) {
			OOCStream.QueueCallback<IndexedMatrixValue> qcb = _cache.take(qBase + col).join();
			if(qcb == null)
				continue;
			try(qcb) {
				out.enqueue(outputCallback(new MatrixIndexes(col + 1L, 1L),
					(MatrixBlock) qcb.get().getValue()));
			}
		}
	}

	private OOCStream.QueueCallback<IndexedMatrixValue>[] takeVectorTiles(int numVBlocks) {
		OOCStream.QueueCallback<IndexedMatrixValue>[] vTiles = newCallbackArray(numVBlocks);
		try {
			for(int col = 0; col < numVBlocks; col++) {
				OOCStream.QueueCallback<IndexedMatrixValue> vcb = _cache.take(col).join();
				if(vcb == null)
					throw new IllegalStateException("Missing XtXv vector tile for column block " + col);
				vTiles[col] = vcb;
			}
			return vTiles;
		}
		catch(Throwable t) {
			closeCallbacks(vTiles);
			throw t;
		}
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> mergeCallbacks(
		OOCStream.QueueCallback<IndexedMatrixValue> left, OOCStream.QueueCallback<IndexedMatrixValue> right,
		BinaryOperator plus) {
		MatrixIndexes outIx = right.get().getIndexes();
		MatrixBlock merged = ((MatrixBlock) left.get().getValue()).binaryOperationsInPlace(plus, right.get().getValue());
		return trackedCallback(outIx, merged);
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> trackedCallback(MatrixIndexes index, MatrixBlock block) {
		long bytes = _allocFn.applyAsLong(index);
		_allowance.reserveBlocking(bytes);
		return new InMemoryQueueCallback(new IndexedMatrixValue(index, block), null, _allowance, bytes);
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> outputCallback(MatrixIndexes index, MatrixBlock block) {
		IndexedMatrixValue imv = new IndexedMatrixValue(index, block);
		long bytes = _allocFn.applyAsLong(index);
		if(_startsRegion)
			_allowance.reserveBlocking(bytes);
		if(_crossBoundaries) {
			return new InMemoryQueueCallback(imv, null, _allowance, bytes);
		}
		return new OOCStream.SimpleQueueCallback<>(imv, null);
	}

	private void fail(Throwable t, OOCStream<?> out, OOCStream<?> workStream) {
		if(!_terminated.compareAndSet(false, true))
			return;
		DMLRuntimeException re = DMLRuntimeException.of(t);
		if(workStream != null)
			workStream.propagateFailure(re);
		out.propagateFailure(re);
		if(_sc != null)
			_sc.failAll(re);
		onComplete();
	}

	private void complete(OOCStream<IndexedMatrixValue> out) {
		if(_terminated.compareAndSet(false, true)) {
			out.closeInput();
			onComplete();
		}
	}

	private CompletableFuture<MatrixBlock> submitComputation(Computation fn) {
		CompletableFuture<MatrixBlock> future = new CompletableFuture<>();
		OOCInstructionUtils.COMPUTE_EXECUTOR.submit(() -> {
			try {
				future.complete(fn.run());
			}
			catch(Throwable t) {
				future.completeExceptionally(t);
			}
		});
		return future;
	}

	private MatrixBlock await(CompletableFuture<MatrixBlock> future) {
		try {
			return future.join();
		}
		catch(RuntimeException ex) {
			throw DMLRuntimeException.of(ex);
		}
	}

	private void closeCallbacks(OOCStream.QueueCallback<IndexedMatrixValue>[] callbacks) {
		if(callbacks == null)
			return;
		for(int i = 0; i < callbacks.length; i++) {
			OOCStream.QueueCallback<IndexedMatrixValue> cb = callbacks[i];
			if(cb != null) {
				cb.close();
				callbacks[i] = null;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private OOCStream.QueueCallback<IndexedMatrixValue>[] newCallbackArray(int size) {
		return (OOCStream.QueueCallback<IndexedMatrixValue>[]) new OOCStream.QueueCallback<?>[size];
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<MatrixBlock>[] newFutureArray(int size) {
		return (CompletableFuture<MatrixBlock>[]) new CompletableFuture<?>[size];
	}

	@FunctionalInterface
	private interface Computation {
		MatrixBlock run();
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

	private record MMChainWorkload(OOCStream.QueueCallback<IndexedMatrixValue> cb1, OOCStream.QueueCallback<IndexedMatrixValue> cb2){}
	private record Phase1Result(int row, int col, MatrixBlock block, OOCStream.QueueCallback<IndexedMatrixValue> xCb){}
	private record Phase2Result(int col, MatrixBlock block){}
}
