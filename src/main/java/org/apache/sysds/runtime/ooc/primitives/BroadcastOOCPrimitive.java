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
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedInputRequest;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.LeaseQueueCallbacks;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MultiplicityLiveness;
import org.apache.sysds.runtime.ooc.store.OOCMaterializedView;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

public class BroadcastOOCPrimitive extends OOCPrimitive {
	private final BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> _mergeFn;
	private final ToIntFunction<IndexedMatrixValue> _broadcastKeyFn;
	private final ToIntFunction<IndexedMatrixValue> _streamedKeyFn;
	private final int _numBroadcastTiles;
	private final int _maxBroadcastCount;
	private final boolean _rowBroadcast;
	private final StreamContext _sc;
	private OOCMaterializedView _broadcastView;
	private volatile MaterializedStore.IndexedReader<IndexedMatrixValue> _reader;
	private final AtomicBoolean _storeReleased = new AtomicBoolean(false);

	private BroadcastOOCPrimitive(OOCPrimitive broadcastPrimitive, OOCPrimitive streamedPrimitive,
		OOCStreamable<IndexedMatrixValue> broadcastStreamable, OOCStreamable<IndexedMatrixValue> streamedStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> mergeFn,
		ToIntFunction<IndexedMatrixValue> broadcastKeyFn, ToIntFunction<IndexedMatrixValue> streamedKeyFn,
		int numBroadcastTiles, int maxBroadcastCount, boolean rowBroadcast, StreamContext sc) {
		super(childrenOf(broadcastPrimitive, streamedPrimitive),
			List.of(broadcastStreamable, streamedStreamable), List.of(outputStreamable));
		_broadcastKeyFn = broadcastKeyFn;
		_streamedKeyFn = streamedKeyFn;
		_numBroadcastTiles = numBroadcastTiles;
		_maxBroadcastCount = maxBroadcastCount;
		_rowBroadcast = rowBroadcast;
		_mergeFn = mergeFn;
		_sc = sc;
	}

	private static List<OOCPrimitive> childrenOf(OOCPrimitive broadcastPrimitive, OOCPrimitive streamedPrimitive) {
		ArrayList<OOCPrimitive> children = new ArrayList<>(2);
		if(broadcastPrimitive != null)
			children.add(broadcastPrimitive);
		if(streamedPrimitive != null)
			children.add(streamedPrimitive);
		return children;
	}

	public BroadcastOOCPrimitive(OOCStreamable<IndexedMatrixValue> broadcastStreamable,
		OOCStreamable<IndexedMatrixValue> streamedStreamable, OOCStreamable<IndexedMatrixValue> outputStreamable,
		BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> mergeFn, boolean rowBroadcast, StreamContext sc) {
		this(safePrimitive(broadcastStreamable),
			safePrimitive(streamedStreamable), broadcastStreamable,
			streamedStreamable, outputStreamable, mergeFn, null, null, -1, -1, rowBroadcast, sc);
	}

	public BroadcastOOCPrimitive(OOCStreamable<IndexedMatrixValue> broadcastStreamable,
		OOCStreamable<IndexedMatrixValue> streamedStreamable, OOCStreamable<IndexedMatrixValue> outputStreamable,
		BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> mergeFn,
		ToIntFunction<IndexedMatrixValue> broadcastKeyFn, ToIntFunction<IndexedMatrixValue> streamedKeyFn,
		int numBroadcastTiles, int maxBroadcastCount, StreamContext sc) {
		this(safePrimitive(broadcastStreamable),
			safePrimitive(streamedStreamable), broadcastStreamable,
			streamedStreamable, outputStreamable, mergeFn, broadcastKeyFn, streamedKeyFn, numBroadcastTiles,
			maxBroadcastCount, false, sc);
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
		return new OOCMaterializedInputRequest(0,
			OOCStoreLayout.of(this::broadcastTileIndex, this::broadcastTileIndexes), 1, 1);
	}

	@Override
	public void onComplete() {
		try {
			releaseStore();
		}
		finally {
			super.onComplete();
		}
	}

	private void releaseStore() {
		if(_broadcastView == null || !_storeReleased.compareAndSet(false, true))
			return;
		if(_reader != null)
			_reader.close();
		_broadcastView.close();
	}

	/**
	 * Linearization of broadcast tile indexes for the store sink. Custom key functions are
	 * index-based at every call site; the value-less probe makes that assumption explicit.
	 */
	private int broadcastTileIndex(MatrixIndexes ix) {
		return _broadcastKeyFn != null ? _broadcastKeyFn.applyAsInt(new IndexedMatrixValue(ix, null)) :
			(int) (_rowBroadcast ? ix.getColumnIndex() - 1 : ix.getRowIndex() - 1);
	}

	private MatrixIndexes broadcastTileIndexes(int index) {
		return _rowBroadcast ? new MatrixIndexes(1, index + 1L) : new MatrixIndexes(index + 1L, 1);
	}

	@Override
	public void inferPatterns() {
		if(_pattern.isUnset())
			_pattern = OOCAccessPattern.ANY;
		getChildren().forEach(child -> {
			if(!child.hasStartedExecution())
				child.requestPattern(_pattern);
		});
		inferPatterns(getParents());
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = _pattern == OOCAccessPattern.UNSET ? accessPattern : _pattern.preferred(accessPattern);
		getChildren().forEach(child -> {
			if(!child.hasStartedExecution())
				child.requestPattern(_pattern);
		});
	}

	@Override
	public void startExecution() {
		_broadcastView = getInputStream(0).materializedView();
		final OOCStreamable<IndexedMatrixValue> streamedStreamable = getStreamedStreamable();
		final OOCStream<IndexedMatrixValue> out = getOutputStreamable().getWriteStream();
		final DataCharacteristics dc = streamedStreamable.getDataCharacteristics();
		final int maxCount = _maxBroadcastCount > 0 ? _maxBroadcastCount :
			(int)(_rowBroadcast ? OOCUtils.getNumRowBlocks(dc) : OOCUtils.getNumColBlocks(dc));
		final int nBroadcastTiles = _numBroadcastTiles > 0 ? _numBroadcastTiles :
			(int)(_rowBroadcast ? OOCUtils.getNumColBlocks(dc) : OOCUtils.getNumRowBlocks(dc));

		final CompletableFuture<Void> buildFuture = new CompletableFuture<>();

		//The planner owns materialization and source attachment. Probing waits for the FULL declared
		//reader set to register (readersSealed), not just this reader.
		_broadcastView.completion().whenComplete((ignored, error) -> {
			if(error != null) {
				DMLRuntimeException re = DMLRuntimeException.of(error);
				out.propagateFailure(re);
				releaseStore();
				buildFuture.completeExceptionally(re);
				return;
			}
			try {
				_reader = _broadcastView.openIndexedReader(
					new MultiplicityLiveness(nBroadcastTiles, maxCount), _allowance);
			}
			catch(RuntimeException ex) {
				out.propagateFailure(DMLRuntimeException.of(ex));
				releaseStore();
				buildFuture.completeExceptionally(ex);
				return;
			}
			_broadcastView.readersSealed().whenComplete((sl, sealError) -> {
				if(sealError != null) {
					out.propagateFailure(DMLRuntimeException.of(sealError));
					releaseStore();
					buildFuture.completeExceptionally(sealError);
				}
				else
					buildFuture.complete(null);
			});
		});
		buildFuture.whenComplete((ignored, buildError) -> {
			if(buildError != null) {
				out.propagateFailure(DMLRuntimeException.of(buildError));
				releaseStore();
				onComplete();
				return;
			}
			final OOCStream<IndexedMatrixValue> streamedStream = streamedStreamable.getReadStream();
			final SubscribableTaskQueue<ProbeWork> int2 = new SubscribableTaskQueue<>();
			final long taskBudgetBytes = broadcastTaskBudgetBytes(streamedStreamable, getBroadcastStreamable(),
				getOutputStreamable());
			final OOCStream<IndexedMatrixValue> admittedStreamed =
				new AllocatedOOCStream<>(streamedStream, _allowance, input -> taskBudgetBytes,
					taskBudgetBytes > 0, ReservationBudget::admitted);
			final AtomicInteger inflightCtr = new AtomicInteger(1);
			CompletableFuture<Void> future = new CompletableFuture<>();
			OOCInstructionUtils.submitOOCTasks(admittedStreamed, cb -> {
				ReservationBudget budget = OOCInstructionUtils.detachBudget(cb);
				try {
					IndexedMatrixValue result;
					try(cb) {
						int idx = _streamedKeyFn != null ? _streamedKeyFn.applyAsInt(cb.get()) :
							(int) (_rowBroadcast ? cb.get().getIndexes().getColumnIndex() - 1 :
							cb.get().getIndexes().getRowIndex() - 1);
						var broadcast = broadcastTile(idx, budget == null ? _allowance : budget);
						if(!broadcast.isDone()) {
							final var fCb = cb.keepOpen();
							final ReservationBudget fBudget = budget;
							budget = null;
							inflightCtr.incrementAndGet();
							broadcast.whenComplete((bcb, error) -> {
								if(error != null) {
									fCb.close();
									if(fBudget != null)
										fBudget.close();
									out.propagateFailure(DMLRuntimeException.of(error));
								}
								else {
									try {
										int2.enqueue(new ProbeWork(bcb, fCb, fBudget));
									}
									catch(RuntimeException ex) {
										bcb.close();
										fCb.close();
										if(fBudget != null)
											fBudget.close();
										out.propagateFailure(DMLRuntimeException.of(ex));
									}
								}
								if(inflightCtr.decrementAndGet() == 0)
									future.complete(null);
							});
							return;
						}
						try(var bcb = broadcast.getNow(null)) {
							result = process(idx, bcb, cb);
						}
					}
					if(budget == null)
						throw new DMLRuntimeException("Missing admitted broadcast output budget.");
					OOCInstructionUtils.enqueueExact(out, result, budget);
					budget = null;
				}
				finally {
					if(budget != null)
						budget.close();
				}
			}, _sc).thenRun(() -> {
				if(inflightCtr.decrementAndGet() == 0)
					future.complete(null);
			});
			future.thenRun(int2::closeInput);
			var future2 = OOCInstructionUtils.submitOOCTasks(int2, c -> {
				ProbeWork work = c.get();
				var bcb = work._broadcast;
				var cb = work._streamed;
				ReservationBudget budget = work._budget;
				try {
					IndexedMatrixValue result;
					try(bcb; cb) {
						int idx = _streamedKeyFn != null ? _streamedKeyFn.applyAsInt(cb.get()) :
							(int) (_rowBroadcast ? cb.get().getIndexes().getColumnIndex() - 1 :
							cb.get().getIndexes().getRowIndex() - 1);
						result = process(idx, bcb, cb);
					}
					if(budget == null)
						throw new DMLRuntimeException("Missing admitted broadcast output budget.");
					OOCInstructionUtils.enqueueExact(out, result, budget);
					budget = null;
				}
				finally {
					if(budget != null)
						budget.close();
				}
			}, cb -> true, (i, cb) -> cb.get().close(), _sc);
			future2.whenComplete((finished, probeError) -> {
				try {
					if(probeError != null)
						out.propagateFailure(DMLRuntimeException.of(probeError));
					else
						out.closeInput();
				}
				finally {
					onComplete();
				}
			});
		});
	}

	/**
	 * Targeted broadcast-tile lookup behind one future shape for both backends. On the store path,
	 * the returned callback wraps an {@code IndexedReader} lease whose close is the exactly-once
	 * consumption driving multiplicity-based forgetting (no manual counting or clearing).
	 */
	private OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> broadcastTile(int idx,
		org.apache.sysds.runtime.ooc.memory.MemoryAllowance allowance) {
		MaterializedStore.Lease<IndexedMatrixValue> live = _reader.requestIfLive(idx, allowance);
		if(live != null)
			return OOCFuture.completed(LeaseQueueCallbacks.store(live));
		OOCFuture<OOCStream.QueueCallback<IndexedMatrixValue>> pending = new OOCFuture<>();
		_reader.request(idx, allowance).whenComplete((lease, error) -> {
			if(error != null)
				pending.completeExceptionally(error);
			else if(lease == null)
				pending.completeExceptionally(
					new DMLRuntimeException("Broadcast store reader closed before tile " + idx + " was served."));
			else
				pending.complete(LeaseQueueCallbacks.store(lease));
		});
		return pending;
	}

	private IndexedMatrixValue process(int idx, OOCStream.QueueCallback<IndexedMatrixValue> bcb,
		OOCStream.QueueCallback<IndexedMatrixValue> cb) {
		return new IndexedMatrixValue(cb.get().getIndexes(), _mergeFn.apply(bcb.get(), cb.get()));
	}

	private static long broadcastTaskBudgetBytes(OOCStreamable<IndexedMatrixValue> streamed,
		OOCStreamable<IndexedMatrixValue> broadcast, OOCStreamable<IndexedMatrixValue> output) {
		long tileBytes = Math.max(OOCInstructionUtils.estimateFullTileBytes(output.getDataCharacteristics()),
			Math.max(OOCInstructionUtils.estimateFullTileBytes(streamed.getDataCharacteristics()),
				OOCInstructionUtils.estimateFullTileBytes(broadcast.getDataCharacteristics())));
		return saturatingAdd(tileBytes, tileBytes);
	}

	private static long saturatingAdd(long a, long b) {
		long result = a + b;
		return result < 0 ? Long.MAX_VALUE : result;
	}

	public OOCStreamable<IndexedMatrixValue> getBroadcastStreamable() {
		return getInputStream(0);
	}

	public OOCStreamable<IndexedMatrixValue> getStreamedStreamable() {
		return getInputStream(1);
	}

	public OOCStreamable<IndexedMatrixValue> getOutputStreamable() {
		return getOutputStream(0);
	}

	private static final class ProbeWork implements AutoCloseable {
		private final OOCStream.QueueCallback<IndexedMatrixValue> _broadcast;
		private final OOCStream.QueueCallback<IndexedMatrixValue> _streamed;
		private final ReservationBudget _budget;

		private ProbeWork(OOCStream.QueueCallback<IndexedMatrixValue> broadcast,
			OOCStream.QueueCallback<IndexedMatrixValue> streamed, ReservationBudget budget) {
			_broadcast = broadcast;
			_streamed = streamed;
			_budget = budget;
		}

		@Override
		public void close() {
			try {
				_broadcast.close();
			}
			finally {
				try {
					_streamed.close();
				}
				finally {
					if(_budget != null)
						_budget.close();
				}
			}
		}
	}
}
