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
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import scala.Tuple2;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

public class BroadcastOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _broadcastStreamable;
	private final OOCStreamable<IndexedMatrixValue> _streamedStreamable;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> _mergeFn;
	private final ToIntFunction<IndexedMatrixValue> _broadcastKeyFn;
	private final ToIntFunction<IndexedMatrixValue> _streamedKeyFn;
	private final int _numBroadcastTiles;
	private final int _maxBroadcastCount;
	private final boolean _rowBroadcast;
	private final StreamContext _sc;
	private CachedAllowance _cache;

	private BroadcastOOCPrimitive(OOCPrimitive broadcastPrimitive, OOCPrimitive streamedPrimitive,
		OOCStreamable<IndexedMatrixValue> broadcastStreamable, OOCStreamable<IndexedMatrixValue> streamedStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> mergeFn,
		ToIntFunction<IndexedMatrixValue> broadcastKeyFn, ToIntFunction<IndexedMatrixValue> streamedKeyFn,
		int numBroadcastTiles, int maxBroadcastCount, boolean rowBroadcast, StreamContext sc) {
		super(List.of(broadcastPrimitive, streamedPrimitive));
		_broadcastStreamable = reserveLazyHandle(broadcastStreamable);
		_streamedStreamable = reserveLazyHandle(streamedStreamable);
		_outputStreamable = outputStreamable;
		_broadcastKeyFn = broadcastKeyFn;
		_streamedKeyFn = streamedKeyFn;
		_numBroadcastTiles = numBroadcastTiles;
		_maxBroadcastCount = maxBroadcastCount;
		_rowBroadcast = rowBroadcast;
		_mergeFn = mergeFn;
		_sc = sc;
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
	public List<OOCStreamable<?>> getInputStreams() {
		return List.of(_broadcastStreamable, _streamedStreamable);
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

	private AtomicIntegerArray _broadcastCount;
	private int _maxCount;

	@Override
	public void startExecution() {
		final OOCStream<IndexedMatrixValue> broadcastStream = getBroadcastStreamable().getReadStream();
		final OOCStream<IndexedMatrixValue> streamedStream = getStreamedStreamable().getReadStream();
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		final DataCharacteristics dcBroadcast = broadcastStream.getDataCharacteristics();
		final DataCharacteristics dc = streamedStream.getDataCharacteristics();
		_maxCount = _maxBroadcastCount > 0 ? _maxBroadcastCount :
			(int)(_rowBroadcast ? OOCUtils.getNumRowBlocks(dc) : OOCUtils.getNumColBlocks(dc));
		final int nBroadcastTiles = _numBroadcastTiles > 0 ? _numBroadcastTiles :
			(int)(_rowBroadcast ? OOCUtils.getNumColBlocks(dc) : OOCUtils.getNumRowBlocks(dc));
		_broadcastCount = new AtomicIntegerArray(nBroadcastTiles);

		final CompletableFuture<Void> buildFuture = new CompletableFuture<>();

		new Thread(() -> {
			try {
				OOCStream.QueueCallback<IndexedMatrixValue> cb;

				while((cb = broadcastStream.dequeueCB()) != null && !cb.isEos()) {
					try {
						IndexedMatrixValue imv = cb.get();
						int idx = _broadcastKeyFn != null ? _broadcastKeyFn.applyAsInt(imv) :
							(int) (_rowBroadcast ? imv.getIndexes().getColumnIndex() - 1 :
							imv.getIndexes().getRowIndex() - 1);
						_cache.handover(cb.keepOpen(), idx);
					}
					finally {
						cb.close();
					}
				}
				if(cb != null)
					cb.close();
				buildFuture.complete(null);
			}
			catch(Throwable t) {
				DMLRuntimeException re = DMLRuntimeException.of(t);
				out.propagateFailure(re);
				buildFuture.completeExceptionally(re);
			}
		}).start();

		buildFuture.thenRun(() -> {
			final SubscribableTaskQueue<IndexedMatrixValue> intermediate = new SubscribableTaskQueue<>();
			final SubscribableTaskQueue<Tuple2<OOCStream.QueueCallback<IndexedMatrixValue>, OOCStream.QueueCallback<IndexedMatrixValue>>> int2 = new SubscribableTaskQueue<>();
			final AtomicInteger inflightCtr = new AtomicInteger(1);
			CompletableFuture<Void> future = new CompletableFuture<>();
				OOCInstructionUtils.submitOOCTasks(streamedStream, cb -> {
					IndexedMatrixValue result;
					long bytesToReserve;
					try(cb) {
						bytesToReserve = _allocFn.applyAsLong(cb.get().getIndexes());
						if(_startsRegion && !_allowance.tryReserve(bytesToReserve)) {
							intermediate.enqueue(cb.keepOpen());
							return;
						}
					int idx = _streamedKeyFn != null ? _streamedKeyFn.applyAsInt(cb.get()) :
						(int) (_rowBroadcast ? cb.get().getIndexes().getColumnIndex() - 1 :
						cb.get().getIndexes().getRowIndex() - 1);
					var broadcast = _cache.get(idx);
					if(!broadcast.isDone()) {
						final var fCb = cb.keepOpen();
						inflightCtr.incrementAndGet();
						broadcast.thenAccept(bcb -> {
							int2.enqueue(new Tuple2<>(bcb, fCb));
							if(inflightCtr.decrementAndGet() == 0)
								future.complete(null);
						});
						return;
					}
					try(var bcb = broadcast.getNow(null)) {
						result = process(idx, bcb, cb);
					}
				}
				out.enqueue(callbackOf(result, bytesToReserve));
			}, _sc).thenRun(() -> {
				if(inflightCtr.decrementAndGet() == 0)
					future.complete(null);
			});
			future.thenRun(intermediate::closeInput);
			var future2 = OOCInstructionUtils.submitOOCTasks(int2, c -> {
				var bcb = c.get()._1;
				var cb = c.get()._2;
				IndexedMatrixValue result;
				try(bcb; cb) {
					int idx = _streamedKeyFn != null ? _streamedKeyFn.applyAsInt(cb.get()) :
						(int) (_rowBroadcast ? cb.get().getIndexes().getColumnIndex() - 1 :
						cb.get().getIndexes().getRowIndex() - 1);
					result = process(idx, bcb, cb);
				}
				out.enqueue(callbackOf(result, _allocFn.applyAsLong(result.getIndexes())));
			}, _sc);
			CompletableFuture<Void> future3 = new CompletableFuture<>();
				new Thread(() -> {
					OOCStream.QueueCallback<IndexedMatrixValue> tmp;
					AtomicInteger ctr = new AtomicInteger(1);
					while((tmp = intermediate.dequeueCB()) != null && !tmp.isEos()) {
						long bytesToReserve = _allocFn.applyAsLong(tmp.get().getIndexes());
						if(_startsRegion)
							_allowance.reserveBlocking(bytesToReserve);
						int idx = _streamedKeyFn != null ? _streamedKeyFn.applyAsInt(tmp.get()) :
							(int) (_rowBroadcast ? tmp.get().getIndexes().getColumnIndex() - 1 :
							tmp.get().getIndexes().getRowIndex() - 1);
					var bcbFuture = _cache.get(idx);
					final var fCb = tmp.keepOpen();
					ctr.incrementAndGet();
					bcbFuture.thenAccept(bcb -> {
						int2.enqueue(new Tuple2<>(bcb, fCb));
						if(ctr.decrementAndGet() == 0)
							future3.complete(null);
					});
					tmp.close();
				}
				if(tmp != null)
					tmp.close();
				if(ctr.decrementAndGet() == 0)
					future3.complete(null);
			}).start();
			CompletableFuture.allOf(future, future3).thenRun(int2::closeInput);
			future2.thenRun(out::closeInput).thenRun(this::onComplete);
		});
	}

	private IndexedMatrixValue process(int idx, OOCStream.QueueCallback<IndexedMatrixValue> bcb,
		OOCStream.QueueCallback<IndexedMatrixValue> cb) {
		var imv = new IndexedMatrixValue(cb.get().getIndexes(), _mergeFn.apply(bcb.get(), cb.get()));
		int cnt = _broadcastCount.incrementAndGet(idx);
		if(cnt == _maxCount)
			_cache.clear(idx);
		return imv;
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> callbackOf(IndexedMatrixValue imv, long attachedBytes) {
		if(_crossBoundaries)
			return new InMemoryQueueCallback(imv, null, _allowance, attachedBytes);
		return new OOCStream.SimpleQueueCallback<>(imv, null);
	}

	public OOCStreamable<IndexedMatrixValue> getBroadcastStreamable() {
		return _broadcastStreamable;
	}

	public OOCStreamable<IndexedMatrixValue> getStreamedStreamable() {
		return _streamedStreamable;
	}

	public OOCStreamable<IndexedMatrixValue> getOutputStreamable() {
		return _outputStreamable;
	}
}
