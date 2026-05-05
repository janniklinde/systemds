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
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiFunction;

public class BroadcastOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _broadcastStreamable;
	private final OOCStreamable<IndexedMatrixValue> _streamedStreamable;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> _mergeFn;
	private final boolean _rowBroadcast;
	private final StreamContext _sc;
	private CachedAllowance _cache;

	private BroadcastOOCPrimitive(OOCPrimitive broadcastPrimitive, OOCPrimitive streamedPrimitive,
		OOCStreamable<IndexedMatrixValue> broadcastStreamable, OOCStreamable<IndexedMatrixValue> streamedStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> mergeFn,
		boolean rowBroadcast, StreamContext sc) {
		super(List.of(broadcastPrimitive, streamedPrimitive));
		_broadcastStreamable = broadcastStreamable;
		_streamedStreamable = streamedStreamable;
		_outputStreamable = outputStreamable;
		_rowBroadcast = rowBroadcast;
		_mergeFn = mergeFn;
		_sc = sc;
	}

	public BroadcastOOCPrimitive(OOCStreamable<IndexedMatrixValue> broadcastStreamable,
		OOCStreamable<IndexedMatrixValue> streamedStreamable, OOCStreamable<IndexedMatrixValue> outputStreamable,
		BiFunction<IndexedMatrixValue, IndexedMatrixValue, MatrixBlock> mergeFn, boolean rowBroadcast, StreamContext sc) {
		this(broadcastStreamable == null ? null : broadcastStreamable.getPrimitive(),
			streamedStreamable == null ? null : streamedStreamable.getPrimitive(), broadcastStreamable,
			streamedStreamable, outputStreamable, mergeFn, rowBroadcast, sc);
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
		if(_pattern == OOCAccessPattern.UNSET)
			_pattern = OOCAccessPattern.ANY;
		getChildren().forEach(child -> child.requestPattern(_pattern));
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = _pattern == OOCAccessPattern.UNSET ? accessPattern : _pattern.preferred(accessPattern);
		getChildren().forEach(child -> child.requestPattern(_pattern));
	}

	@Override
	public void startExecution() {
		final OOCStream<IndexedMatrixValue> broadcastStream = getBroadcastStreamable().getReadStream();
		final OOCStream<IndexedMatrixValue> streamedStream = getStreamedStreamable().getReadStream();
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		final DataCharacteristics dcBroadcast = broadcastStream.getDataCharacteristics();
		final DataCharacteristics dc = streamedStream.getDataCharacteristics();
		final long nBroadcastConsumptions = _rowBroadcast ? OOCUtils.getNumRowBlocks(dc) : OOCUtils.getNumColBlocks(dc);
		final int nBroadcastTiles = (int)(_rowBroadcast ? OOCUtils.getNumColBlocks(dc) : OOCUtils.getNumRowBlocks(dc));
		final AtomicIntegerArray broadcastCount = new AtomicIntegerArray(nBroadcastTiles);

		final CompletableFuture<Void> buildFuture = new CompletableFuture<>();

		new Thread(() -> {
			try {
				OOCStream.QueueCallback<IndexedMatrixValue> cb;

				while(!(cb = broadcastStream.dequeueCB()).isEos()) {
					try {
						IndexedMatrixValue imv = cb.get();
						int idx = (int) (_rowBroadcast ? imv.getIndexes().getColumnIndex() - 1 :
							imv.getIndexes().getRowIndex() - 1);
						_cache.handover(cb.keepOpen(), idx);
					}
					finally {
						cb.close();
					}
				}
				cb.close();
				buildFuture.complete(null);
			}
			catch(Throwable t) {
				DMLRuntimeException re = DMLRuntimeException.of(t);
				out.propagateFailure(re);
				buildFuture.completeExceptionally(re);
			}
		}).start();

		final SubscribableTaskQueue<IndexedMatrixValue> intermediate = new SubscribableTaskQueue<>();
		final SubscribableTaskQueue<Tuple2<OOCStream.QueueCallback<IndexedMatrixValue>, OOCStream.QueueCallback<IndexedMatrixValue>>> int2 = new SubscribableTaskQueue<>();
		OOCInstructionUtils.submitOOCTasks(streamedStream, cb -> {
			try(cb) {
				long bytesToReserve = _allocFn.applyAsLong(cb.get().getIndexes());
				if(!_allowance.tryReserve(bytesToReserve)) {
					intermediate.enqueue(cb.keepOpen());
					return;
				}
				int idx = (int) (_rowBroadcast ? cb.get().getIndexes().getColumnIndex() - 1 :
					cb.get().getIndexes().getRowIndex() - 1);
				var broadcast = _cache.get(idx);
				if(!broadcast.isDone()) {
					final var fCb = cb.keepOpen();
					broadcast.thenAccept(bcb -> int2.enqueue(new Tuple2<>(bcb, fCb)));
					return;
				}
				var imv = new IndexedMatrixValue(cb.get().getIndexes(), _mergeFn.apply(broadcast.getNow(null).get(), cb.get()));
				int cnt = broadcastCount.incrementAndGet(idx);
				// TODO emit if possible

				if(_crossBoundaries)
					out.enqueue(new InMemoryQueueCallback(imv, null, _allowance, bytesToReserve));
				else
					out.enqueue(imv);
			}
		}, _sc);
		// TODO handle intermediate and int2 stream
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
