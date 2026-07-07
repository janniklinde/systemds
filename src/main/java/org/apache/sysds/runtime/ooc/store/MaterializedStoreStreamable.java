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

package org.apache.sysds.runtime.ooc.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.message.OOCStreamMessage;
import org.apache.sysds.runtime.util.IndexRange;

/**
 * Materialized-store backed replacement for the shared stream handle created by tee. It publishes
 * the source stream exactly once into a store, live-feeds readers that subscribe while publication is
 * active, and opens store-backed replay streams after publication completes.
 */
public final class MaterializedStoreStreamable implements OOCStreamable<IndexedMatrixValue> {
	private static final int DEFAULT_REPLAY_PREFETCH = 8;

	private final OOCStream<IndexedMatrixValue> _source;
	private final MaterializedStore<IndexedMatrixValue> _store;
	private final MemoryAllowance _allowance;
	private final OOCPrimitive _primitive;
	private final CopyOnWriteArrayList<LiveReader> _liveReaders;
	private final AtomicBoolean _complete;
	private final AtomicBoolean _deleteScheduled;
	private boolean _readersSealed;
	private int _openingReaders;
	private int _pendingReaderReservations;
	private volatile CopyOnWriteArrayList<Consumer<OOCStreamMessage>> _downstreamRelays;
	private volatile DMLRuntimeException _failure;
	private CacheableData<?> _data;
	private DataCharacteristics _dataCharacteristics;
	private int _nextIndex;

	public MaterializedStoreStreamable(OOCStream<IndexedMatrixValue> source, CacheableData<?> data) {
		_source = source;
		_data = data;
		_dataCharacteristics = data == null ? source.getDataCharacteristics() : data.getDataCharacteristics();
		_store = new MaterializedStore<>(OOCCacheManager.getGlobalCache(), CachingStream._streamSeq.getNextID());
		_allowance = new SyncMemoryAllowance(GlobalMemoryBroker.get(), 200_000_000,
			estimateDenseTileBytes(_dataCharacteristics));
		((SyncMemoryAllowance) _allowance).registerDebugOwner("MaterializedStoreStreamable@"
			+ System.identityHashCode(this) + "[store=" + System.identityHashCode(_store) + "]");
		_primitive = new MaterializedStoreBoundaryPrimitive(this, safePrimitive(source));
		_liveReaders = new CopyOnWriteArrayList<>();
		_complete = new AtomicBoolean(false);
		_deleteScheduled = new AtomicBoolean(false);
		_openingReaders = 0;
		_source.setDownstreamMessageRelay(this::messageDownstream);
		_source.setSubscriber(this::publish);
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReadStream() {
		return getReadStream(OOCAccessPattern.ROW_MAJOR);
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReadStream(OOCAccessPattern pattern) {
		DMLRuntimeException failure = _failure;
		if(failure != null)
			throw failure;
		beginReaderOpen();
		boolean readerOpenFinished = false;
		try {
			if(_complete.get()) {
				OOCStream<IndexedMatrixValue> stream = replayStream(pattern);
				finishReaderOpen();
				readerOpenFinished = true;
				return stream;
			}
			SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
			stream.setData(_data);
			stream.assignPrimitive(_primitive);
			LiveReader reader = new LiveReader(stream);
			int replayLimit;
			synchronized(this) {
				_liveReaders.add(reader);
				replayLimit = _nextIndex;
			}
			finishReaderOpen();
			readerOpenFinished = true;
			replayPublishedPrefix(reader, replayLimit);
			return stream;
		}
		finally {
			if(!readerOpenFinished)
				finishReaderOpen();
		}
	}

	private OOCStream<IndexedMatrixValue> replayStream(OOCAccessPattern pattern) {
		StoreBackedStream stream = new StoreBackedStream(_store.openReader(
			new SequentialAccessPattern(_store.size()), _allowance, DEFAULT_REPLAY_PREFETCH));
		stream.setData(_data);
		stream.setDataCharacteristics(_dataCharacteristics);
		return stream;
	}

	private void publish(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		try(callback) {
			if(callback.isFailure()) {
				DMLRuntimeException failure;
				try {
					callback.get();
					failure = new DMLRuntimeException("Source stream failed without cause.");
				}
				catch(DMLRuntimeException ex) {
					failure = ex;
				}
				fail(failure);
				return;
			}
			if(callback.isEos()) {
				complete();
				return;
			}
			publishTile(callback);
		}
		catch(Throwable t) {
			fail(DMLRuntimeException.of(t));
		}
	}

	private void publishTile(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		List<LiveReader> readers;
		int index;
		synchronized(this) {
			index = _nextIndex++;
			readers = List.copyOf(_liveReaders);
		}
		IndexedMatrixValue value = callback.get();
		StoreLiveLease<IndexedMatrixValue> lease;
		long managedBytes = callback.getManagedBytes();
		if(callback instanceof InMemoryQueueCallback managed && managedBytes > 0) {
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC STORE TRACE] publish managed store=" + System.identityHashCode(_store)
					+ " index=" + index + " bytes=" + managedBytes + " readers=" + readers.size()
					+ " cb=" + System.identityHashCode(callback));
			lease = _store.publishPinnedLive(index, managed.extractManagedPayload());
		}
		else {
			long bytes = ((MatrixBlock)value.getValue()).getExactSerializedSize();
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC STORE TRACE] publish measured store=" + System.identityHashCode(_store)
					+ " index=" + index + " bytes=" + bytes + " readers=" + readers.size()
					+ " cb=" + System.identityHashCode(callback));
			_allowance.reserveBlocking(bytes);
			try {
				lease = _store.publishPinnedLive(index, value, bytes, _allowance);
			}
			catch(RuntimeException ex) {
				_allowance.release(bytes);
				throw ex;
			}
		}
		try {
			for(LiveReader reader : readers) {
				if(OOCDebug.TRACE_HOT_PATH)
					System.out.println("[OOC STORE TRACE] retain live store=" + System.identityHashCode(_store)
						+ " index=" + index + " reader=" + System.identityHashCode(reader));
				reader.enqueueLive(LeaseQueueCallbacks.pinned(lease.retain()));
			}
		}
		finally {
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC STORE TRACE] close canonical store=" + System.identityHashCode(_store)
					+ " index=" + index);
			lease.close();
		}
	}

	private void replayPublishedPrefix(LiveReader reader, int replayLimit) {
		if(replayLimit == 0) {
			reader.finishReplay();
			return;
		}
		Thread replay = new Thread(() -> {
			try {
				for(int i = 0; i < replayLimit; i++) {
					MaterializedStore.Lease<IndexedMatrixValue> lease = _store.requestPublished(i, _allowance).get();
					if(lease != null)
						reader.enqueuePrefix(LeaseQueueCallbacks.store(lease));
				}
				reader.finishReplay();
			}
			catch(Throwable t) {
				reader.fail(DMLRuntimeException.of(t));
			}
		}, "ooc-store-live-prefix-replay");
		replay.setDaemon(true);
		replay.start();
	}

	private void complete() {
		if(!_complete.compareAndSet(false, true))
			return;
		_store.complete();
		for(LiveReader reader : _liveReaders)
			reader.closeWhenReplayDone();
		_liveReaders.clear();
		trySealReaders();
	}

	private void fail(DMLRuntimeException failure) {
		if(_failure == null)
			_failure = failure;
		for(LiveReader reader : _liveReaders)
			reader.fail(failure);
		_liveReaders.clear();
	}

	@Override
	public OOCStream<IndexedMatrixValue> getWriteStream() {
		throw new UnsupportedOperationException("Materialized store streams are read-only.");
	}

	@Override
	public boolean hasStreamCache() {
		return false;
	}

	@Override
	public CachingStream getStreamCache() {
		return null;
	}

	@Override
	public boolean hasMaterializedStore() {
		return true;
	}

	@Override
	public void scheduleMaterializedStoreDeletion() {
		_deleteScheduled.compareAndSet(false, true);
		trySealReaders();
	}

	@Override
	public synchronized void reserveLazyHandle() {
		if(_deleteScheduled.get())
			throw new DMLRuntimeException("Cannot reserve a reader for a materialized stream scheduled for deletion.");
		_pendingReaderReservations++;
	}

	@Override
	public void discardHandle() {
		synchronized(this) {
			if(_pendingReaderReservations <= 0)
				return;
			_pendingReaderReservations--;
		}
		trySealReaders();
	}

	@Override
	public boolean isProcessed() {
		return false;
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return _primitive;
	}

	private static OOCPrimitive safePrimitive(OOCStream<IndexedMatrixValue> source) {
		try {
			return source.getPrimitive();
		}
		catch(RuntimeException ex) {
			return null;
		}
	}

	private void beginReaderOpen() {
		synchronized(this) {
			if(_pendingReaderReservations > 0)
				_pendingReaderReservations--;
			else if(_deleteScheduled.get())
				throw new DMLRuntimeException("Cannot add unreserved reader to materialized stream scheduled for deletion.");
			_openingReaders++;
		}
	}

	private void finishReaderOpen() {
		synchronized(this) {
			_openingReaders--;
			if(_openingReaders < 0)
				throw new IllegalStateException("Materialized store opening-reader count underflow.");
		}
		trySealReaders();
	}

	private void trySealReaders() {
		boolean seal;
		synchronized(this) {
			seal = _deleteScheduled.get() && !_readersSealed && _pendingReaderReservations == 0
				&& _openingReaders == 0;
			if(seal)
				_readersSealed = true;
		}
		if(seal)
			_store.sealReaders();
	}

	private static long estimateDenseTileBytes(DataCharacteristics dc) {
		if(dc == null || dc.getBlocksize() <= 0 || !dc.dimsKnown()) {
			int blen = dc != null && dc.getBlocksize() > 0 ? dc.getBlocksize() : 1000;
			return MatrixBlock.estimateSizeDenseInMemory(blen, blen);
		}
		long rows = Math.min(dc.getBlocksize(), dc.getRows());
		long cols = Math.min(dc.getBlocksize(), dc.getCols());
		return MatrixBlock.estimateSizeDenseInMemory(rows, cols);
	}

	@Override
	public DataCharacteristics getDataCharacteristics() {
		return _dataCharacteristics;
	}

	@Override
	public CacheableData<?> getData() {
		return _data;
	}

	@Override
	public void setData(CacheableData<?> data) {
		_data = data;
		_dataCharacteristics = data == null ? null : data.getDataCharacteristics();
	}

	@Override
	public void messageUpstream(OOCStreamMessage msg) {
		_source.messageUpstream(msg);
	}

	@Override
	public void messageDownstream(OOCStreamMessage msg) {
		CopyOnWriteArrayList<Consumer<OOCStreamMessage>> relays = _downstreamRelays;
		if(relays == null)
			return;
		for(Consumer<OOCStreamMessage> relay : relays) {
			if(msg.isCancelled())
				break;
			relay.accept(msg);
		}
	}

	@Override
	public void setUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		_source.setUpstreamMessageRelay(relay);
	}

	@Override
	public void setDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		addDownstreamMessageRelay(relay);
	}

	@Override
	public void addUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		_source.addUpstreamMessageRelay(relay);
	}

	@Override
	public void addDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		if(relay == null)
			throw new IllegalArgumentException("Cannot add null downstream relay.");
		CopyOnWriteArrayList<Consumer<OOCStreamMessage>> relays = _downstreamRelays;
		if(relays == null) {
			synchronized(this) {
				if(_downstreamRelays == null)
					_downstreamRelays = new CopyOnWriteArrayList<>();
				relays = _downstreamRelays;
			}
		}
		relays.add(0, relay);
	}

	@Override
	public void clearUpstreamMessageRelays() {
		_source.clearUpstreamMessageRelays();
	}

	@Override
	public void clearDownstreamMessageRelays() {
		_downstreamRelays = null;
	}

	@Override
	public void setIXTransform(BiFunction<Boolean, IndexRange, IndexRange> transform) {
		_source.setIXTransform(transform);
	}

	@Override
	public BiFunction<Boolean, IndexRange, IndexRange> getIXTransform() {
		return _source.getIXTransform();
	}

	private static final class LiveReader {
		private final SubscribableTaskQueue<IndexedMatrixValue> _stream;
		private final ArrayList<OOCStream.QueueCallback<IndexedMatrixValue>> _bufferedLive;
		private boolean _replayDone;
		private boolean _sourceComplete;
		private boolean _failed;

		private LiveReader(SubscribableTaskQueue<IndexedMatrixValue> stream) {
			_stream = stream;
			_bufferedLive = new ArrayList<>();
		}

		private void enqueuePrefix(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
			_stream.enqueue(callback);
		}

		private synchronized void enqueueLive(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
			if(_failed) {
				callback.close();
				return;
			}
			if(!_replayDone) {
				_bufferedLive.add(callback);
				return;
			}
			_stream.enqueue(callback);
		}

		private synchronized void finishReplay() {
			if(_failed)
				return;
			_replayDone = true;
			for(OOCStream.QueueCallback<IndexedMatrixValue> callback : _bufferedLive)
				_stream.enqueue(callback);
			_bufferedLive.clear();
			if(_sourceComplete)
				_stream.closeInput();
		}

		private synchronized void closeWhenReplayDone() {
			if(_failed)
				return;
			if(_replayDone)
				_stream.closeInput();
			else
				_sourceComplete = true;
		}

		private synchronized void fail(DMLRuntimeException failure) {
			if(_failed)
				return;
			_failed = true;
			for(OOCStream.QueueCallback<IndexedMatrixValue> callback : _bufferedLive)
				callback.close();
			_bufferedLive.clear();
			_stream.propagateFailure(failure);
		}
	}

	private static final class MaterializedStoreBoundaryPrimitive extends OOCPrimitive {
		private final MaterializedStoreStreamable _owner;
		private final OOCPrimitive _sourcePrimitive;

		private MaterializedStoreBoundaryPrimitive(MaterializedStoreStreamable owner, OOCPrimitive sourcePrimitive) {
			super(sourcePrimitive == null ? List.of() : List.of(sourcePrimitive), List.of(), List.of(owner));
			_owner = owner;
			_sourcePrimitive = sourcePrimitive;
		}

		@Override
		public boolean isMaterializationBoundary() {
			return true;
		}

		@Override
		public void startExecution() {
			if(_sourcePrimitive != null && _sourcePrimitive != this)
				_sourcePrimitive.tryStartExecution();
			onComplete();
		}

		@Override
		public void inferPatterns() {
			if(_pattern.isUnset())
				_pattern = OOCAccessPattern.ROW_MAJOR;
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
			_pattern = _pattern.isUnset() ? accessPattern : _pattern.preferred(accessPattern);
			getChildren().forEach(child -> {
				if(!child.hasStartedExecution())
					child.requestPattern(_pattern);
			});
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "@" + System.identityHashCode(this)
				+ "[store=" + System.identityHashCode(_owner) + "]";
		}
	}
}
