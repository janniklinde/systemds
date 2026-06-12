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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.message.OOCStreamMessage;
import org.apache.sysds.runtime.util.IndexRange;

/**
 * Read-only {@code OOCStream} compatibility view over a completed {@link MaterializedStore}: the
 * offline replay half of the {@code CachingStream} replacement. Wraps an ordered
 * {@link MaterializedStore.Reader} (or an opportunistic {@link MaterializedStore.PackReader}, whose
 * packs are flattened into per-tile callbacks) and adapts leases to queue callbacks: {@code get()}
 * reads the leased value, {@code keepOpen()} retains the lease, {@code close()} closes it. EOS is
 * delivered after exhaustion; a failure closes the reader before it propagates.
 *
 * The reader must be opened on a completed store and the store's readers sealed before consumption —
 * live consumption and offline replay are never mixed in one structure.
 */
public final class StoreBackedStream implements OOCStream<IndexedMatrixValue> {
	private final TileSource _source;
	private final AtomicBoolean _subscriberSet;
	private volatile CopyOnWriteArrayList<Consumer<OOCStreamMessage>> _downstreamRelays;
	private OOCStream.QueueCallback<IndexedMatrixValue> _lastDequeue;
	private DMLRuntimeException _failure;
	private boolean _exhausted;
	private CacheableData<?> _data;
	private DataCharacteristics _dataCharacteristics;

	public StoreBackedStream(MaterializedStore.Reader<IndexedMatrixValue> reader) {
		_source = new OrderedTileSource(reader);
		_subscriberSet = new AtomicBoolean(false);
	}

	public StoreBackedStream(MaterializedStore.PackReader<IndexedMatrixValue> reader) {
		_source = new PackTileSource(reader);
		_subscriberSet = new AtomicBoolean(false);
	}

	@Override
	public void enqueue(IndexedMatrixValue t) {
		throw new DMLRuntimeException("Cannot enqueue to a store-backed stream");
	}

	@Override
	public void enqueue(QueueCallback<IndexedMatrixValue> callback) {
		throw new DMLRuntimeException("Cannot enqueue to a store-backed stream");
	}

	@Override
	public void closeInput() {
		throw new DMLRuntimeException("Cannot close the input of a store-backed stream");
	}

	@Override
	public synchronized IndexedMatrixValue dequeue() {
		return dequeueInternal().get();
	}

	@Override
	public synchronized QueueCallback<IndexedMatrixValue> dequeueCB() {
		return dequeueInternal();
	}

	private QueueCallback<IndexedMatrixValue> dequeueInternal() {
		if(_subscriberSet.get())
			throw new IllegalStateException("Cannot dequeue from a store-backed stream if a subscriber has been set");
		if(_lastDequeue != null) {
			_lastDequeue.close();
			_lastDequeue = null;
		}
		if(_failure != null)
			return OOCStream.eos(_failure);
		if(_exhausted)
			return OOCStream.eos(null);
		try {
			QueueCallback<IndexedMatrixValue> next = _source.nextCallback();
			if(next == null) {
				_exhausted = true;
				_source.close();
				return OOCStream.eos(null);
			}
			_lastDequeue = next;
			return next;
		}
		catch(Throwable t) {
			//failure closes the reader before propagation
			_failure = DMLRuntimeException.of(t);
			_source.close();
			throw _failure;
		}
	}

	@Override
	public void setSubscriber(Consumer<QueueCallback<IndexedMatrixValue>> subscriber) {
		if(!_subscriberSet.compareAndSet(false, true))
			throw new IllegalArgumentException("Subscriber cannot be set multiple times");
		Thread driver = new Thread(() -> drive(subscriber), "ooc-store-replay");
		driver.setDaemon(true);
		driver.start();
	}

	private void drive(Consumer<QueueCallback<IndexedMatrixValue>> subscriber) {
		try {
			QueueCallback<IndexedMatrixValue> next;
			while((next = _source.nextCallback()) != null) {
				try(QueueCallback<IndexedMatrixValue> cb = next) {
					subscriber.accept(cb);
				}
			}
			_source.close();
			subscriber.accept(OOCStream.eos(null));
		}
		catch(Throwable t) {
			//failure closes the reader before propagation
			_source.close();
			subscriber.accept(OOCStream.eos(DMLRuntimeException.of(t)));
		}
	}

	@Override
	public synchronized void propagateFailure(DMLRuntimeException re) {
		if(_failure == null)
			_failure = re;
		_source.close();
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReadStream() {
		return this;
	}

	@Override
	public OOCStream<IndexedMatrixValue> getWriteStream() {
		throw new UnsupportedOperationException("A store-backed stream has no write stream");
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
	public OOCPrimitive getPrimitive() {
		return null;
	}

	@Override
	public boolean isProcessed() {
		return false;
	}

	@Override
	public DataCharacteristics getDataCharacteristics() {
		return _dataCharacteristics;
	}

	public void setDataCharacteristics(DataCharacteristics dataCharacteristics) {
		_dataCharacteristics = dataCharacteristics;
	}

	@Override
	public CacheableData<?> getData() {
		return _data;
	}

	@Override
	public void setData(CacheableData<?> data) {
		_data = data;
	}

	@Override
	public void messageUpstream(OOCStreamMessage msg) {
		//the producing pipeline is gone; a completed store has no upstream
	}

	@Override
	public void messageDownstream(OOCStreamMessage msg) {
		CopyOnWriteArrayList<Consumer<OOCStreamMessage>> relays = _downstreamRelays;
		if(relays != null) {
			for(Consumer<OOCStreamMessage> relay : relays) {
				if(msg.isCancelled())
					break;
				relay.accept(msg);
			}
		}
	}

	@Override
	public void setUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		addDownstreamMessageRelay(relay);
	}

	@Override
	public void addUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		if(relay == null)
			throw new IllegalArgumentException("Cannot set downstream relay to null");
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
		//no upstream relays supported
	}

	@Override
	public void clearDownstreamMessageRelays() {
		_downstreamRelays = null;
	}

	@Override
	public void setIXTransform(BiFunction<Boolean, IndexRange, IndexRange> transform) {
		throw new UnsupportedOperationException();
	}

	private interface TileSource {
		/**
		 * Returns the next tile callback, or null after exhaustion.
		 */
		QueueCallback<IndexedMatrixValue> nextCallback() throws InterruptedException;

		void close();
	}

	private static final class OrderedTileSource implements TileSource {
		private final MaterializedStore.Reader<IndexedMatrixValue> reader;

		private OrderedTileSource(MaterializedStore.Reader<IndexedMatrixValue> reader) {
			this.reader = reader;
		}

		@Override
		public QueueCallback<IndexedMatrixValue> nextCallback() throws InterruptedException {
			if(!reader.hasNext())
				return null;
			return new LeaseCallback(reader.next());
		}

		@Override
		public void close() {
			reader.close();
		}
	}

	private static final class PackTileSource implements TileSource {
		private final MaterializedStore.PackReader<IndexedMatrixValue> reader;
		private SharedPack shared;
		private int slot;

		private PackTileSource(MaterializedStore.PackReader<IndexedMatrixValue> reader) {
			this.reader = reader;
		}

		@Override
		public QueueCallback<IndexedMatrixValue> nextCallback() throws InterruptedException {
			while(shared == null || slot >= shared.pack.size()) {
				if(shared != null) {
					shared.release();
					shared = null;
				}
				if(!reader.hasNext())
					return null;
				shared = new SharedPack(reader.nextPack());
				slot = 0;
			}
			shared.retain();
			return new PackSlotCallback(shared, slot++);
		}

		@Override
		public void close() {
			if(shared != null) {
				shared.release();
				shared = null;
			}
			reader.close();
		}
	}

	/**
	 * Keeps a physical pack lease open until the source stopped iterating it AND every flattened
	 * per-slot callback (including keepOpen aliases) closed. Consumption is recorded at pack
	 * granularity when the pack lease finally closes.
	 */
	private static final class SharedPack {
		private final MaterializedStore.PackLease<IndexedMatrixValue> pack;
		private final AtomicInteger references;

		private SharedPack(MaterializedStore.PackLease<IndexedMatrixValue> pack) {
			this.pack = pack;
			references = new AtomicInteger(1);
		}

		private void retain() {
			if(references.getAndIncrement() <= 0)
				throw new IllegalStateException("Pack lease is already fully closed");
		}

		private void release() {
			if(references.decrementAndGet() == 0)
				pack.close();
		}
	}

	/**
	 * Queue-callback view of a store lease: {@code get()} reads the leased value, {@code keepOpen()}
	 * retains the lease, {@code close()} closes it (consumption + forgetting are driven by the lease).
	 */
	public static final class LeaseCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final MaterializedStore.Lease<IndexedMatrixValue> _lease;
		private DMLRuntimeException _failure;
		private boolean _closed;

		private LeaseCallback(MaterializedStore.Lease<IndexedMatrixValue> lease) {
			_lease = lease;
		}

		@Override
		public IndexedMatrixValue get() {
			if(_failure != null)
				throw _failure;
			return _lease.value();
		}

		@Override
		public synchronized QueueCallback<IndexedMatrixValue> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed callback");
			return new LeaseCallback(_lease.retain());
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

	private static final class PackSlotCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final SharedPack _shared;
		private final int _slot;
		private DMLRuntimeException _failure;
		private boolean _closed;

		private PackSlotCallback(SharedPack shared, int slot) {
			_shared = shared;
			_slot = slot;
		}

		@Override
		public IndexedMatrixValue get() {
			if(_failure != null)
				throw _failure;
			return _shared.pack.value(_slot);
		}

		@Override
		public synchronized QueueCallback<IndexedMatrixValue> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed callback");
			_shared.retain();
			return new PackSlotCallback(_shared, _slot);
		}

		@Override
		public synchronized void close() {
			if(_closed)
				return;
			_closed = true;
			_shared.release();
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
}
