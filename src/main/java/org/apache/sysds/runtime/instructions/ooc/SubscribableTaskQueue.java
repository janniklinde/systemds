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

package org.apache.sysds.runtime.instructions.ooc;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.parfor.LocalTaskQueue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class SubscribableTaskQueue<T> extends LocalTaskQueue<OOCStream.QueueCallback<T>> implements OOCStream<T> {

	/** Queues that have buffered at least once, i.e. the candidates of a purge run. */
	private static final ConcurrentLinkedQueue<WeakReference<SubscribableTaskQueue<?>>> BUFFERING =
		new ConcurrentLinkedQueue<>();
	private static final int PURGE_REGISTRY_PRUNE_INTERVAL = 4096;
	private static final AtomicLong REGISTERED = new AtomicLong(0);
	private static final AtomicLong PARKED_BLOCKS = new AtomicLong(0);
	private static final AtomicLong PARKED_BYTES = new AtomicLong(0);

	private final AtomicInteger _availableCtr = new AtomicInteger(1);
	private final AtomicBoolean _closed = new AtomicBoolean(false);
	private final AtomicBoolean _terminalDelivered = new AtomicBoolean(false);
	private final AtomicInteger _blockCount = new AtomicInteger(0);
	private QueueCallback<T> _lastDequeued = null;
	private CacheableData<?> _cdata;
	private volatile OOCPrimitive _primitive;
	private volatile Consumer<QueueCallback<T>> _subscriber = null;
	private boolean _registeredForPurge = false;
	private String _watchdogId;

	public SubscribableTaskQueue() {
		if(OOCWatchdog.WATCH) {
			_watchdogId = "STQ-" + hashCode();
			// Capture a short context to help identify origin
			OOCWatchdog.registerOpen(_watchdogId, "SubscribableTaskQueue@" + hashCode(), getCtxMsg(), this);
		}
	}

	private String getCtxMsg() {
		StackTraceElement[] st = new Exception().getStackTrace();
		// Skip the first few frames (constructor, createWritableStream, etc.)
		StringBuilder sb = new StringBuilder();
		int limit = Math.min(st.length, 7);
		for(int i = 2; i < limit; i++) {
			sb.append(st[i].getClassName()).append(".").append(st[i].getMethodName()).append(":")
				.append(st[i].getLineNumber());
			if(i < limit - 1)
				sb.append(" <- ");
		}
		return sb.toString();
	}

	@Override
	public void enqueue(T t) {
		enqueue(new SimpleQueueCallback<>(t, _failure));
	}

	@Override
	public void enqueue(QueueCallback<T> cb) {
		if(cb == NO_MORE_TASKS)
			throw new DMLRuntimeException("Cannot enqueue NO_MORE_TASKS item");
		int cnt = _availableCtr.incrementAndGet();

		if(cnt <= 1) { // Then the queue was already closed and we disallow further enqueues
			_availableCtr.decrementAndGet(); // Undo increment
			throw new DMLRuntimeException("Cannot enqueue into closed SubscribableTaskQueue");
		}

		int blocks = cb instanceof GroupQueueCallback<?> group ? group.size() : 1;
		_blockCount.addAndGet(blocks);

		Consumer<QueueCallback<T>> s = _subscriber;
		final Consumer<QueueCallback<T>> fS = s;

		if(fS != null) {
			fS.accept(cb);
			onDeliveryFinished();
			return;
		}

		synchronized(this) {
			// Re-check that subscriber is really null to avoid race conditions
			if(_subscriber == null) {
				registerForPurge();
				try {
					super.enqueueTask(cb);
				}
				catch(InterruptedException e) {
					throw new DMLRuntimeException(e);
				}
				return;
			}
			// Otherwise do not insert and re-schedule subscriber invocation
			s = _subscriber;
		}

		// Last case if due to race a subscriber has been set
		s.accept(cb);
		onDeliveryFinished();
	}

	protected boolean tryDeliverCallback(QueueCallback<T> cb, int blockCount) {
		Consumer<QueueCallback<T>> s = _subscriber;
		if(s == null)
			return false;
		int cnt = _availableCtr.incrementAndGet();
		if(cnt <= 1) { // Then the queue was already closed and we disallow further enqueues
			_availableCtr.decrementAndGet(); // Undo increment
			throw new DMLRuntimeException("Cannot enqueue into closed SubscribableTaskQueue");
		}
		_blockCount.addAndGet(blockCount);
		s.accept(cb);
		onDeliveryFinished();
		return true;
	}

	@Override
	public synchronized void enqueueTask(OOCStream.QueueCallback<T> t) {
		enqueue(t);
	}

	@Override
	public T dequeue() {
		try {
			if(OOCWatchdog.WATCH)
				OOCWatchdog.addEvent(_watchdogId, "dequeue -- " + getCtxMsg());
			if(_lastDequeued != null) {
				_lastDequeued.close();
				_lastDequeued = null;
			}
			OOCStream.QueueCallback<T> deq = super.dequeueTask();
			if(deq != NO_MORE_TASKS) {
				onDeliveryFinished();
				_lastDequeued = deq;
				return deq.get();
			}
			_terminalDelivered.set(true);
			return null;
		}
		catch(InterruptedException e) {
			throw new DMLRuntimeException(e);
		}
	}

	@Override
	public OOCStream.QueueCallback<T> dequeueCB() {
		try {
			if(OOCWatchdog.WATCH)
				OOCWatchdog.addEvent(_watchdogId, "dequeue -- " + getCtxMsg());
			if(_lastDequeued != null) {
				_lastDequeued.close();
				_lastDequeued = null;
			}
			OOCStream.QueueCallback<T> deq = super.dequeueTask();
			if(deq != NO_MORE_TASKS) {
				onDeliveryFinished();
				_lastDequeued = deq;
			}
			else
				_terminalDelivered.set(true);
			return deq == NO_MORE_TASKS ? null : deq;
		}
		catch(InterruptedException e) {
			throw new DMLRuntimeException(e);
		}
	}

	@Override
	public synchronized OOCStream.QueueCallback<T> dequeueTask() {
		return dequeueCB();
	}

	@Override
	public synchronized void closeInput() {
		if(_closed.compareAndSet(false, true)) {
			super.closeInput();
			onDeliveryFinished();
		}
		else {
			throw new IllegalStateException("Multiple close input calls");
		}
	}

	private void validateBlockCountOnClose() {
		DataCharacteristics dc = getDataCharacteristics();
		if(dc != null && dc.dimsKnown() && dc.getBlocksize() > 0) {
			long expected = OOCUtils.getNumBlocks(dc);
			if(expected >= 0 && _blockCount.get() != expected) {
				throw new DMLRuntimeException("OOCStream block count mismatch: expected " + expected + " but saw "
					+ _blockCount.get() + " (" + dc.getRows() + "x" + dc.getCols() + ")");
			}
		}
	}

	@Override
	public void setSubscriber(Consumer<QueueCallback<T>> subscriber) {
		if(subscriber == null)
			throw new IllegalArgumentException("Cannot set subscriber to null");

		LinkedList<QueueCallback<T>> data;
		boolean needsEos;

		synchronized(this) {
			if(_subscriber != null)
				throw new DMLRuntimeException("Cannot set multiple subscribers");
			_subscriber = subscriber;
			if(_failure != null)
				throw _failure;
			data = _data;
			_data = new LinkedList<>();
			// If this stream was already closed with no buffered data, no further
			// onDeliveryFinished() call will happen, so emit EOS immediately.
			needsEos = _closed.get() && data.isEmpty() && _availableCtr.get() == 0;
			if(needsEos)
				_availableCtr.incrementAndGet(); // route terminal emission via onDeliveryFinished
		}

		for(QueueCallback<T> t : data) {
			subscriber.accept(t);
			onDeliveryFinished();
		}

		if(needsEos)
			onDeliveryFinished();
	}

	private void registerForPurge() {
		if(_registeredForPurge)
			return;
		_registeredForPurge = true;
		BUFFERING.add(new WeakReference<>(this));
		//the registry is only pruned by purge runs, which may never happen; keep it bounded by live queues
		if(REGISTERED.incrementAndGet() % PURGE_REGISTRY_PRUNE_INTERVAL == 0)
			pruneRegistry();
	}

	private static void pruneRegistry() {
		for(Iterator<WeakReference<SubscribableTaskQueue<?>>> it = BUFFERING.iterator(); it.hasNext();)
			if(it.next().get() == null)
				it.remove();
	}

	/**
	 * Last resort against a hard stall in the {@code GlobalMemoryBroker}: force-parks the payloads of all buffered
	 * in-memory callbacks into the cache, handing their bytes back to the broker. A buffered callback has by
	 * definition not been handed to a consumer yet - the queue monitor held here is what guarantees that, since
	 * dequeuing is synchronized on the same monitor. Consumers see the payload again through
	 * {@code QueueCallback.get()}, which revives it from the cache.
	 *
	 * @return the number of bytes released back to the broker
	 */
	public static long purgeBuffered() {
		if(BUFFERING.isEmpty())
			return 0;
		OOCCache cache = OOCCacheManager.getGlobalCache();
		long freed = 0;
		for(Iterator<WeakReference<SubscribableTaskQueue<?>>> it = BUFFERING.iterator(); it.hasNext();) {
			SubscribableTaskQueue<?> queue = it.next().get();
			if(queue == null)
				it.remove();
			else
				freed += queue.parkBuffered(cache);
		}
		return freed;
	}

	private long parkBuffered(OOCCache cache) {
		long freed = 0;
		long blocks = 0;
		synchronized(this) {
			//iterate a snapshot: parking releases memory, which may re-enter this queue on the same thread
			Object[] buffered = _data.toArray();
			for(Object cb : buffered) {
				if(!(cb instanceof InMemoryQueueCallback<?> managed))
					continue;
				long bytes = managed.tryPark(cache);
				if(bytes > 0) {
					freed += bytes;
					blocks++;
				}
			}
		}
		if(blocks > 0) {
			PARKED_BLOCKS.addAndGet(blocks);
			PARKED_BYTES.addAndGet(freed);
		}
		return freed;
	}

	public static String describePurgeState() {
		return "parkValve[blocks=" + PARKED_BLOCKS.get() + " bytes=" + PARKED_BYTES.get() + " queues="
			+ BUFFERING.size() + "]";
	}

	private void onDeliveryFinished() {
		int ctr = _availableCtr.decrementAndGet();

		if(ctr == 0) {
			validateBlockCountOnClose();
			Consumer<QueueCallback<T>> s = _subscriber;
			if(s != null) {
				s.accept(OOCStream.eos(_failure));
				_terminalDelivered.set(true);
			}

			if(OOCWatchdog.WATCH)
				OOCWatchdog.registerClose(_watchdogId);
		}
	}

	@Override
	public synchronized void propagateFailure(DMLRuntimeException re) {
		// Ignore late failures
		if(_terminalDelivered.get())
			return;
		super.propagateFailure(re);
		Consumer<QueueCallback<T>> s = _subscriber;
		if(s != null) {
			s.accept(new SimpleQueueCallback<>(null, re));
			_terminalDelivered.set(true);
		}
	}

	@Override
	public OOCStream<T> getReadStream() {
		return this;
	}

	@Override
	public OOCStream<T> getWriteStream() {
		return this;
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
		return _primitive;
	}

	@Override
	public void assignPrimitive(OOCPrimitive primitive) {
		if(_primitive != null)
			throw new IllegalStateException("Primitive already assigned");
		_primitive = primitive;
	}

	@Override
	public DataCharacteristics getDataCharacteristics() {
		return _cdata == null ? null : _cdata.getDataCharacteristics();
	}

	@Override
	public CacheableData<?> getData() {
		return _cdata;
	}

	@Override
	public void setData(CacheableData<?> data) {
		if(_cdata == null && _closed.get())
			System.out.println("[WARN] Data type was defined after closing, which may bypass validation checks");
		_cdata = data;
	}

	@Override
	public synchronized String toString() {
		return "STQ-" + hashCode();
	}

	@Override
	public synchronized String debugState() {
		return "STQ@" + System.identityHashCode(this) + "[avail=" + _availableCtr.get() + ", blocks=" + _blockCount.get()
			+ ", buffered=" + _data.size() + ", closed=" + _closed.get() + ", eosDelivered=" + _terminalDelivered.get()
			+ ", subscriber=" + (_subscriber == null ? "none" : _subscriber.getClass().getName()) + "]";
	}
}
