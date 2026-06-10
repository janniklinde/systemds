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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.collections.ConcurrentBitSet;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

public final class MaterializedStoreImpl<T extends SpillableObject> implements MaterializedStore<T> {
	private final OOCCache cache;
	private final long streamId;
	private final ArrayList<StoreReader> registeredReaders;

	private volatile List<StoreReader> readers;
	private volatile ConcurrentBitSet forgotten;
	private volatile int published;
	private volatile boolean complete;
	private volatile boolean readersSealed;
	private volatile boolean closed;

	public MaterializedStoreImpl(OOCCache cache, long streamId) {
		this.cache = cache;
		this.streamId = streamId;
		registeredReaders = new ArrayList<>();
		readers = Collections.emptyList();
	}

	@Override
	public synchronized void publishPinned(int index, T value, long bytes, MemoryAllowance allowance) {
		if(complete || closed)
			throw new IllegalStateException("Store no longer accepts published items");
		if(index != published)
			throw new IllegalArgumentException("Expected sequential index " + published + " but received " + index);
		BlockEntry entry = cache.putPinned(streamId, index, value, bytes, allowance);
		cache.unpin(entry, allowance);
		published++;
	}

	@Override
	public synchronized void complete() {
		if(complete)
			return;
		if(cache instanceof OOCPackedCache packed)
			packed.flushPacks();
		forgotten = new ConcurrentBitSet(published);
		complete = true;
	}

	@Override
	public synchronized Reader<T> openReader(AccessPattern pattern, MemoryAllowance allowance, int maxPrefetch) {
		if(!complete || closed)
			throw new IllegalStateException("Readers require a completed store");
		if(readersSealed)
			throw new IllegalStateException("Reader set is already sealed");
		StoreReader reader = new StoreReader(pattern, allowance, Math.max(1, maxPrefetch));
		registeredReaders.add(reader);
		return reader;
	}

	@Override
	public synchronized void sealReaders() {
		if(!complete || closed)
			throw new IllegalStateException("Cannot seal readers for an incomplete store");
		if(readersSealed)
			return;
		readers = new ArrayList<>(registeredReaders);
		readersSealed = true;
	}

	@Override
	public int size() {
		return published;
	}

	@Override
	public void close() {
		if(closed)
			return;
		closed = true;
		List<StoreReader> localReaders = readers;
		for(int i = 0; i < localReaders.size(); i++)
			localReaders.get(i).close();
		ConcurrentBitSet localForgotten = forgotten;
		if(localForgotten != null) {
			for(int i = 0; i < published; i++)
				if(localForgotten.set(i))
					cache.dereference(new BlockKey(streamId, i));
		}
	}

	private void tryForget(int index) {
		List<StoreReader> localReaders = readers;
		for(int i = 0; i < localReaders.size(); i++) {
			StoreReader reader = localReaders.get(i);
			if(!reader.closed && reader.pattern.needs(index))
				return;
		}
		if(forgotten.set(index))
			cache.dereference(new BlockKey(streamId, index));
	}

	private final class StoreReader implements Reader<T> {
		private final AccessPattern pattern;
		private final MemoryAllowance allowance;
		private final int maxPrefetch;
		private final ArrayDeque<Request> requests;
		private volatile boolean closed;
		private boolean admissionBlocked;

		private StoreReader(AccessPattern pattern, MemoryAllowance allowance, int maxPrefetch) {
			this.pattern = pattern;
			this.allowance = allowance;
			this.maxPrefetch = maxPrefetch;
			requests = new ArrayDeque<>(maxPrefetch);
		}

		@Override
		public boolean hasNext() {
			checkReady();
			fill();
			return !requests.isEmpty();
		}

		@Override
		public Lease<T> next() throws InterruptedException {
			checkReady();
			fill();
			if(requests.isEmpty())
				throw new IllegalStateException("No remaining item");
			Request request = requests.peekFirst();
			BlockEntry entry = awaitEntry(request);
			requests.removeFirst();
			fill();
			return new LeaseAlias(new LeaseState(request.index, entry));
		}

		@Override
		public void close() {
			if(closed)
				return;
			closed = true;
			Request request;
			while((request = requests.pollFirst()) != null) {
				request.future.whenComplete((entry, error) -> {
					if(entry != null)
						cache.unpin(entry, allowance);
				});
			}
		}

		private void checkReady() {
			if(closed)
				throw new IllegalStateException("Reader is closed");
			if(!readersSealed)
				throw new IllegalStateException("All readers must be registered and sealed before reading");
		}

		private void fill() {
			if(admissionBlocked)
				return;
			while(requests.size() < maxPrefetch && pattern.hasNext()) {
				int index = pattern.next();
				OOCFuture<BlockEntry> future = cache.pin(streamId, index, allowance);
				Request request = new Request(index, future);
				requests.addLast(request);
				if(future.isDone() && future.getNow(null) == null) {
					admissionBlocked = true;
					request.blocksAdmission = true;
					return;
				}
			}
		}

		private BlockEntry awaitEntry(Request request) throws InterruptedException {
			OOCFuture<BlockEntry> future = request.future;
			while(true) {
				try {
					BlockEntry entry = future.get();
					if(entry != null) {
						if(request.blocksAdmission)
							admissionBlocked = false;
						return entry;
					}
					Thread.sleep(1);
					future = cache.pin(streamId, request.index, allowance);
					request.future = future;
				}
				catch(ExecutionException e) {
					throw DMLRuntimeException.of(e.getCause());
				}
			}
		}

		private final class LeaseState {
			private final int index;
			private final BlockEntry entry;
			private final AtomicInteger references;

			private LeaseState(int index, BlockEntry entry) {
				this.index = index;
				this.entry = entry;
				references = new AtomicInteger(1);
			}

			private void release() {
				if(references.decrementAndGet() != 0)
					return;
				cache.unpin(entry, allowance);
				pattern.consumed(index);
				tryForget(index);
			}
		}

		private final class LeaseAlias implements Lease<T> {
			private final LeaseState state;
			private final AtomicBoolean open;

			private LeaseAlias(LeaseState state) {
				this.state = state;
				open = new AtomicBoolean(true);
			}

			@Override
			public int index() {
				return state.index;
			}

			@SuppressWarnings("unchecked")
			@Override
			public T value() {
				if(!open.get())
					throw new IllegalStateException("Lease is closed");
				return (T)state.entry.getData();
			}

			@Override
			public Lease<T> retain() {
				if(!open.get())
					throw new IllegalStateException("Lease is closed");
				state.references.incrementAndGet();
				return new LeaseAlias(state);
			}

			@Override
			public void close() {
				if(open.compareAndSet(true, false))
					state.release();
			}
		}

		private static final class Request {
			private final int index;
			private OOCFuture<BlockEntry> future;
			private boolean blocksAdmission;

			private Request(int index, OOCFuture<BlockEntry> future) {
				this.index = index;
				this.future = future;
			}
		}
	}
}
