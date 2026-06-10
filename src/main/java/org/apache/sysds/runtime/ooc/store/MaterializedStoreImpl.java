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
import java.util.concurrent.LinkedBlockingQueue;
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
	private final ArrayList<RegisteredReader> registeredReaders;

	private volatile List<RegisteredReader> readers;
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
	public synchronized PackReader<T> openOpportunisticReader(AccessPattern pattern, MemoryAllowance allowance,
		int maxPrefetch) {
		if(!complete || closed)
			throw new IllegalStateException("Readers require a completed store");
		if(readersSealed)
			throw new IllegalStateException("Reader set is already sealed");
		if(!(cache instanceof OOCPackedCache packed))
			throw new IllegalStateException("Opportunistic pack reading requires OOCPackedCache");
		OpportunisticPackReader reader =
			new OpportunisticPackReader(packed, pattern, allowance, Math.max(1, maxPrefetch));
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
		List<RegisteredReader> localReaders = readers;
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
		List<RegisteredReader> localReaders = readers;
		for(int i = 0; i < localReaders.size(); i++) {
			RegisteredReader reader = localReaders.get(i);
			if(!reader.isClosed() && reader.pattern().needs(index))
				return;
		}
		if(forgotten.set(index))
			cache.dereference(new BlockKey(streamId, index));
	}

	private interface RegisteredReader extends AutoCloseable {
		AccessPattern pattern();

		boolean isClosed();

		@Override
		void close();
	}

	private final class StoreReader implements Reader<T>, RegisteredReader {
		private final AccessPattern pattern;
		private final MemoryAllowance allowance;
		private final int maxPrefetch;
		private final ArrayDeque<Request> requests;
		private volatile boolean closed;

		private StoreReader(AccessPattern pattern, MemoryAllowance allowance, int maxPrefetch) {
			this.pattern = pattern;
			this.allowance = allowance;
			this.maxPrefetch = maxPrefetch;
			requests = new ArrayDeque<>(maxPrefetch);
		}

		@Override
		public AccessPattern pattern() {
			return pattern;
		}

		@Override
		public boolean isClosed() {
			return closed;
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
			return new LeaseAlias(request.index, entry);
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
			while(requests.size() < maxPrefetch && pattern.hasNext()) {
				int index = pattern.next();
				OOCFuture<BlockEntry> future = cache.pin(streamId, index, allowance);
				requests.addLast(new Request(index, future));
			}
		}

		private BlockEntry awaitEntry(Request request) throws InterruptedException {
			OOCFuture<BlockEntry> future = request.future;
			while(true) {
				try {
					BlockEntry entry = future.get();
					if(entry != null)
						return entry;
					Thread.sleep(1);
					future = cache.pin(streamId, request.index, allowance);
					request.future = future;
				}
				catch(ExecutionException e) {
					throw DMLRuntimeException.of(e.getCause());
				}
			}
		}

		private void release(int index, BlockEntry entry) {
			cache.unpin(entry, allowance);
			pattern.consumed(index);
			tryForget(index);
		}

		private final class SharedLeaseState {
			private final int index;
			private final BlockEntry entry;
			private final AtomicInteger references;

			private SharedLeaseState(int index, BlockEntry entry) {
				this.index = index;
				this.entry = entry;
				references = new AtomicInteger(2);
			}

			private void retain() {
				references.incrementAndGet();
			}

			private void release() {
				if(references.decrementAndGet() != 0)
					return;
				StoreReader.this.release(index, entry);
			}
		}

		private final class LeaseAlias implements Lease<T> {
			private final int index;
			private final BlockEntry entry;
			private boolean open;
			private SharedLeaseState shared;

			private LeaseAlias(int index, BlockEntry entry) {
				this.index = index;
				this.entry = entry;
				open = true;
			}

			private LeaseAlias(SharedLeaseState shared) {
				index = shared.index;
				entry = shared.entry;
				this.shared = shared;
				open = true;
			}

			@Override
			public int index() {
				return index;
			}

			@SuppressWarnings("unchecked")
			@Override
			public T value() {
				if(!open)
					throw new IllegalStateException("Lease is closed");
				return (T)entry.getData();
			}

			@Override
			public Lease<T> retain() {
				if(!open)
					throw new IllegalStateException("Lease is closed");
				if(shared == null)
					shared = new SharedLeaseState(index, entry);
				else
					shared.retain();
				return new LeaseAlias(shared);
			}

			@Override
			public void close() {
				if(!open)
					return;
				open = false;
				if(shared == null)
					StoreReader.this.release(index, entry);
				else
					shared.release();
			}
		}

		private static final class Request {
			private final int index;
			private OOCFuture<BlockEntry> future;

			private Request(int index, OOCFuture<BlockEntry> future) {
				this.index = index;
				this.future = future;
			}
		}
	}

	private final class OpportunisticPackReader implements PackReader<T>, RegisteredReader {
		private final OOCPackedCache packed;
		private final AccessPattern pattern;
		private final MemoryAllowance allowance;
		private final int maxPrefetch;
		private final ConcurrentBitSet claimedGroups;
		private final ConcurrentBitSet consumedIndices;
		private final LinkedBlockingQueue<CompletedPack> ready;
		private final ArrayDeque<OOCPackedCache.PackGroup> blocked;

		private volatile boolean closed;
		private int inFlight;

		private OpportunisticPackReader(OOCPackedCache packed, AccessPattern pattern, MemoryAllowance allowance,
			int maxPrefetch) {
			this.packed = packed;
			this.pattern = pattern;
			this.allowance = allowance;
			this.maxPrefetch = maxPrefetch;
			claimedGroups = new ConcurrentBitSet(Math.max(1, packed.getPackGroupCount()));
			consumedIndices = new ConcurrentBitSet(Math.max(1, published));
			ready = new LinkedBlockingQueue<>();
			blocked = new ArrayDeque<>();
		}

		@Override
		public AccessPattern pattern() {
			return pattern;
		}

		@Override
		public boolean isClosed() {
			return closed;
		}

		@Override
		public boolean hasNext() {
			checkReady();
			fill();
			synchronized(this) {
				return !ready.isEmpty() || inFlight > 0 || !blocked.isEmpty() || pattern.hasNext();
			}
		}

		@Override
		public PackLease<T> nextPack() throws InterruptedException {
			checkReady();
			while(true) {
				fill();
				CompletedPack completion;
				synchronized(this) {
					if(ready.isEmpty() && inFlight == 0 && blocked.isEmpty() && !pattern.hasNext())
						throw new IllegalStateException("No remaining pack");
				}
				completion = ready.take();
				if(completion.error != null)
					throw DMLRuntimeException.of(completion.error);
				if(completion.lease != null) {
					fill();
					return completion.lease;
				}
			}
		}

		@Override
		public void close() {
			if(closed)
				return;
			closed = true;
			CompletedPack completion;
			while((completion = ready.poll()) != null)
				if(completion.lease != null)
					completion.lease.close();
			synchronized(this) {
				blocked.clear();
			}
		}

		private void checkReady() {
			if(closed)
				throw new IllegalStateException("Reader is closed");
			if(!readersSealed)
				throw new IllegalStateException("All readers must be registered and sealed before reading");
		}

		private void fill() {
			ArrayList<OOCPackedCache.PackGroup> groups = null;
			synchronized(this) {
				while(!closed && inFlight < maxPrefetch) {
					OOCPackedCache.PackGroup group = blocked.pollFirst();
					if(group == null)
						group = nextUnclaimedGroup();
					if(group == null)
						break;
					if(groups == null)
						groups = new ArrayList<>();
					groups.add(group);
					inFlight++;
				}
			}
			if(groups != null)
				for(int i = 0; i < groups.size(); i++)
					request(groups.get(i));
		}

		private OOCPackedCache.PackGroup nextUnclaimedGroup() {
			while(pattern.hasNext()) {
				int index = pattern.next();
				if(index < 0 || index >= published)
					throw new IndexOutOfBoundsException("Invalid requested index: " + index);
				if(consumedIndices.get(index))
					continue;
				OOCPackedCache.PackGroup group = packed.getPackGroup(streamId, index);
				if(group == null)
					throw new IllegalStateException("Index is not represented by a packed cache entry: " + index);
				if(claimedGroups.set(group.id()))
					return group;
			}
			return null;
		}

		private void request(OOCPackedCache.PackGroup group) {
			packed.pinPack(group, allowance).whenComplete((lease, error) -> complete(group, lease, error));
		}

		private void complete(OOCPackedCache.PackGroup group, OOCPackedCache.PackLease lease, Throwable error) {
			StorePackLease selected = null;
			if(lease != null && error == null)
				selected = select(lease);
			else if(lease != null)
				lease.close();
			synchronized(this) {
				inFlight--;
				if(closed) {
					if(lease != null && error == null)
						lease.close();
					return;
				}
				if(error != null)
					ready.offer(new CompletedPack(null, error));
				else if(lease == null)
					blocked.addLast(group);
				else if(selected != null)
					ready.offer(new CompletedPack(selected, null));
				else
					ready.offer(new CompletedPack(null, null));
			}
		}

		private StorePackLease select(OOCPackedCache.PackLease lease) {
			int[] slots = new int[lease.size()];
			int selected = 0;
			for(int slot = 0; slot < lease.size(); slot++) {
				int index = lease.index(slot);
				if(index >= 0 && index < published && !consumedIndices.get(index) && pattern.needs(index))
					slots[selected++] = slot;
			}
			if(selected == 0) {
				lease.close();
				return null;
			}
			if(selected != slots.length) {
				int[] compact = new int[selected];
				System.arraycopy(slots, 0, compact, 0, selected);
				slots = compact;
			}
			return new StorePackLease(lease, slots);
		}

		private final class StorePackLease implements PackLease<T> {
			private final OOCPackedCache.PackLease physical;
			private final int[] slots;
			private boolean open;

			private StorePackLease(OOCPackedCache.PackLease physical, int[] slots) {
				this.physical = physical;
				this.slots = slots;
				open = true;
			}

			@Override
			public int size() {
				return slots.length;
			}

			@Override
			public int index(int slot) {
				return physical.index(slots[slot]);
			}

			@SuppressWarnings("unchecked")
			@Override
			public T value(int slot) {
				if(!open)
					throw new IllegalStateException("Pack lease is closed");
				return (T)physical.value(slots[slot]);
			}

			@Override
			public void close() {
				if(!open)
					return;
				open = false;
				for(int slot : slots) {
					int index = physical.index(slot);
					if(consumedIndices.set(index)) {
						pattern.consumed(index);
						tryForget(index);
					}
				}
				physical.close();
				fill();
			}
		}

		private final class CompletedPack {
			private final StorePackLease lease;
			private final Throwable error;

			private CompletedPack(StorePackLease lease, Throwable error) {
				this.lease = lease;
				this.error = error;
			}
		}
	}
}
