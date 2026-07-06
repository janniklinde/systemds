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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.collections.ConcurrentBitSet;
import org.apache.sysds.runtime.ooc.cache.collections.ConcurrentGrowableBitSet;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

public final class MaterializedStoreImpl<T extends SpillableObject> implements MaterializedStore<T> {
	private final OOCCache cache;
	private final long streamId;
	private final ArrayList<RegisteredReader> registeredReaders;
	private final ConcurrentGrowableBitSet forgotten;
	private final AtomicInteger published;
	private final AtomicInteger publishedCount;

	private volatile List<RegisteredReader> readers;
	private volatile int completedSize;
	private volatile boolean complete;
	private volatile boolean readersSealed;
	private volatile boolean closed;

	public MaterializedStoreImpl(OOCCache cache, long streamId) {
		this.cache = cache;
		this.streamId = streamId;
		registeredReaders = new ArrayList<>();
		forgotten = new ConcurrentGrowableBitSet();
		published = new AtomicInteger();
		publishedCount = new AtomicInteger();
		readers = Collections.emptyList();
	}

	@Override
	public void publishPinned(int index, T value, long bytes, MemoryAllowance allowance) {
		if(complete || closed)
			throw new IllegalStateException("Store no longer accepts published items");
		if(index < 0 || index == Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("Invalid index: " + index);
		BlockEntry entry = cache.putPinned(streamId, index, value, bytes, allowance);
		cache.unpin(entry, allowance);
		publishedCount.incrementAndGet();
		updatePublished(index + 1);
		tryForget(index);
	}

	@Override
	public LiveLease<T> publishPinnedLive(int index, T value, long bytes, MemoryAllowance allowance) {
		if(complete || closed)
			throw new IllegalStateException("Store no longer accepts published items");
		if(index < 0 || index == Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("Invalid index: " + index);
		BlockEntry entry = cache.putPinned(streamId, index, value, bytes, allowance);
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[OOC STORE TRACE] store publish live store=" + System.identityHashCode(this)
				+ " stream=" + streamId + " index=" + index + " bytes=" + bytes
				+ " allowance=" + System.identityHashCode(allowance)
				+ " entry=" + System.identityHashCode(entry));
		publishedCount.incrementAndGet();
		updatePublished(index + 1);
		return new LiveLeaseAlias(new LiveLeaseState(index, entry, allowance));
	}

	@Override
	public void publishPackPinned(int[] indices, T[] values, long[] bytes, int off, int len,
		MemoryAllowance allowance) {
		if(complete || closed)
			throw new IllegalStateException("Store no longer accepts published items");
		if(!(cache instanceof OOCPackedCache packed))
			throw new IllegalStateException("Packed publication requires OOCPackedCache");
		if(len == 0)
			return;
		int maxIndex = -1;
		for(int i = off; i < off + len; i++) {
			if(indices[i] < 0 || indices[i] == Integer.MAX_VALUE)
				throw new IndexOutOfBoundsException("Invalid index: " + indices[i]);
			maxIndex = Math.max(maxIndex, indices[i]);
		}
		BlockEntry physical = packed.putSealedPackPinned(streamId, Arrays.stream(indices).asLongStream().toArray(),
			values, bytes, off, len, allowance);
		cache.unpin(physical, allowance);
		publishedCount.addAndGet(len);
		updatePublished(maxIndex + 1);
		for(int i = off; i < off + len; i++)
			tryForget(indices[i]);
	}

	@Override
	public synchronized void complete() {
		if(complete)
			return;
		if(cache instanceof OOCPackedCache packed)
			packed.flushPacks();
		completedSize = published.get();
		//holes or duplicates would otherwise surface as unbounded pin retries on missing indices
		if(publishedCount.get() != completedSize)
			throw new IllegalStateException("Incomplete publication: " + publishedCount.get()
				+ " published items for logical range [0, " + completedSize + ")");
		complete = true;
	}

	@Override
	public synchronized Reader<T> openReader(AccessPattern pattern, MemoryAllowance allowance, int maxPrefetch) {
		if(!complete || closed)
			throw new IllegalStateException("Readers require a completed store");
		if(readersSealed)
			throw new IllegalStateException("Store no longer accepts new readers");
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
			throw new IllegalStateException("Store no longer accepts new readers");
		if(!(cache instanceof OOCPackedCache packed))
			throw new IllegalStateException("Opportunistic pack reading requires OOCPackedCache");
		OpportunisticPackReader reader =
			new OpportunisticPackReader(packed, pattern, allowance, Math.max(1, maxPrefetch));
		registeredReaders.add(reader);
		return reader;
	}

	@Override
	public synchronized IndexedReader<T> openIndexedReader(Liveness liveness, MemoryAllowance allowance) {
		if(!complete || closed)
			throw new IllegalStateException("Readers require a completed store");
		if(readersSealed)
			throw new IllegalStateException("Store no longer accepts new readers");
		IndexedStoreReader reader = new IndexedStoreReader(liveness, allowance);
		registeredReaders.add(reader);
		return reader;
	}

	@Override
	public OOCFuture<Lease<T>> requestPublished(int index, MemoryAllowance allowance) {
		if(closed)
			throw new IllegalStateException("Store is closed");
		if(index < 0 || index >= published.get())
			throw new IndexOutOfBoundsException("Invalid requested index: " + index);
		OOCFuture<BlockEntry> pinned = new OOCFuture<>();
		StorePinRetry.pinWithRetry(cache, streamId, index, allowance, () -> closed, pinned);
		OOCFuture<Lease<T>> result = new OOCFuture<>();
		pinned.whenComplete((entry, error) -> {
			if(error != null)
				result.completeExceptionally(error);
			else if(entry == null)
				result.complete(null);
			else
				result.complete(new LeaseAlias((idx, current) -> cache.unpin(current, allowance), index, entry));
		});
		return result;
	}

	@Override
	public synchronized void sealReaders() {
		if(closed)
			throw new IllegalStateException("Cannot seal readers for a closed store");
		if(readersSealed)
			return;
		readers = new ArrayList<>(registeredReaders);
		readersSealed = true;
		int publishedSize = complete ? completedSize : published.get();
		for(int i = 0; i < publishedSize; i++)
			tryForget(i);
	}

	@Override
	public int size() {
		return complete ? completedSize : published.get();
	}

	@Override
	public void close() {
		List<RegisteredReader> localReaders;
		synchronized(this) {
			if(closed)
				return;
			closed = true;
			localReaders = readersSealed ? readers : new ArrayList<>(registeredReaders);
		}
		for(int i = 0; i < localReaders.size(); i++)
			localReaders.get(i).close();
		for(int i = 0; i < size(); i++)
			if(forgotten.set(i))
				cache.dereference(new BlockKey(streamId, i));
	}

	private void tryForget(int index) {
		if(!readersSealed)
			return;
		List<RegisteredReader> localReaders = readers;
		for(int i = 0; i < localReaders.size(); i++) {
			RegisteredReader reader = localReaders.get(i);
			if(!reader.isClosed() && reader.liveness().needs(index))
				return;
		}
		if(forgotten.set(index))
			cache.dereference(new BlockKey(streamId, index));
	}

	/**
	 * Re-evaluates forgetting for the full logical range after a reader closed early; without this,
	 * entries only needed by the closed reader would stay referenced until store closure. Closing the
	 * store sweeps itself. Before sealing, rmvar has not closed the streamable yet, so future
	 * readers may still be added and no tile may be forgotten solely from current readers.
	 */
	private void forgetAfterReaderClose() {
		if(closed || !readersSealed)
			return;
		for(int i = 0; i < completedSize; i++)
			tryForget(i);
	}

	private void updatePublished(int size) {
		int current = published.get();
		while(current < size && !published.compareAndSet(current, size))
			current = published.get();
	}

	private interface RegisteredReader extends AutoCloseable {
		Liveness liveness();

		boolean isClosed();

		@Override
		void close();
	}

	private interface LeaseReleaser {
		void release(int index, BlockEntry entry);
	}

	private static final class SharedLeaseState {
		private final LeaseReleaser releaser;
		private final int index;
		private final BlockEntry entry;
		private final AtomicInteger references;

		private SharedLeaseState(LeaseReleaser releaser, int index, BlockEntry entry) {
			this.releaser = releaser;
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
			releaser.release(index, entry);
		}
	}

	private final class LeaseAlias implements Lease<T> {
		private final LeaseReleaser releaser;
		private final int index;
		private final BlockEntry entry;
		private boolean open;
		private SharedLeaseState shared;

		private LeaseAlias(LeaseReleaser releaser, int index, BlockEntry entry) {
			this.releaser = releaser;
			this.index = index;
			this.entry = entry;
			open = true;
		}

		private LeaseAlias(SharedLeaseState shared) {
			releaser = shared.releaser;
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
				shared = new SharedLeaseState(releaser, index, entry);
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
				releaser.release(index, entry);
			else
				shared.release();
		}
	}

	/**
	 * Shared pin lifetime of a live fan-out publication: aliases serve concurrent live consumers, the
	 * last alias close unpins (resident ownership to the cache, possibly deferred). No consumption or
	 * forgetting is involved — readers and reclamation start only after completion and sealing.
	 */
	private final class LiveLeaseState {
		private final int index;
		private final BlockEntry entry;
		private final MemoryAllowance allowance;
		private final AtomicInteger references;

		private LiveLeaseState(int index, BlockEntry entry, MemoryAllowance allowance) {
			this.index = index;
			this.entry = entry;
			this.allowance = allowance;
			references = new AtomicInteger(1);
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC STORE TRACE] live state create store=" + System.identityHashCode(MaterializedStoreImpl.this)
					+ " index=" + index + " refs=1 entry=" + System.identityHashCode(entry)
					+ " allowance=" + System.identityHashCode(allowance));
		}

		private void retain() {
			int before = references.getAndIncrement();
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC STORE TRACE] live retain store=" + System.identityHashCode(MaterializedStoreImpl.this)
					+ " index=" + index + " refs=" + before + "->" + (before + 1)
					+ " entry=" + System.identityHashCode(entry));
			if(before <= 0)
				throw new IllegalStateException("Live lease is already fully closed");
		}

		private void release() {
			int after = references.decrementAndGet();
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC STORE TRACE] live release store=" + System.identityHashCode(MaterializedStoreImpl.this)
					+ " index=" + index + " refs->" + after + " entry=" + System.identityHashCode(entry)
					+ " allowance=" + System.identityHashCode(allowance));
			if(after == 0) {
				if(OOCDebug.TRACE_HOT_PATH)
					System.out.println("[OOC STORE TRACE] live unpin store=" + System.identityHashCode(MaterializedStoreImpl.this)
						+ " index=" + index + " entry=" + System.identityHashCode(entry)
						+ " allowance=" + System.identityHashCode(allowance));
				cache.unpin(entry, allowance);
				tryForget(index);
			}
		}
	}

	private final class LiveLeaseAlias implements LiveLease<T> {
		private final LiveLeaseState state;
		private boolean open;

		private LiveLeaseAlias(LiveLeaseState state) {
			this.state = state;
			open = true;
		}

		@Override
		public int index() {
			return state.index;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T value() {
			if(!open)
				throw new IllegalStateException("Lease is closed");
			return (T)state.entry.getData();
		}

		@Override
		public BlockEntry entry() {
			if(!open)
				throw new IllegalStateException("Lease is closed");
			return state.entry;
		}

		@Override
		public LiveLease<T> retain() {
			if(!open)
				throw new IllegalStateException("Lease is closed");
			state.retain();
			return new LiveLeaseAlias(state);
		}

		@Override
		public void close() {
			if(!open)
				return;
			open = false;
			state.release();
		}
	}

	private final class IndexedStoreReader implements IndexedReader<T>, RegisteredReader, LeaseReleaser {
		private final Liveness liveness;
		private final MemoryAllowance allowance;
		private volatile boolean closed;

		private IndexedStoreReader(Liveness liveness, MemoryAllowance allowance) {
			this.liveness = liveness;
			this.allowance = allowance;
		}

		@Override
		public Liveness liveness() {
			return liveness;
		}

		@Override
		public boolean isClosed() {
			return closed;
		}

		@Override
		public OOCFuture<Lease<T>> request(int index) {
			checkReady(index);
			reserve(index);
			OOCFuture<BlockEntry> pinned = new OOCFuture<>();
			StorePinRetry.pinWithRetry(cache, streamId, index, allowance, () -> closed, pinned);
			//complete a fresh future exactly once; a mapped view would create one lease per read
			OOCFuture<Lease<T>> result = new OOCFuture<>();
			pinned.whenComplete((entry, error) -> {
				if(error != null) {
					liveness.unreserve(index);
					result.completeExceptionally(error);
				}
				else if(entry == null) {
					liveness.unreserve(index);
					result.complete(null);
				}
				else
					result.complete(new LeaseAlias(this, index, entry));
			});
			return result;
		}

		@Override
		public Lease<T> requestIfLive(int index) {
			checkReady(index);
			reserve(index);
			BlockEntry entry = cache.pinIfLive(streamId, index, allowance);
			if(entry == null) {
				liveness.unreserve(index);
				return null;
			}
			return new LeaseAlias(this, index, entry);
		}

		@Override
		public void close() {
			if(closed)
				return;
			closed = true;
			forgetAfterReaderClose();
		}

		@Override
		public void release(int index, BlockEntry entry) {
			cache.unpin(entry, allowance);
			liveness.consumed(index);
			tryForget(index);
		}

		private void reserve(int index) {
			if(!liveness.reserve(index))
				throw new IllegalStateException("Index is no longer live for this reader: " + index);
		}

		private void checkReady(int index) {
			if(closed)
				throw new IllegalStateException("Reader is closed");
			if(index < 0 || index >= completedSize)
				throw new IndexOutOfBoundsException("Invalid requested index: " + index);
		}
	}

	private final class StoreReader implements Reader<T>, RegisteredReader, LeaseReleaser {
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
		public Liveness liveness() {
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
			return new LeaseAlias(this, request.index, entry);
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
			forgetAfterReaderClose();
		}

		private void checkReady() {
			if(closed)
				throw new IllegalStateException("Reader is closed");
		}

		private void fill() {
			while(requests.size() < maxPrefetch && pattern.hasNext()) {
				int index = pattern.next();
				OOCFuture<BlockEntry> future = cache.pin(streamId, index, allowance);
				requests.addLast(new Request(index, future));
			}
		}

		private BlockEntry awaitEntry(Request request) throws InterruptedException {
			try {
				BlockEntry entry = request.future.get();
				if(entry == null) {
					//admission failed; retry asynchronously with backoff instead of polling here
					OOCFuture<BlockEntry> retried = new OOCFuture<>();
					StorePinRetry.pinWithRetry(cache, streamId, request.index, allowance, () -> closed, retried);
					request.future = retried;
					entry = retried.get();
					if(entry == null)
						throw new IllegalStateException("Reader is closed");
				}
				return entry;
			}
			catch(ExecutionException e) {
				throw DMLRuntimeException.of(e.getCause());
			}
		}

		@Override
		public void release(int index, BlockEntry entry) {
			cache.unpin(entry, allowance);
			pattern.consumed(index);
			tryForget(index);
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
			consumedIndices = new ConcurrentBitSet(Math.max(1, completedSize));
			ready = new LinkedBlockingQueue<>();
			blocked = new ArrayDeque<>();
		}

		@Override
		public Liveness liveness() {
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
			forgetAfterReaderClose();
		}

		private void checkReady() {
			if(closed)
				throw new IllegalStateException("Reader is closed");
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
				if(index < 0 || index >= completedSize)
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
				if(index >= 0 && index < completedSize && !consumedIndices.get(index) && pattern.needs(index))
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
