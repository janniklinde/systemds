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

package org.apache.sysds.runtime.ooc.cache;

import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class OOCCacheImpl implements OOCCache {
	private static final int MIN_EVICTION_CANDIDATES = 1024;
	private static final int MAX_EVICTION_CANDIDATES = 65536;
	private static final long EVICTION_CANDIDATE_BYTE_FACTOR = 250_000;

	private final OOCIOHandler _ioHandler;
	private final SegmentedStreamTableList<BlockEntry> _blocks;
	private final SegmentedStreamTableList<EvictController> _evictControllers;
	private final EvictController _defaultEvictController;
	private final IdentityHashMap<BlockEntry, EntryMeta> _meta;
	private final Executor _collectorExecutor;
	private final AtomicBoolean _evictionRunning;

	private long _hardLimit;
	private long _evictionLimit;
	private long _ownedBytes;
	private long _evictingBytes;
	private boolean _running;

	public OOCCacheImpl(OOCIOHandler ioHandler, long hardLimit, long evictionLimit) {
		_ioHandler = ioHandler;
		_hardLimit = hardLimit;
		_evictionLimit = evictionLimit;
		_ownedBytes = 0;
		_evictingBytes = 0;
		_running = true;
		_blocks = new SegmentedStreamTableList<>();
		_evictControllers = new SegmentedStreamTableList<>();
		_defaultEvictController = new EvictController();
		_meta = new IdentityHashMap<>();
		_collectorExecutor = Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "ooc-cache-collector");
			t.setDaemon(true);
			return t;
		});
		_evictionRunning = new AtomicBoolean(false);
	}

	@Override
	public BlockEntry putPinned(long sId, long tId, Object data, long size, MemoryAllowance allowance) {
		BlockKey key = new BlockKey(sId, tId);
		BlockEntry entry = new BlockEntry(key, size, data, BlockState.REMOVED);
		entry.pin();
		EntryMeta meta = new EntryMeta(entry);
		meta.addPin(allowance);
		synchronized(this) {
			checkRunning();
			putEntry(entry);
			_meta.put(entry, meta);
		}
		return entry;
	}

	@Override
	public CompletableFuture<BlockEntry> pin(long sId, long tId, MemoryAllowance allowance) {
		BlockEntry entry;
		EntryMeta meta;
		synchronized(this) {
			checkRunning();
			entry = findEntry(new BlockKey(sId, tId));
			if(entry == null)
				return CompletableFuture.completedFuture(null);
			meta = _meta.get(entry);
			BlockEntry adopted = tryAdoptDeferredPin(meta, allowance);
			if(adopted != null)
				return CompletableFuture.completedFuture(adopted);
		}

		BlockEntry immediate = pinIfLive(sId, tId, allowance);
		if(immediate != null)
			return CompletableFuture.completedFuture(immediate);
		return pinFromBacking(meta, allowance);
	}

	@Override
	public BlockEntry pinIfLive(long sId, long tId, MemoryAllowance allowance) {
		synchronized(this) {
			checkRunning();
			BlockEntry entry = findEntry(new BlockKey(sId, tId));
			if(entry == null || entry.getDataUnsafe() == null)
				return null;
			EntryMeta meta = _meta.get(entry);
			BlockEntry adopted = tryAdoptDeferredPin(meta, allowance);
			if(adopted != null)
				return adopted;
			if(entry.getState() == BlockState.COLD || entry.getState() == BlockState.READING)
				return null;
			if(!allowance.tryReserve(entry.getSize()))
				return null;
			pinResident(meta, allowance);
			return entry;
		}
	}

	@Override
	public UnpinHandle unpin(BlockEntry entry, MemoryAllowance allowance) {
		synchronized(this) {
			EntryMeta meta = _meta.get(entry);
			if(meta == null)
				return ImmediateUnpinHandle.committed(entry, allowance, Math.max(0, entry.getSize()));

			meta.removePin(allowance);
			if(meta.activePinCount() > 0) {
				entry.unpin();
				allowance.release(entry.getSize());
				return ImmediateUnpinHandle.committed(entry, allowance, entry.getSize());
			}

			if(canAcceptOwnedBytes(entry.getSize()))
				return commitLastUnpin(meta, allowance);

			DeferredUnpinHandle handle = new DeferredUnpinHandle(meta, allowance);
			meta.deferredUnpin = handle;
			return handle;
		}
	}

	@Override
	public synchronized int reference(BlockEntry entry) {
		return entry.addReference();
	}

	@Override
	public int dereference(BlockEntry entry) {
		int refs;
		synchronized(this) {
			refs = entry.forget();
			if(refs <= 0)
				removeIfUnused(_meta.get(entry));
		}
		return refs;
	}

	@Override
	public synchronized void updateLimits(long hardLimit, long evictionLimit) {
		_hardLimit = hardLimit;
		_evictionLimit = evictionLimit;
		processDeferredUnpins();
		scheduleEvictionIfNeeded();
	}

	@Override
	public synchronized long getOwnedCacheSize() {
		return _ownedBytes;
	}

	@Override
	public synchronized void shutdown() {
		_running = false;
		_blocks.clear();
		_meta.clear();
		_ownedBytes = 0;
		_evictingBytes = 0;
		_ioHandler.shutdown();
	}

	private CompletableFuture<BlockEntry> pinFromBacking(EntryMeta meta, MemoryAllowance allowance) {
		if(!allowance.tryReserve(meta.entry.getSize()))
			return CompletableFuture.completedFuture(null);

		CompletableFuture<BlockEntry> readFuture;
		synchronized(this) {
			if(meta.entry.getDataUnsafe() != null) {
				pinResident(meta, allowance);
				return CompletableFuture.completedFuture(meta.entry);
			}
				if(meta.readFuture == null) {
					meta.entry.setState(BlockState.READING);
					CompletableFuture<BlockEntry> scheduled = _ioHandler.scheduleRead(meta.entry);
					meta.readFuture = scheduled;
					readFuture = scheduled;
					scheduled.whenComplete((entry, ex) -> {
						synchronized(OOCCacheImpl.this) {
							if(meta.readFuture == scheduled)
								meta.readFuture = null;
							if(ex != null && meta.entry.getState() == BlockState.READING)
								meta.entry.setState(BlockState.COLD);
						}
					});
				}
				else
					readFuture = meta.readFuture;
			}

		return readFuture.handle((entry, ex) -> {
			if(ex != null) {
				allowance.release(meta.entry.getSize());
				throw new CompletionException(ex);
			}
			synchronized(OOCCacheImpl.this) {
				if(entry == null || meta.entry.getDataUnsafe() == null) {
					allowance.release(meta.entry.getSize());
					if(meta.entry.getState() == BlockState.READING)
						meta.entry.setState(BlockState.COLD);
					return null;
				}
				pinResident(meta, allowance);
				return meta.entry;
			}
		});
	}

	private void pinResident(EntryMeta meta, MemoryAllowance allowance) {
		BlockEntry entry = meta.entry;
		if(isCacheOwned(entry)) {
			_ownedBytes -= entry.getSize();
			if(entry.getState() == BlockState.EVICTING)
				_evictingBytes -= entry.getSize();
			clearLive(entry);
		}
		entry.setState(BlockState.REMOVED);
		entry.pin();
		meta.addPin(allowance);
		resolveDeferredUnpin(meta, false);
	}

	private BlockEntry tryAdoptDeferredPin(EntryMeta meta, MemoryAllowance allowance) {
		if(meta == null || meta.deferredUnpin == null)
			return null;
		DeferredUnpinHandle handle = meta.deferredUnpin;
		if(handle.allowance != allowance) {
			if(!allowance.tryReserve(meta.entry.getSize()))
				return null;
			handle.allowance.release(meta.entry.getSize());
			handle.complete(false);
		}
		else {
			handle.complete(false);
		}
		meta.deferredUnpin = null;
		meta.addPin(allowance);
		return meta.entry;
	}

	private UnpinHandle commitLastUnpin(EntryMeta meta, MemoryAllowance allowance) {
		BlockEntry entry = meta.entry;
		entry.unpin();
		allowance.release(entry.getSize());
		if(entry.getReferenceCount() <= 0) {
			removeEntry(entry.getKey());
			entry.clear();
			_meta.remove(entry);
			if(meta.backed)
				_ioHandler.scheduleDeletion(entry);
			return ImmediateUnpinHandle.committed(entry, allowance, entry.getSize());
		}
		entry.setState(meta.backed ? BlockState.WARM : BlockState.HOT);
		setLive(entry);
		_ownedBytes += entry.getSize();
		scheduleEvictionIfNeeded();
		return ImmediateUnpinHandle.committed(entry, allowance, entry.getSize());
	}

	private void resolveDeferredUnpin(EntryMeta meta, boolean keepPhysicalPin) {
		DeferredUnpinHandle handle = meta.deferredUnpin;
		if(handle == null)
			return;
		meta.deferredUnpin = null;
		if(!keepPhysicalPin)
			meta.entry.unpin();
		handle.allowance.release(meta.entry.getSize());
		handle.complete(false);
	}

	private void processDeferredUnpins() {
		for(EntryMeta meta : new ArrayList<>(_meta.values())) {
			if(meta.deferredUnpin == null || !canAcceptOwnedBytes(meta.entry.getSize()))
				continue;
			commitDeferredUnpin(meta);
		}
	}

	private void commitDeferredUnpin(EntryMeta meta) {
		DeferredUnpinHandle handle = meta.deferredUnpin;
		if(handle == null)
			return;
		meta.deferredUnpin = null;
		BlockEntry entry = meta.entry;
		entry.unpin();
		handle.allowance.release(entry.getSize());
		if(entry.getReferenceCount() <= 0) {
			removeEntry(entry.getKey());
			entry.clear();
			_meta.remove(entry);
			if(meta.backed)
				_ioHandler.scheduleDeletion(entry);
		}
		else {
			entry.setState(meta.backed ? BlockState.WARM : BlockState.HOT);
			setLive(entry);
			_ownedBytes += entry.getSize();
		}
		handle.complete(true);
	}

	private boolean canAcceptOwnedBytes(long bytes) {
		return _ownedBytes + bytes <= _hardLimit;
	}

	private void scheduleEvictionIfNeeded() {
		if(evictionPressure() <= _evictionLimit || !_evictionRunning.compareAndSet(false, true))
			return;
		_collectorExecutor.execute(this::runEviction);
	}

	private void runEviction() {
		try {
			while(true) {
				long bytes;
				synchronized(this) {
					bytes = evictionPressure() - _evictionLimit;
					if(bytes <= 0)
						return;
				}

				List<IndexedObjectPair<BlockEntry>> candidates = collectEvictionCandidates(bytes);
				if(candidates.isEmpty())
					return;

				List<BlockEntry> toWrite = new ArrayList<>();
				boolean progress = false;
				synchronized(this) {
					for(IndexedObjectPair<BlockEntry> candidate : candidates) {
						if(evictionPressure() <= _evictionLimit)
							break;
						EntryMeta meta = _meta.get(candidate.obj());
						if(meta == null || meta.activePinCount() > 0 || meta.deferredUnpin != null)
							continue;
						BlockEntry entry = meta.entry;
						if(entry.getState() == BlockState.WARM) {
							entry.clear();
							entry.setState(BlockState.COLD);
							clearLive(entry);
							_ownedBytes -= entry.getSize();
							progress = true;
						}
						else if(entry.getState() == BlockState.HOT) {
							entry.setState(BlockState.EVICTING);
							_evictingBytes += entry.getSize();
							clearLive(entry);
							toWrite.add(entry);
							progress = true;
						}
					}
					processDeferredUnpins();
				}
				for(BlockEntry entry : toWrite)
					_ioHandler.scheduleEviction(entry).whenComplete((ignored, ex) -> onEvicted(entry));
				if(!progress)
					return;
			}
		}
		finally {
			_evictionRunning.set(false);
			synchronized(this) {
				if(evictionPressure() > _evictionLimit)
					scheduleEvictionIfNeeded();
			}
		}
	}

	private void onEvicted(BlockEntry entry) {
		synchronized(this) {
			EntryMeta meta = _meta.get(entry);
			if(meta == null)
				return;
			meta.backed = true;
			if(entry.getState() == BlockState.HOT) {
				entry.setState(BlockState.WARM);
				return;
			}
			if(entry.getState() != BlockState.EVICTING)
				return;
			entry.clear();
			entry.setState(BlockState.COLD);
			_ownedBytes -= entry.getSize();
			_evictingBytes -= entry.getSize();
			removeIfUnused(meta);
			processDeferredUnpins();
			scheduleEvictionIfNeeded();
		}
	}

	private List<IndexedObjectPair<BlockEntry>> collectEvictionCandidates(long bytes) {
		int k = evictionCandidateLimit(bytes);
		PriorityQueue<IndexedObjectPair<BlockEntry>> queue = new PriorityQueue<>();
		_blocks.forEachStreamTable((streamId, stream) ->
			getEvictController(streamId).findEvictionCandidates(stream, queue, k, 0));

		List<IndexedObjectPair<BlockEntry>> candidates = new ArrayList<>(queue.size());
		while(!queue.isEmpty())
			candidates.add(queue.poll());
		Collections.reverse(candidates);
		return candidates;
	}

	private int evictionCandidateLimit(long bytes) {
		long limit = Math.max(MIN_EVICTION_CANDIDATES,
			(bytes + EVICTION_CANDIDATE_BYTE_FACTOR - 1) / EVICTION_CANDIDATE_BYTE_FACTOR);
		return (int)Math.min(MAX_EVICTION_CANDIDATES, limit);
	}

	private EvictController getEvictController(long streamId) {
		MaskedOnceArrayList<EvictController> controllers = _evictControllers.get(streamId);
		if(controllers == null)
			return _defaultEvictController;
		EvictController controller = controllers.get(0);
		return controller == null ? _defaultEvictController : controller;
	}

	private void removeIfUnused(EntryMeta meta) {
		if(meta == null || meta.entry.getReferenceCount() > 0 || meta.activePinCount() > 0 ||
			meta.deferredUnpin != null)
			return;
		BlockEntry entry = meta.entry;
		if(isCacheOwned(entry))
			_ownedBytes -= entry.getSize();
		if(entry.getState() == BlockState.EVICTING)
			_evictingBytes -= entry.getSize();
		removeEntry(entry.getKey());
		clearLive(entry);
		entry.clear();
		_meta.remove(entry);
		if(meta.backed)
			_ioHandler.scheduleDeletion(entry);
	}

	private boolean isCacheOwned(BlockEntry entry) {
		return entry.getState() == BlockState.HOT || entry.getState() == BlockState.WARM ||
			entry.getState() == BlockState.EVICTING;
	}

	private long evictionPressure() {
		return _ownedBytes - _evictingBytes;
	}

	private BlockEntry findEntry(BlockKey key) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.get(key.getStreamId());
		return stream == null ? null : stream.get(blockIndex(key));
	}

	private void putEntry(BlockEntry entry) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.getOrCreate(entry.getKey().getStreamId());
		int index = blockIndex(entry.getKey());
		if(stream.get(index) != null)
			throw new IllegalStateException("Cache entry already exists: " + entry.getKey());
		stream.put(index, entry);
	}

	private BlockEntry removeEntry(BlockKey key) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.get(key.getStreamId());
		if(stream == null)
			return null;
		return stream.clear(blockIndex(key)) ? null : stream.get(blockIndex(key));
	}

	private void setLive(BlockEntry entry) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.get(entry.getKey().getStreamId());
		if(stream != null)
			stream.setLive(blockIndex(entry.getKey()));
	}

	private void clearLive(BlockEntry entry) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.get(entry.getKey().getStreamId());
		if(stream != null)
			stream.clearLive(blockIndex(entry.getKey()));
	}

	private int blockIndex(BlockKey key) {
		long sequenceNumber = key.getSequenceNumber();
		if(sequenceNumber < 0 || sequenceNumber > Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("Invalid block index: " + sequenceNumber);
		return (int) sequenceNumber;
	}

	private void checkRunning() {
		if(!_running)
			throw new IllegalStateException("Cache has been shut down.");
	}

	private static class EntryMeta {
		private final BlockEntry entry;
		private final IdentityHashMap<MemoryAllowance, Integer> activePins;
		private boolean backed;
		private CompletableFuture<BlockEntry> readFuture;
		private DeferredUnpinHandle deferredUnpin;

		private EntryMeta(BlockEntry entry) {
			this.entry = entry;
			activePins = new IdentityHashMap<>();
			backed = entry.getState().isBackedByDisk();
		}

		private void addPin(MemoryAllowance allowance) {
			activePins.merge(allowance, 1, Integer::sum);
		}

		private void removePin(MemoryAllowance allowance) {
			Integer count = activePins.get(allowance);
			if(count == null || count <= 1)
				activePins.remove(allowance);
			else
				activePins.put(allowance, count - 1);
		}

		private int activePinCount() {
			int count = 0;
			for(Integer pins : activePins.values())
				count += pins;
			return count;
		}
	}

	private static class ImmediateUnpinHandle implements UnpinHandle {
		private final BlockEntry entry;
		private final MemoryAllowance allowance;
		private final long bytes;
		private final CompletableFuture<Boolean> future;

		private static ImmediateUnpinHandle committed(BlockEntry entry, MemoryAllowance allowance, long bytes) {
			return new ImmediateUnpinHandle(entry, allowance, bytes);
		}

		private ImmediateUnpinHandle(BlockEntry entry, MemoryAllowance allowance, long bytes) {
			this.entry = entry;
			this.allowance = allowance;
			this.bytes = bytes;
			future = CompletableFuture.completedFuture(true);
		}

		@Override
		public BlockEntry getEntry() {
			return entry;
		}

		@Override
		public MemoryAllowance getAllowance() {
			return allowance;
		}

		@Override
		public long getBytes() {
			return bytes;
		}

		@Override
		public boolean isCommitted() {
			return true;
		}

		@Override
		public CompletableFuture<Boolean> getCompletionFuture() {
			return future;
		}

		@Override
		public BlockEntry reclaim() {
			return null;
		}
	}

	private class DeferredUnpinHandle implements UnpinHandle {
		private final EntryMeta meta;
		private final MemoryAllowance allowance;
		private final CompletableFuture<Boolean> future;
		private boolean completed;

		private DeferredUnpinHandle(EntryMeta meta, MemoryAllowance allowance) {
			this.meta = meta;
			this.allowance = allowance;
			future = new CompletableFuture<>();
			completed = false;
		}

		@Override
		public BlockEntry getEntry() {
			return meta.entry;
		}

		@Override
		public MemoryAllowance getAllowance() {
			return allowance;
		}

		@Override
		public long getBytes() {
			return meta.entry.getSize();
		}

		@Override
		public synchronized boolean isCommitted() {
			return completed && Boolean.TRUE.equals(future.getNow(false));
		}

		@Override
		public CompletableFuture<Boolean> getCompletionFuture() {
			return future;
		}

		@Override
		public BlockEntry reclaim() {
			synchronized(OOCCacheImpl.this) {
				if(completed || meta.deferredUnpin != this)
					return null;
				meta.deferredUnpin = null;
				meta.addPin(allowance);
				complete(false);
				return meta.entry;
			}
		}

		private synchronized void complete(boolean committed) {
			if(completed)
				return;
			completed = true;
			future.complete(committed);
		}
	}
}
