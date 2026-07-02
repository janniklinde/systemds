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

import org.apache.sysds.runtime.ooc.cache.collections.MaskedOnceArrayList;
import org.apache.sysds.runtime.ooc.cache.collections.SegmentedStreamTableList;
import org.apache.sysds.runtime.ooc.cache.eviction.EvictController;
import org.apache.sysds.runtime.ooc.cache.eviction.IndexedObjectPair;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.utils.Statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;

public class OOCCacheImpl implements OOCCache {
	private static final int MIN_EVICTION_CANDIDATES = 1024;
	private static final int MAX_EVICTION_CANDIDATES = 65536;
	private static final long EVICTION_CANDIDATE_BYTE_FACTOR = 250_000;

	private final OOCIOHandler _ioHandler;
	private final SegmentedStreamTableList<BlockEntry> _blocks;
	private final SegmentedStreamTableList<EvictController> _evictControllers;
	private final EvictController _defaultEvictController;
	private final ConcurrentLinkedQueue<BlockKey> _deferredUnpins;
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
		_deferredUnpins = new ConcurrentLinkedQueue<>();
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
		entry.setCacheMeta(meta);
		synchronized(this) {
			checkRunning();
			putEntry(entry);
		}
		Statistics.incrementOOCEvictionPut();
		return entry;
	}

	@Override
	public OOCFuture<BlockEntry> pin(long sId, long tId, MemoryAllowance allowance) {
		BlockKey key = new BlockKey(sId, tId);
		BlockEntry entry = findEntry(key);
		EntryMeta meta;
		BlockEntry adopted = null;
		DeferredUnpinHandle adoptedHandle = null;
		long reserveBytes = -1;
		synchronized(this) {
			checkRunning();
			if(entry == null)
				return OOCFuture.completed(null);
			meta = getMeta(entry);
			if(meta == null)
				return OOCFuture.completed(null);
			if(meta.deferredUnpin != null) {
				if(meta.deferredUnpin.allowance == allowance) {
					adoptedHandle = meta.deferredUnpin;
					meta.deferredUnpin = null;
					adopted = entry;
					Statistics.incrementOOCEvictionGet();
				}
				else
					reserveBytes = entry.getSize();
			}
			else if(entry.getDataUnsafe() != null && entry.getState() != BlockState.COLD &&
				entry.getState() != BlockState.READING) {
				reserveBytes = entry.getSize();
			}
		}
		if(adopted != null) {
			adoptedHandle.complete(false);
			return OOCFuture.completed(adopted);
		}
		if(reserveBytes >= 0) {
			if(!allowance.tryReserve(reserveBytes))
				return OOCFuture.completed(null);
			return pinReserved(key, allowance, reserveBytes);
		}
		return pinFromBacking(meta, allowance);
	}

	@Override
	public OOCFuture<BlockEntry> pinAdmitted(long sId, long tId, MemoryAllowance allowance) {
		OOCFuture<BlockEntry> immediate = pin(sId, tId, allowance);
		if(!immediate.isDone())
			return immediate;
		try {
			if(immediate.getNow(null) != null)
				return immediate;
		}
		catch(RuntimeException ex) {
			return OOCFuture.failed(ex);
		}

		BlockKey key = new BlockKey(sId, tId);
		BlockEntry entry = findEntry(key);
		EntryMeta meta;
		synchronized(this) {
			checkRunning();
			meta = getMeta(entry);
			if(meta == null)
				return OOCFuture.completed(null);
		}

		long bytes = entry.getSize();
		OOCFuture<BlockEntry> result = new OOCFuture<>();
		allowance.reserveAsync(bytes).whenComplete((ignored, error) -> {
			if(error != null) {
				result.completeExceptionally(error);
				return;
			}
			try {
				pinReserved(key, allowance, bytes).whenComplete((pinned, pinError) -> {
					if(pinError != null)
						result.completeExceptionally(pinError);
					else
						result.complete(pinned);
				});
			}
			catch(Throwable t) {
				allowance.release(bytes);
				result.completeExceptionally(t);
			}
		});
		return result;
	}

	@Override
	public BlockEntry pinIfLive(long sId, long tId, MemoryAllowance allowance) {
		BlockKey key = new BlockKey(sId, tId);
		BlockEntry entry = findEntry(key);
		BlockEntry adopted = null;
		DeferredUnpinHandle adoptedHandle = null;
		long reserveBytes = -1;
		synchronized(this) {
			checkRunning();
			if(entry == null)
				return null;
			EntryMeta meta = getMeta(entry);
			if(meta == null || entry.getDataUnsafe() == null)
				return null;
			if(meta.deferredUnpin != null) {
				if(meta.deferredUnpin.allowance == allowance) {
					adoptedHandle = meta.deferredUnpin;
					meta.deferredUnpin = null;
					adopted = entry;
					Statistics.incrementOOCEvictionGet();
				}
				else
					reserveBytes = entry.getSize();
			}
			else if(entry.getState() == BlockState.COLD || entry.getState() == BlockState.READING)
				return null;
			else
				reserveBytes = entry.getSize();
		}
		if(adopted != null) {
			adoptedHandle.complete(false);
			return adopted;
		}
		if(reserveBytes < 0 || !allowance.tryReserve(reserveBytes))
			return null;
		return pinIfLiveReserved(key, allowance, reserveBytes);
	}

	@Override
	public UnpinHandle unpin(BlockEntry entry, MemoryAllowance allowance) {
		if(entry.fastUnpin()) {
			allowance.release(entry.getSize());
			return ImmediateUnpinHandle.committed(entry, allowance, entry.getSize());
		}
		UnpinHandle result;
		long releaseBytes = 0;
		synchronized(this) {
			EntryMeta meta = getMeta(entry);
			if(meta == null)
				return ImmediateUnpinHandle.committed(entry, allowance, Math.max(0, entry.getSize()));
			if(entry.getPinCount() > 1) {
				entry.unpin();
				releaseBytes = entry.getSize();
				result = ImmediateUnpinHandle.committed(entry, allowance, releaseBytes);
			}
			else if(canAcceptOwnedBytes(entry.getSize())) {
				releaseBytes = entry.getSize();
				result = commitLastUnpin(meta, allowance);
			}
			else {
				DeferredUnpinHandle handle = new DeferredUnpinHandle(meta, allowance);
				meta.deferredUnpin = handle;
				_deferredUnpins.offer(entry.getKey());
				return handle;
			}
		}
		if(releaseBytes > 0)
			allowance.release(releaseBytes);
		return result;
	}

	@Override
	public synchronized int reference(BlockEntry entry) {
		return entry.addReference();
	}

	@Override
	public int dereference(BlockEntry entry) {
		int refs;
		synchronized(this) {
			EntryMeta meta = getMeta(entry);
			if(meta == null)
				return 0;
			refs = entry.forget();
			if(refs <= 0)
				removeIfUnused(meta);
		}
		return refs;
	}

	@Override
	public int dereference(BlockKey key) {
		BlockEntry entry = findEntry(key);
		if(entry == null)
			return 0;
		int refs;
		synchronized(this) {
			EntryMeta meta = getMeta(entry);
			if(meta == null)
				return 0;
			refs = entry.forget();
			if(refs <= 0)
				removeIfUnused(meta);
		}
		return refs;
	}

	@Override
	public void updateLimits(long hardLimit, long evictionLimit) {
		List<DeferredCompletion> completions;
		synchronized(this) {
			_hardLimit = hardLimit;
			_evictionLimit = evictionLimit;
			completions = processDeferredUnpins();
			scheduleEvictionIfNeeded();
		}
		completeDeferred(completions);
	}

	@Override
	public synchronized void addEvictionPolicy(long streamId, LongUnaryOperator scoreFn) {
		getOrCreateEvictController(streamId).addEvictionPolicy(scoreFn);
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
		_deferredUnpins.clear();
		_ownedBytes = 0;
		_evictingBytes = 0;
		_ioHandler.shutdown();
	}

	private OOCFuture<BlockEntry> pinFromBacking(EntryMeta meta, MemoryAllowance allowance) {
		if(!allowance.tryReserve(meta.entry.getSize()))
			return OOCFuture.completed(null);
		return pinFromBackingReserved(meta, allowance, meta.entry.getSize());
	}

	private OOCFuture<BlockEntry> pinReserved(BlockKey key, MemoryAllowance allowance, long reservedBytes) {
		EntryMeta meta;
		BlockEntry adopted = null;
		DeferredUnpinHandle adoptedHandle = null;
		MemoryAllowance releaseAllowance = null;
		DeferredCompletion deferredCompletion = null;
		BlockEntry resident = null;
		long releaseBytes = 0;
		boolean releaseReserved = false;
		boolean returnNull = false;
		synchronized(this) {
			if(!_running) {
				releaseReserved = true;
				returnNull = true;
			}
			if(returnNull)
				meta = null;
			else {
				BlockEntry entry = findEntry(key);
				meta = getMeta(entry);
				if(meta == null) {
					releaseReserved = true;
					returnNull = true;
				}
				else if(meta.deferredUnpin != null) {
					adoptedHandle = meta.deferredUnpin;
					meta.deferredUnpin = null;
					adopted = meta.entry;
					if(adoptedHandle.allowance == allowance)
						releaseReserved = true;
					else {
						releaseAllowance = adoptedHandle.allowance;
						releaseBytes = meta.entry.getSize();
					}
					Statistics.incrementOOCEvictionGet();
				}
				else if(entry.getDataUnsafe() != null && entry.getState() != BlockState.COLD &&
						entry.getState() != BlockState.READING) {
					deferredCompletion = pinResident(meta);
					Statistics.incrementOOCEvictionGet();
					resident = entry;
				}
			}
		}
		if(releaseReserved)
			allowance.release(reservedBytes);
		if(releaseAllowance != null) {
			releaseAllowance.release(releaseBytes);
			adoptedHandle.complete(false);
		}
		else if(adoptedHandle != null)
			adoptedHandle.complete(false);
		completeDeferred(deferredCompletion);
		if(resident != null)
			return OOCFuture.completed(resident);
		if(returnNull || adopted != null)
			return OOCFuture.completed(adopted);
		return pinFromBackingReserved(meta, allowance, reservedBytes);
	}

	private BlockEntry pinIfLiveReserved(BlockKey key, MemoryAllowance allowance, long reservedBytes) {
		BlockEntry adopted = null;
		DeferredUnpinHandle adoptedHandle = null;
		MemoryAllowance releaseAllowance = null;
		DeferredCompletion deferredCompletion = null;
		BlockEntry resident = null;
		long releaseBytes = 0;
		boolean releaseReserved = false;
		boolean returnNull = false;
		synchronized(this) {
			if(!_running) {
				releaseReserved = true;
				returnNull = true;
			}
			else {
				BlockEntry entry = findEntry(key);
				EntryMeta meta = getMeta(entry);
				if(meta == null || entry.getDataUnsafe() == null) {
					releaseReserved = true;
					returnNull = true;
				}
				else if(meta.deferredUnpin != null) {
					adoptedHandle = meta.deferredUnpin;
					meta.deferredUnpin = null;
					adopted = meta.entry;
					if(adoptedHandle.allowance == allowance)
						releaseReserved = true;
					else {
						releaseAllowance = adoptedHandle.allowance;
						releaseBytes = meta.entry.getSize();
					}
					Statistics.incrementOOCEvictionGet();
				}
				else if(entry.getState() == BlockState.COLD || entry.getState() == BlockState.READING) {
					releaseReserved = true;
					returnNull = true;
				}
				else {
					deferredCompletion = pinResident(meta);
					Statistics.incrementOOCEvictionGet();
					resident = entry;
				}
			}
		}
		if(releaseReserved)
			allowance.release(reservedBytes);
		if(releaseAllowance != null) {
			releaseAllowance.release(releaseBytes);
			adoptedHandle.complete(false);
		}
		else if(adoptedHandle != null)
			adoptedHandle.complete(false);
		completeDeferred(deferredCompletion);
		if(resident != null)
			return resident;
		return returnNull ? null : adopted;
	}

	private OOCFuture<BlockEntry> pinFromBackingReserved(EntryMeta meta, MemoryAllowance allowance,
		long reservedBytes) {
		OOCFuture<BlockEntry> readFuture;
		boolean releaseReserved = false;
		DeferredCompletion deferredCompletion = null;
		BlockEntry resident = null;
		synchronized(this) {
			if(!_running || getMeta(meta.entry) != meta) {
				releaseReserved = true;
				readFuture = null;
			}
			else if(meta.entry.getDataUnsafe() != null) {
				deferredCompletion = pinResident(meta);
				Statistics.incrementOOCEvictionGet();
				resident = meta.entry;
				readFuture = null;
			}
			else if(meta.readFuture == null) {
				meta.entry.setState(BlockState.READING);
				OOCFuture<BlockEntry> scheduled = _ioHandler.scheduleRead(meta.entry);
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
		if(releaseReserved) {
			allowance.release(reservedBytes);
			return OOCFuture.completed(null);
		}
		completeDeferred(deferredCompletion);
		if(resident != null)
			return OOCFuture.completed(resident);

		OOCFuture<BlockEntry> result = new OOCFuture<>();
		readFuture.whenComplete((entry, ex) -> {
			boolean release = false;
			DeferredCompletion completion = null;
			try {
				if(ex != null) {
					release = true;
					allowance.release(reservedBytes);
					result.completeExceptionally(ex);
					return;
				}
				BlockEntry pinned;
				synchronized(OOCCacheImpl.this) {
					if(entry == null || getMeta(meta.entry) != meta || meta.entry.getDataUnsafe() == null) {
						release = true;
						if(meta.entry.getState() == BlockState.READING)
							meta.entry.setState(BlockState.COLD);
						pinned = null;
					}
					else {
						completion = pinResident(meta);
						Statistics.incrementOOCEvictionGet();
						pinned = meta.entry;
					}
				}
				if(release)
					allowance.release(reservedBytes);
				completeDeferred(completion);
				result.complete(pinned);
			}
			catch(Throwable t) {
				if(!release)
					allowance.release(reservedBytes);
				result.completeExceptionally(t);
			}
		});
		return result;
	}

	private DeferredCompletion pinResident(EntryMeta meta) {
		BlockEntry entry = meta.entry;
		if(isCacheOwned(entry)) {
			_ownedBytes -= entry.getSize();
			if(entry.getState() == BlockState.EVICTING)
				_evictingBytes -= entry.getSize();
			clearLive(entry);
		}
		entry.setState(BlockState.REMOVED);
		entry.pin();
		return resolveDeferredUnpin(meta, false);
	}

	private UnpinHandle commitLastUnpin(EntryMeta meta, MemoryAllowance allowance) {
		BlockEntry entry = meta.entry;
		entry.unpin();
		if(entry.getReferenceCount() <= 0) {
			removeEntry(entry.getKey());
			entry.clear();
			entry.setCacheMeta(null);
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

	private DeferredCompletion resolveDeferredUnpin(EntryMeta meta, boolean keepPhysicalPin) {
		DeferredUnpinHandle handle = meta.deferredUnpin;
		if(handle == null)
			return null;
		long bytes = meta.entry.getSize();
		meta.deferredUnpin = null;
		if(!keepPhysicalPin)
			meta.entry.unpin();
		return new DeferredCompletion(handle, bytes, false);
	}

	private List<DeferredCompletion> processDeferredUnpins() {
		List<DeferredCompletion> completions = null;
		while(true) {
			BlockKey key = _deferredUnpins.peek();
			if(key == null)
				return completions;
			BlockEntry entry = findEntry(key);
			EntryMeta meta = getMeta(entry);
			if(meta == null || meta.deferredUnpin == null) {
				_deferredUnpins.poll();
				continue;
			}
			if(!canAcceptOwnedBytes(meta.entry.getSize()))
				return completions;
			_deferredUnpins.poll();
			DeferredCompletion completion = commitDeferredUnpin(meta);
			if(completion != null) {
				if(completions == null)
					completions = new ArrayList<>();
				completions.add(completion);
			}
		}
	}

	private DeferredCompletion commitDeferredUnpin(EntryMeta meta) {
		DeferredUnpinHandle handle = meta.deferredUnpin;
		if(handle == null)
			return null;
		meta.deferredUnpin = null;
		BlockEntry entry = meta.entry;
		long bytes = entry.getSize();
		entry.unpin();
		if(entry.getReferenceCount() <= 0) {
			removeEntry(entry.getKey());
			entry.clear();
			entry.setCacheMeta(null);
			if(meta.backed)
				_ioHandler.scheduleDeletion(entry);
		}
		else {
			entry.setState(meta.backed ? BlockState.WARM : BlockState.HOT);
			setLive(entry);
			_ownedBytes += entry.getSize();
		}
		return new DeferredCompletion(handle, bytes, true);
	}

	private void completeDeferred(List<DeferredCompletion> completions) {
		if(completions == null)
			return;
		for(DeferredCompletion completion : completions)
			completeDeferred(completion);
	}

	private void completeDeferred(DeferredCompletion completion) {
		if(completion == null)
			return;
		completion.handle.allowance.release(completion.bytes);
		completion.handle.complete(completion.committed);
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
				List<DeferredCompletion> completions;
				boolean progress = false;
				synchronized(this) {
					for(IndexedObjectPair<BlockEntry> candidate : candidates) {
						if(evictionPressure() <= _evictionLimit)
							break;
						EntryMeta meta = getMeta(candidate.obj());
						if(meta == null || candidate.obj().getPinCount() > 0 || meta.deferredUnpin != null)
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
					completions = processDeferredUnpins();
				}
				completeDeferred(completions);
				for(BlockEntry entry : toWrite)
					_ioHandler.scheduleEviction(entry).whenComplete((ignored, ex) -> onEvicted(entry, ex));
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

	private void onEvicted(BlockEntry entry, Throwable ex) {
		List<DeferredCompletion> completions = null;
		synchronized(this) {
			EntryMeta meta = getMeta(entry);
			if(meta == null)
				return;
			if(ex != null) {
				if(entry.getState() == BlockState.EVICTING) {
					entry.setState(BlockState.HOT);
					_evictingBytes -= entry.getSize();
					setLive(entry);
					scheduleEvictionIfNeeded();
				}
				return;
			}
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
			completions = processDeferredUnpins();
			scheduleEvictionIfNeeded();
		}
		completeDeferred(completions);
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

	private EvictController getOrCreateEvictController(long streamId) {
		MaskedOnceArrayList<EvictController> controllers = _evictControllers.getOrCreate(streamId);
		EvictController controller = controllers.get(0);
		if(controller != null)
			return controller;
		controller = new EvictController();
		controllers.put(0, controller);
		return controller;
	}

	private void removeIfUnused(EntryMeta meta) {
		if(meta == null || meta.entry.getReferenceCount() > 0 || meta.entry.getPinCount() > 0 ||
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
		entry.setCacheMeta(null);
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

	private EntryMeta getMeta(BlockEntry entry) {
		return entry == null ? null : (EntryMeta)entry.getCacheMeta();
	}

	private static class EntryMeta {
		private final BlockEntry entry;
		private boolean backed;
		private OOCFuture<BlockEntry> readFuture;
		private DeferredUnpinHandle deferredUnpin;

		private EntryMeta(BlockEntry entry) {
			this.entry = entry;
			backed = entry.getState().isBackedByDisk();
		}
	}

	private class DeferredCompletion {
		private final DeferredUnpinHandle handle;
		private final long bytes;
		private final boolean committed;

		private DeferredCompletion(DeferredUnpinHandle handle, long bytes, boolean committed) {
			this.handle = handle;
			this.bytes = bytes;
			this.committed = committed;
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
			BlockEntry entry;
			synchronized(OOCCacheImpl.this) {
				if(completed || meta.deferredUnpin != this)
					return null;
				meta.deferredUnpin = null;
				entry = meta.entry;
			}
			complete(false);
			return entry;
		}

		private synchronized void complete(boolean committed) {
			if(completed)
				return;
			completed = true;
			future.complete(committed);
		}
	}
}
