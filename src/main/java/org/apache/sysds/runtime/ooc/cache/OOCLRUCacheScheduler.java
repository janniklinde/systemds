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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.utils.Statistics;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class OOCLRUCacheScheduler implements OOCCacheScheduler {
	private static final boolean SANITY_CHECKS = false;
	private static final Log LOG = LogFactory.getLog(OOCLRUCacheScheduler.class.getName());
	private final Executor collectorExecutor =
		Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "buffer-pool-collector");
			t.setDaemon(true);
			return t;
		});
	private final OOCIOHandler _ioHandler;
	private final SegmentedStreamTableList<BlockEntry> _blocks;
	private final SegmentedStreamTableList<EvictController> _evictControllers;
	private final DeferredReadQueue _deferredReadRequests;
	private final Deque<PendingHandover> _pendingHandovers;
	private final Deque<PendingBackingRelease> _pendingBackingReleases;
	private final HashMap<BlockKey, BlockReadState> _blockReads;
	private volatile long _hardLimit;
	private long _evictionLimit;
	private long _readBuffer;
	private final int _callerId;
	private volatile long _cacheSize;
	private long _bytesUpForEviction;
	private long _pinnedBytes;
	private long _pinnedEvictingBytes;
	private long _backedCacheBytes;
	private long _backedEvictingBytes;
	private long _backedWarmPinnedBytes;
	private long _readingReservedBytes;
	private long _warmPinnedBytes;
	private volatile boolean _running;
	private boolean _warnThrottling;
	private long _lastEvictRun;
	private volatile int _deferredReadCountHint;
	private final AtomicBoolean _maintenanceRunning;
	private final AtomicBoolean _maintenanceRequested;
	private final AtomicBoolean _maintenanceNeedsIncr;
	private final AtomicBoolean _maintenanceNeedsDecr;

	public OOCLRUCacheScheduler(OOCIOHandler ioHandler, long evictionLimit, long hardLimit, long readBuffer) {
		this._ioHandler = ioHandler;
		this._blocks = new SegmentedStreamTableList<>();
		this._evictControllers = new SegmentedStreamTableList<>();
		this._deferredReadRequests = new DeferredReadQueue();
		this._pendingHandovers = new ArrayDeque<>();
		this._pendingBackingReleases = new ArrayDeque<>();
		this._blockReads = new HashMap<>();
		this._hardLimit = hardLimit;
		this._evictionLimit = evictionLimit;
		this._readBuffer = readBuffer;
		this._cacheSize = 0;
		this._bytesUpForEviction = 0;
		this._pinnedEvictingBytes = 0;
		this._pinnedBytes = 0;
		this._backedCacheBytes = 0;
		this._backedEvictingBytes = 0;
		this._backedWarmPinnedBytes = 0;
		this._readingReservedBytes = 0;
		this._warmPinnedBytes = 0;
		this._lastEvictRun = System.currentTimeMillis();
		this._deferredReadCountHint = 0;
		this._running = true;
		this._warnThrottling = false;
		this._maintenanceRunning = new AtomicBoolean(false);
		this._maintenanceRequested = new AtomicBoolean(false);
		this._maintenanceNeedsIncr = new AtomicBoolean(false);
		this._maintenanceNeedsDecr = new AtomicBoolean(false);
		this._callerId = DMLScript.OOC_LOG_EVENTS ? OOCEventLog.registerCaller("LRUCacheScheduler") : 0;

		if (DMLScript.OOC_LOG_EVENTS) {
			OOCEventLog.putRunSetting("CacheEvictionLimit", _evictionLimit);
			OOCEventLog.putRunSetting("CacheHardLimit", _hardLimit);
		}
	}

	@Override
	public CompletableFuture<BlockEntry> request(BlockKey key) {
		if (!this._running)
			throw new IllegalStateException("Cache scheduler has been shut down.");

		Statistics.incrementOOCEvictionGet();

		BlockEntry entry;
		PendingHandover pending = null;
		synchronized(this) {
			entry = findEntry(key);
			if (entry == null)
				throw new IllegalArgumentException("Could not find requested block with key " + key);

			synchronized(entry) {
				if(entry.getState() == BlockState.HANDOVER_PENDING) {
					pending = (PendingHandover) entry.getDataUnsafe();
				}
				else if (entry.getState().isAvailable()) {
					if (pinEntryWithAccounting(entry) == 0)
						throw new IllegalStateException();
					return CompletableFuture.completedFuture(entry);
				}
			}
		}

		if(pending != null) {
			pending.retainForCallback();
			return CompletableFuture.completedFuture(entry);
		}

		// Schedule deferred read otherwise
		final CompletableFuture<List<BlockEntry>> requestFuture = new CompletableFuture<>();
		CompletableFuture<BlockEntry> future = requestFuture.thenApply(l -> l.get(0));
		scheduleDeferredRead(new DeferredReadRequest(requestFuture, Collections.singletonList(entry)));
		return future;
	}

	@Override
	public List<BlockEntry> tryRequest(List<BlockKey> keys) {
		CompletableFuture<List<BlockEntry>> f = request(keys, true);
		if(f == null)
			return null;
		return f.getNow(null);
	}

	@Override
	public CompletableFuture<List<BlockEntry>> requestAnyOf(List<BlockKey> keys, int n, List<BlockKey> selectionOut) {
		List<BlockEntry> l = tryRequestAnyOf(keys, n, selectionOut);
		if(l != null)
			return CompletableFuture.completedFuture(l);
		return request(keys.subList(0, n));
	}

	@Override
	public List<BlockEntry> tryRequestAnyOf(List<BlockKey> keys, int n, List<BlockKey> selectionOut) {
		List<BlockEntry> present = new ArrayList<>(n);
		for(BlockKey key : keys) {
			List<BlockEntry> l = tryRequest(List.of(key));
			if(l != null) {
				present.add(l.get(0));
				selectionOut.add(l.get(0).getKey());
				if(present.size() == n)
					return present;
			}
		}
		present.forEach(this::releaseRequestedEntry);
		return null;
	}

	@Override
	public CompletableFuture<List<BlockEntry>> request(List<BlockKey> keys) {
		return request(keys, false);
	}

	public CompletableFuture<List<BlockEntry>> request(List<BlockKey> keys, boolean onlyIfAvailable) {
		if (!this._running)
			throw new IllegalStateException("Cache scheduler has been shut down.");

		Statistics.incrementOOCEvictionGet(keys.size());

		List<BlockEntry> entries = new ArrayList<>(keys.size());
		boolean allRequestable = true;

		synchronized(this) {
			for (BlockKey key : keys) {
				BlockEntry entry = findEntry(key);
				if (entry == null)
					throw new IllegalArgumentException("Could not find requested block with key " + key);

				synchronized(entry) {
					if(entry.getState() == BlockState.HANDOVER_PENDING) {
						// Pending handovers are requestable because the handover callback can serve the tile.
					}
					else if(!entry.getState().isAvailable())
						allRequestable = false;
				}
				entries.add(entry);
			}

			if(allRequestable) {
				for(BlockEntry entry : entries) {
					synchronized(entry) {
						if(entry.getState() == BlockState.HANDOVER_PENDING) {
							PendingHandover pending = (PendingHandover) entry.getDataUnsafe();
							pending.retainForCallback();
						}
						else if(pinEntryWithAccounting(entry) == 0)
							throw new IllegalStateException();
					}
				}
			}
		}

		if (allRequestable) {
			// Then we could pin all entries
			return CompletableFuture.completedFuture(entries);
		}

		if(onlyIfAvailable)
			return null;

		// Schedule deferred read otherwise
		final  CompletableFuture<List<BlockEntry>> future = new CompletableFuture<>();
		DeferredReadRequest request = new DeferredReadRequest(future, entries);
		for (int i = 0; i < entries.size(); i++) {
			BlockEntry entry = entries.get(i);
			synchronized(entry) {
				if (entry.getState().isAvailable()) {
					entry.addRetainHint();
					request.markRetainHinted(i);
				}
			}
		}
		scheduleDeferredRead(request);
		return future;
	}

	@Override
	public void prioritize(BlockKey key, double priority) {
		if (!this._running)
			return;
		if (priority == 0)
			return;

		synchronized(this) {
			boolean matched = _deferredReadRequests.boost(key, priority);
			if(matched) {
				BlockReadState state = _blockReads.computeIfAbsent(key, k -> new BlockReadState());
				state.priority += priority;
			}
		}
		_ioHandler.prioritizeRead(key, priority);
	}

	private void scheduleDeferredRead(DeferredReadRequest deferredReadRequest) {
		synchronized(this) {
			double score = 0;
			int readyCount = 0;
			for(BlockEntry entry : deferredReadRequest.getEntries()) {
				// Snapshot for scheduling heuristic only; exact state will be checked when reserving.
				if(entry.getState().isAvailable())
					readyCount++;
				BlockReadState state = _blockReads.get(entry.getKey());
				if (state != null)
					score += state.priority;
			}
			if (!deferredReadRequest.getEntries().isEmpty())
				score /= deferredReadRequest.getEntries().size();
			if (!deferredReadRequest.getEntries().isEmpty())
				score += ((double) readyCount) / deferredReadRequest.getEntries().size();
			deferredReadRequest.setPriorityScore(score);
			_deferredReadRequests.add(deferredReadRequest);
			_deferredReadCountHint = _deferredReadRequests.size();
		}
		onCacheSizeChanged(true);  // Apply pressure from deferred read demand.
		onCacheSizeChanged(false); // Attempt to schedule deferred reads.
	}

	@Override
	public BlockKey put(BlockKey key, Object data, long size) {
		return put(key, data, size, false, null).getKey();
	}

	@Override
	public BlockEntry putAndPin(BlockKey key, Object data, long size) {
		return put(key, data, size, true, null);
	}

	@Override
	public HandoverHandle handover(BlockKey key, InMemoryQueueCallback callback) {
		return registerHandover(key, callback, 0);
	}

	@Override
	public OOCStream.QueueCallback<IndexedMatrixValue> handoverAndPin(BlockKey key, InMemoryQueueCallback callback) {
		PendingHandover handover = registerHandover(key, callback, 1);
		return new OOCCacheManager.HandoverCachedQueueCallback<>(handover, null);
	}

	@Override
	public void putSourceBacked(BlockKey key, Object data, long size, OOCIOHandler.SourceBlockDescriptor descriptor) {
		put(key, data, size, false, descriptor);
	}

	@Override
	public BlockEntry putAndPinSourceBacked(BlockKey key, Object data, long size, OOCIOHandler.SourceBlockDescriptor descriptor) {
		return put(key, data, size, true, descriptor);
	}

	@Override
	public void addReference(BlockKey key) {
		synchronized(this) {
			BlockEntry entry = findEntry(key);
			if(entry == null)
				throw new IllegalArgumentException("Could not find requested block with key " + key);
			entry.addReference();
		}
	}

	private PendingHandover registerHandover(BlockKey key, InMemoryQueueCallback callback, int callbackRefs) {
		if(!this._running)
			throw new IllegalStateException("Cache scheduler has been shut down.");
		if(callback == null)
			throw new IllegalArgumentException("Cannot hand over a null callback.");
		Statistics.incrementOOCEvictionPut();
		PendingHandover handover = new PendingHandover(this, key, callback, callbackRefs);
		BlockEntry entry = new BlockEntry(key, callback.getManagedBytes(), handover, BlockState.HANDOVER_PENDING);
		handover.attachEntry(entry);
		boolean immediateCommit;
		synchronized(this) {
			if(findEntry(key) != null)
				throw new IllegalStateException("Cannot overwrite existing entries: " + key);
			putEntry(entry, true);
			immediateCommit = canAcceptHandoverLocked(callback.getManagedBytes());
			if(!immediateCommit)
				_pendingHandovers.addLast(handover);
		}
		if(immediateCommit) {
			if(commitHandover(handover))
				onCacheSizeChanged(true);
		}
		else {
			onCacheSizeChanged(true);
		}
		return handover;
	}

	private BlockEntry put(BlockKey key, Object data, long size, boolean pin, OOCIOHandler.SourceBlockDescriptor descriptor) {
		if (!this._running)
			throw new IllegalStateException();
		if (data == null)
			throw new IllegalArgumentException();
		if (descriptor != null)
			_ioHandler.registerSourceLocation(key, descriptor);

		Statistics.incrementOOCEvictionPut();
		BlockEntry entry = new BlockEntry(key, size, data);
		if (descriptor != null)
			entry.setState(BlockState.WARM);
		if (pin)
			entry.pin();
		synchronized(this) {
			if(findEntry(key) != null)
				throw new IllegalStateException("Cannot overwrite existing entries: " + key);
			putEntry(entry, true);
			_cacheSize += size;
			if(pin) {
				_pinnedBytes += size;
				if(entry.getState() == BlockState.WARM)
					_warmPinnedBytes += entry.getSize();
			}
		}
		onCacheSizeChanged(true);
		return entry;
	}

	@Override
	public void forget(BlockKey key) {
		if(!this._running)
			return;
		BlockEntry entry;
		boolean shouldScheduleDeletion = false;
		long cacheSizeDelta = 0;
		PendingHandover pendingHandover = null;
		synchronized(this) {
			entry = findEntry(key);
			if(entry != null && (entry.forget() != 0 || entry.getState() == BlockState.HANDOVER_PENDING ||
				entry.isPinned() || entry.isBackedPinned()))
				entry = null;

			if(entry != null) {
				BlockEntry removed = removeEntry(key);
				if(removed != entry)
					throw new IllegalStateException();
				synchronized(entry) {
					if(entry.getState() == BlockState.HANDOVER_PENDING)
						pendingHandover = (PendingHandover) entry.getDataUnsafe();
					shouldScheduleDeletion =
						entry.getState().isBackedByDisk() || entry.getState() == BlockState.EVICTING;
					cacheSizeDelta = transitionMemState(entry, BlockState.REMOVED);
					entry.setDataUnsafe(null);
				}
			}
		}
		if(cacheSizeDelta != 0)
			onCacheSizeChanged(cacheSizeDelta > 0);
		if(shouldScheduleDeletion)
			_ioHandler.scheduleDeletion(entry);
		if(pendingHandover != null) {
			OOCStream.QueueCallback<IndexedMatrixValue> callback = pendingHandover.reclaim();
			if(callback != null)
				callback.close();
		}
	}

	@Override
	public void pin(BlockEntry entry) {
		if(!this._running)
			throw new IllegalStateException("Cache scheduler has been shut down.");
		if(entry.fastPin())
			return; // Try to avoid using global lock first

		synchronized(this) {
			synchronized(entry) {
				int pinCount = pinEntryWithAccounting(entry);
				if (pinCount == 0)
					throw new IllegalStateException("Could not pin the requested entry: " + entry.getKey());
			}
		}
	}

	@Override
	public void unpin(BlockEntry entry) {
		if(entry.fastUnpin())
			return; // Try to avoid using global lock first
		long cacheSizeDelta = 0;
		boolean shouldCheckEviction = false;
		boolean shouldScheduleDeletion = false;
		synchronized(this) {
			synchronized(entry) {
				if(!unpinEntryWithAccounting(entry))
					return;
				if (entry.isPinned())
					return; // Pin state changed so we cannot evict
				if(entry.getReferenceCount() == 0 && !entry.isBackedPinned() &&
					entry.getState() != BlockState.HANDOVER_PENDING) {
					shouldScheduleDeletion =
						entry.getState().isBackedByDisk() || entry.getState() == BlockState.EVICTING;
					cacheSizeDelta = transitionMemState(entry, BlockState.REMOVED);
					entry.setDataUnsafe(null);
					BlockEntry tmp = removeEntry(entry.getKey());
					if(tmp != null && tmp != entry)
						throw new IllegalStateException();
				}
				else if (_cacheSize <= _evictionLimit) {
					return; // Nothing to do
				}
				else if(entry.getReferenceCount() != 0) {
					if (entry.getState().isAvailable() && entry.getState().isBackedByDisk()) {
						if (entry.getRetainHintCount() > 0) {
							shouldCheckEviction = true;
						}
						else {
							cacheSizeDelta =  transitionMemState(entry, BlockState.COLD);
							long cleared = entry.clear();
							if (cleared != entry.getSize())
								throw new IllegalStateException();
							clearLive(entry);
						}
					}
					else if (entry.getState() == BlockState.HOT) {
						if (entry.getRetainHintCount() > 0) {
							shouldCheckEviction = true;
						}
						else {
							cacheSizeDelta = onUnpinnedHotBlockUnderMemoryPressure(entry);
						}
					}
				}
			}
		}
		if (cacheSizeDelta != 0)
			onCacheSizeChanged(cacheSizeDelta > 0);
		else if (shouldCheckEviction)
			onCacheSizeChanged(true);
		if(shouldScheduleDeletion)
			_ioHandler.scheduleDeletion(entry);
	}

	@Override
	public AllowanceBackedPin pinBacked(BlockEntry entry, MemoryAllowance backingAllowance, long logicalBytes) {
		validateBackedPinArgs(entry, backingAllowance, logicalBytes);
		boolean pinned = false;
		try {
			pin(entry);
			pinned = true;
			addBackedPinWithAccounting(entry);
			return new AllowanceBackedPinImpl(this, entry, backingAllowance, logicalBytes);
		}
		catch(RuntimeException ex) {
			if(pinned) {
				try {
					unpin(entry);
				}
				catch(RuntimeException ignored) {
					// The original failure owns the error path.
				}
			}
			backingAllowance.release(logicalBytes);
			throw ex;
		}
	}

	@Override
	public AllowanceBackedPin adoptPinnedBacked(BlockEntry entry, MemoryAllowance backingAllowance, long logicalBytes) {
		validateBackedPinArgs(entry, backingAllowance, logicalBytes);
		if(!entry.isPinned()) {
			backingAllowance.release(logicalBytes);
			throw new IllegalStateException("Cannot adopt an unpinned entry: " + entry.getKey());
		}
		try {
			addBackedPinWithAccounting(entry);
			return new AllowanceBackedPinImpl(this, entry, backingAllowance, logicalBytes);
		}
		catch(RuntimeException ex) {
			backingAllowance.release(logicalBytes);
			throw ex;
		}
	}

	@Override
	public CompletableFuture<AllowanceBackedPin> requestBacked(BlockKey key, MemoryAllowance backingAllowance,
		long logicalBytes) {
		validateBackedPinArgs(key, backingAllowance, logicalBytes);
		CompletableFuture<AllowanceBackedPin> out = new CompletableFuture<>();
		try {
			request(key).whenComplete((entry, ex) -> {
				if(ex != null) {
					backingAllowance.release(logicalBytes);
					out.completeExceptionally(ex);
					return;
				}
				if(out.isCancelled()) {
					releaseRequestedEntry(entry);
					backingAllowance.release(logicalBytes);
					return;
				}
				try {
					PendingHandover pending = null;
					synchronized(entry) {
						if(entry.getState() == BlockState.HANDOVER_PENDING)
							pending = (PendingHandover) entry.getDataUnsafe();
					}
					if(pending != null) {
						releaseRequestedEntry(entry);
						pending.getCompletionFuture().whenComplete((committed, pendingEx) -> {
							if(pendingEx != null) {
								backingAllowance.release(logicalBytes);
								out.completeExceptionally(pendingEx);
							}
							else if(!Boolean.TRUE.equals(committed)) {
								backingAllowance.release(logicalBytes);
								out.completeExceptionally(new IllegalStateException(
									"Pending handover was cancelled: " + key));
							}
							else {
								requestBacked(key, backingAllowance, logicalBytes)
									.whenComplete((pin, pinEx) -> {
										if(pinEx != null)
											out.completeExceptionally(pinEx);
										else if(!out.complete(pin))
											pin.close();
									});
							}
						});
						return;
					}
					AllowanceBackedPin pin = adoptPinnedBacked(entry, backingAllowance, logicalBytes);
					if(!out.complete(pin))
						pin.close();
				}
				catch(RuntimeException rex) {
					out.completeExceptionally(rex);
				}
			});
			return out;
		}
		catch(RuntimeException ex) {
			backingAllowance.release(logicalBytes);
			throw ex;
		}
	}

	@Override
	public AllowanceBackedPin tryRequestBacked(BlockKey key, MemoryAllowance backingAllowance, long logicalBytes) {
		validateBackedPinArgs(key, backingAllowance, logicalBytes);
		BlockEntry entry = tryRequest(key);
		if(entry == null)
			return null;
		synchronized(entry) {
			if(entry.getState() == BlockState.HANDOVER_PENDING) {
				releaseRequestedEntry(entry);
				return null;
			}
		}
		return adoptPinnedBacked(entry, backingAllowance, logicalBytes);
	}

	@Override
	public BackingReleaseHandle releaseBacking(AllowanceBackedPin pin) {
		if(!(pin instanceof AllowanceBackedPinImpl))
			throw new IllegalArgumentException("Backing release requires a pin created by this scheduler.");
		if(((AllowanceBackedPinImpl) pin)._handle._scheduler != this)
			throw new IllegalArgumentException("Backing release pin belongs to a different scheduler.");
		if(!this._running)
			throw new IllegalStateException("Cache scheduler has been shut down.");
		PendingBackingRelease release = new PendingBackingRelease(pin);
		boolean commit;
		synchronized(this) {
			commit = canAcceptBackingReleaseLocked(pin.getLogicalBytes());
			if(!commit)
				_pendingBackingReleases.addLast(release);
		}
		if(commit)
			release.commit();
		else
			onCacheSizeChanged(true);
		return release;
	}

	private void addBackedPinWithAccounting(BlockEntry entry) {
		synchronized(this) {
			synchronized(entry) {
				if(!entry.addBackedPin())
					return;
				addBackedStateContribution(entry);
			}
		}
		onCacheSizeChanged(false);
	}

	private void removeBackedPinWithAccounting(BlockEntry entry) {
		synchronized(this) {
			synchronized(entry) {
				if(!entry.removeBackedPin())
					return;
				removeBackedStateContribution(entry);
			}
		}
		// Releasing a backed pin lowers charged cache pressure, which is the signal
		// deferred reads need in order to retry admission.
		onCacheSizeChanged(false);
	}

	@Override
	public synchronized long getCacheSize() {
		return getChargedCacheSizeLocked();
	}

	@Override
	public synchronized long getPinnedBytes() {
		return _pinnedBytes;
	}

	@Override
	public synchronized long getHardLimit() {
		return _hardLimit;
	}

	@Override
	public boolean isWithinLimits() {
		return getChargedCacheSize() < _hardLimit;
	}

	@Override
	public boolean isWithinSoftLimits() {
		return getChargedCacheSize() < (_evictionLimit + _hardLimit) / 2;
	}

	@Override
	public synchronized void shutdown() {
		this._running = false;
		List<BlockEntry> cachedEntries = liveEntries();
		List<BlockEntry> entries = allEntries();
		if(!entries.isEmpty()) {
			int evictedSize = entries.size() - cachedEntries.size();
			System.out.println("[WARN] Cache still holds " + cachedEntries.size() + " / " + evictedSize + " blocks");

			Set<Long> streams = new HashSet<>();
			int pinned = 0;
			for(BlockEntry entry : entries) {
				streams.add(entry.getKey().getStreamId());
				if(entry.isPinned())
					pinned++;
			}
			System.out.println("[WARN] Affected stream IDs: " + streams + ", Pinned: " + pinned);
			if(OOCDebug.DUMP_CACHE_STATE) {
				Set<BlockEntry> cachedEntrySet = new HashSet<>(cachedEntries);
				System.out.print(CachingStream.dumpStreams(streams));
				cachedEntries.stream()
					.sorted((l, r) -> l.getKey().compareTo(r.getKey()))
					.forEach(e -> System.out.println("[WARN] Cache entry key=" + e.getKey()
						+ " state=" + e.getState()
						+ " refs=" + e.getReferenceCount()
						+ " retainHints=" + e.getRetainHintCount()
						+ " pins=" + e.getPinCount()
						+ " backedPins=" + e.getBackedPinCount()
						+ " hasData=" + (e.getDataUnsafe() != null)));
				entries.stream()
					.filter(e -> !cachedEntrySet.contains(e))
					.sorted((l, r) -> l.getKey().compareTo(r.getKey()))
					.forEach(e -> System.out.println("[WARN] Eviction entry key=" + e.getKey()
						+ " state=" + e.getState()
						+ " refs=" + e.getReferenceCount()
						+ " retainHints=" + e.getRetainHintCount()
						+ " pins=" + e.getPinCount()
						+ " backedPins=" + e.getBackedPinCount()
						+ " hasData=" + (e.getDataUnsafe() != null)));
			}
		}
		_blocks.clear();
		while(!_pendingHandovers.isEmpty()) {
			PendingHandover pending = _pendingHandovers.pollFirst();
			if(pending == null)
				continue;
			OOCStream.QueueCallback<IndexedMatrixValue> callback = pending.reclaim();
			if(callback != null)
				callback.close();
		}
		while(!_pendingBackingReleases.isEmpty()) {
			PendingBackingRelease pending = _pendingBackingReleases.pollFirst();
			if(pending == null)
				continue;
			AllowanceBackedPin pin = pending.reclaim();
			if(pin != null)
				pin.close();
		}
		_deferredReadRequests.clear();
		_deferredReadCountHint = 0;
		_blockReads.clear();
		_cacheSize = 0;
		_bytesUpForEviction = 0;
		_pinnedBytes = 0;
		_pinnedEvictingBytes = 0;
		_backedCacheBytes = 0;
		_backedEvictingBytes = 0;
		_backedWarmPinnedBytes = 0;
		_readingReservedBytes = 0;
		_warmPinnedBytes = 0;
	}

	@Override
	public synchronized void updateLimits(long evictionLimit, long hardLimit) {
		_evictionLimit = evictionLimit;
		_hardLimit = hardLimit;
	}

	@Override
	public synchronized Collection<BlockEntry> snapshot() {
		return allEntries();
	}

	private BlockEntry findEntry(BlockKey key) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.get(key.getStreamId());
		return stream == null ? null : stream.get(blockIndex(key));
	}

	private void putEntry(BlockEntry entry, boolean live) {
		BlockKey key = entry.getKey();
		MaskedOnceArrayList<BlockEntry> stream = _blocks.getOrCreate(key.getStreamId());
		int blockIndex = blockIndex(key);
		if(stream.get(blockIndex) != null)
			throw new IllegalStateException("Cannot overwrite existing entries: " + key);
		stream.put(blockIndex, entry);
		if(live)
			stream.setLive(blockIndex);
		else
			stream.clearLive(blockIndex);
	}

	private BlockEntry removeEntry(BlockKey key) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.get(key.getStreamId());
		if(stream == null)
			return null;
		int blockIndex = blockIndex(key);
		BlockEntry entry = stream.get(blockIndex);
		if(entry != null)
			stream.clear(blockIndex);
		return entry;
	}

	private void setLive(BlockEntry entry) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.getOrCreate(entry.getKey().getStreamId());
		int blockIndex = blockIndex(entry.getKey());
		BlockEntry current = stream.get(blockIndex);
		if(current == null)
			stream.put(blockIndex, entry);
		else if(current != entry)
			throw new IllegalStateException();
		stream.setLive(blockIndex);
	}

	private void clearLive(BlockEntry entry) {
		MaskedOnceArrayList<BlockEntry> stream = _blocks.get(entry.getKey().getStreamId());
		if(stream == null)
			throw new IllegalStateException();
		stream.clearLive(blockIndex(entry.getKey()));
	}

	private List<BlockEntry> liveEntries() {
		ArrayList<BlockEntry> entries = new ArrayList<>();
		_blocks.forEachLive(entries::add);
		return entries;
	}

	private List<BlockEntry> allEntries() {
		ArrayList<BlockEntry> entries = new ArrayList<>();
		_blocks.forEachVisible(entries::add);
		return entries;
	}

	private static int blockIndex(BlockKey key) {
		long sequenceNumber = key.getSequenceNumber();
		if(sequenceNumber < 0 || sequenceNumber > Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("Invalid block index: " + sequenceNumber);
		return (int) sequenceNumber;
	}

	private synchronized long getChargedCacheSize() {
		return getChargedCacheSizeLocked();
	}

	private synchronized long getChargedEvictingBytes() {
		return getChargedEvictingBytesLocked();
	}

	private long getChargedCacheSizeLocked() {
		return _cacheSize - _backedCacheBytes;
	}

	private long getChargedEvictingBytesLocked() {
		return _bytesUpForEviction - _backedEvictingBytes;
	}

	private long getChargedWarmPinnedBytesLocked() {
		return _warmPinnedBytes - _backedWarmPinnedBytes;
	}

	/**
	 * Must be called while this cache and the corresponding entry are locked
	 */
	private long onUnpinnedHotBlockUnderMemoryPressure(BlockEntry entry) {
		long cacheSizeDelta = transitionMemState(entry, BlockState.EVICTING);
		evict(entry, true);
		return cacheSizeDelta;
	}

	private void onCacheSizeChanged(boolean incr) {
		if(incr)
			_maintenanceNeedsIncr.set(true);
		else
			_maintenanceNeedsDecr.set(true);
		_maintenanceRequested.set(true);
		if(!_maintenanceRunning.compareAndSet(false, true))
			return;

		runMaintenanceLoop();
	}

	private void runMaintenanceLoop() {
		while(true) {
			try {
				do {
					_maintenanceRequested.set(false);
					onCacheSizeChangedInternal(
						_maintenanceNeedsIncr.getAndSet(false),
						_maintenanceNeedsDecr.getAndSet(false));
				} while(_maintenanceRequested.get());
			}
			finally {
				_maintenanceRunning.set(false);
			}

			// Re-check in case a request came in after releasing the running flag.
			if(!(_maintenanceRequested.get() && _maintenanceRunning.compareAndSet(false, true)))
				return;
		}
	}

	private void onCacheSizeChangedInternal(boolean incr, boolean decr) {
		if(incr)
			onCacheSizeIncremented();
		if(decr)
			while(onCacheSizeDecremented()) {}
		while(processPendingBackingReleases()) {
			onCacheSizeIncremented();
		}
		while(processPendingHandovers()) {
			onCacheSizeIncremented();
			while(processPendingBackingReleases()) {
				onCacheSizeIncremented();
			}
		}
		if(DMLScript.OOC_LOG_EVENTS)
			OOCEventLog.onCacheSizeChangedEvent(_callerId, System.nanoTime(), getChargedCacheSize(), getChargedEvictingBytes(),
				_pinnedBytes, _readingReservedBytes);
	}

	private synchronized void sanityCheck() {
		long chargedCacheSize = getChargedCacheSizeLocked();
		if (chargedCacheSize > _hardLimit * 1.1) {
			if (!_warnThrottling) {
				_warnThrottling = true;
				System.out.println("[WARN] Cache hard limit exceeded by over 10%: " + String.format("%.2f", chargedCacheSize/1000000.0) + "MB (-" + String.format("%.2f", getChargedEvictingBytesLocked()/1000000.0) + "MB) > " + String.format("%.2f", _hardLimit/1000000.0) + "MB");
			}
		}
		else if (_warnThrottling && chargedCacheSize < _hardLimit) {
			_warnThrottling = false;
			System.out.println("[INFO] Cache within limit: " + String.format("%.2f", chargedCacheSize/1000000.0) + "MB (-" + String.format("%.2f", getChargedEvictingBytesLocked()/1000000.0) + "MB) <= " + String.format("%.2f", _hardLimit/1000000.0) + "MB");
		}

		if (!SANITY_CHECKS)
			return;

		int pinned = 0;
		int backedByDisk = 0;
		int evicting = 0;
		int total = 0;
		long actualCacheSize = 0;
		long upForEviction = 0;
		long actualPinnedBytes = 0;
		long actualPinnedEvictingBytes = 0;
		long actualWarmPinnedBytes = 0;
		long actualBackedCacheBytes = 0;
		long actualBackedEvictingBytes = 0;
		long actualBackedWarmPinnedBytes = 0;
		long actualReadingReservedBytes = 0;
		for (BlockEntry entry : allEntries()) {
			if(entry.getState() == BlockState.HANDOVER_PENDING) {
				total++;
				continue;
			}
			if (entry.isPinned()) {
				pinned++;
				actualPinnedBytes += entry.getSize();
				if(entry.getState() == BlockState.WARM)
					actualWarmPinnedBytes += entry.getSize();
			}
			if (entry.getState().isBackedByDisk())
				backedByDisk++;
			if (entry.getState() == BlockState.EVICTING) {
				evicting++;
				upForEviction += entry.getSize();
				if(entry.isPinned())
					actualPinnedEvictingBytes += entry.getSize();
			}
			if(entry.getState() == BlockState.READING)
				actualReadingReservedBytes += entry.getSize();
			total++;
			switch(entry.getState()) {
				case HOT:
				case WARM:
				case EVICTING:
				case READING:
					actualCacheSize += entry.getSize();
					break;
				default:
					break;
			}
			if(entry.isBackedPinned()) {
				switch(entry.getState()) {
					case HOT:
					case WARM:
					case EVICTING:
					case READING:
						actualBackedCacheBytes += entry.getSize();
						break;
					case COLD:
					case REMOVED:
						break;
				}
				if(entry.getState() == BlockState.EVICTING)
					actualBackedEvictingBytes += entry.getSize();
				if(entry.getState() == BlockState.WARM && entry.isPinned())
					actualBackedWarmPinnedBytes += entry.getSize();
			}
		}
		if (actualCacheSize != _cacheSize)
			throw new IllegalStateException(actualCacheSize + " != " + _cacheSize);
		if (upForEviction != _bytesUpForEviction)
			throw new IllegalStateException(upForEviction + " != " + _bytesUpForEviction);
		if (actualPinnedBytes != _pinnedBytes)
			throw new IllegalStateException(actualPinnedBytes + " != " + _pinnedBytes);
		if (actualPinnedEvictingBytes != _pinnedEvictingBytes)
			throw new IllegalStateException(actualPinnedEvictingBytes + " != " + _pinnedEvictingBytes);
		if (_pinnedEvictingBytes > _bytesUpForEviction)
			throw new IllegalStateException(_pinnedEvictingBytes + " > " + _bytesUpForEviction);
		if(actualWarmPinnedBytes != _warmPinnedBytes)
			throw new IllegalStateException(actualWarmPinnedBytes + " != " + _warmPinnedBytes);
		if(actualBackedCacheBytes != _backedCacheBytes)
			throw new IllegalStateException(actualBackedCacheBytes + " != " + _backedCacheBytes);
		if(actualBackedEvictingBytes != _backedEvictingBytes)
			throw new IllegalStateException(actualBackedEvictingBytes + " != " + _backedEvictingBytes);
		if(actualBackedWarmPinnedBytes != _backedWarmPinnedBytes)
			throw new IllegalStateException(actualBackedWarmPinnedBytes + " != " + _backedWarmPinnedBytes);
		if (actualReadingReservedBytes != _readingReservedBytes)
			throw new IllegalStateException(actualReadingReservedBytes + " != " + _readingReservedBytes);
		System.out.println("==========");
		System.out.println("Limit: " + _evictionLimit/1000 + "KB");
		System.out.println("Memory: (" + getChargedCacheSizeLocked()/1000 + "KB - " + getChargedEvictingBytesLocked()/1000 + "KB) / " + _hardLimit/1000 + "KB");
		System.out.println("Pinned: " + pinned + " / " + total);
		System.out.println("Disk backed: " + backedByDisk + " / " + total);
		System.out.println("Evicting: " + evicting + " / " + total);
	}

	private void onCacheSizeIncremented() {
		if(System.currentTimeMillis() - _lastEvictRun < 5)
			return; // Debounce (at least 5ms) // TODO This can create stalls / deadlocks
		long cacheSizeDelta = 0;
		List<BlockEntry> upForEvictionNeedsWrite;
		List<BlockEntry> upForEvictionNoWrite;
		synchronized(this) {
			long pressure = getChargedCacheSizeLocked() + _readBuffer - getChargedEvictingBytesLocked() -
				getChargedWarmPinnedBytesLocked();
			if(pressure <= _evictionLimit)
				return; // Nothing to do

			long overshoot = Math.max((long)(0.1 * _evictionLimit), 10000000);
			long lowLimit = _evictionLimit - _readBuffer - overshoot;

			//System.out.println("[CACHE] Claiming " + (pressure + overshoot - _evictionLimit)/1000 + "kB (last claim was " + (System.currentTimeMillis() - _lastEvictRun) + "ms ago)");

			// Scan for values that can be evicted
			List<BlockEntry> entries = liveEntries();
			List<BlockEntry> toRemove = new ArrayList<>();
			upForEvictionNeedsWrite = new ArrayList<>();
			upForEvictionNoWrite = new ArrayList<>();

			for(int pass = 0; pass < 2; pass++) {
				boolean allowRetainHint = pass == 1;
				for(BlockEntry entry : entries) {
					if(getEvictionPressure() <= lowLimit)
						break;

					synchronized(entry) {
						//if(entry.isPinned())
						//	continue;
						if(!allowRetainHint && entry.getRetainHintCount() > 0)
							continue;
						if(entry.getState() == BlockState.COLD || entry.getState() == BlockState.EVICTING ||
							entry.getState() == BlockState.HANDOVER_PENDING)
							continue;

						if(entry.getState().isBackedByDisk() && !entry.isPinned()) {
							cacheSizeDelta += transitionMemState(entry, BlockState.COLD);
							entry.clear();
							toRemove.add(entry);
						}
						else {
							boolean needsWrite = !entry.getState().isBackedByDisk();
							cacheSizeDelta += transitionMemState(entry, BlockState.EVICTING);
							if(needsWrite)
								upForEvictionNeedsWrite.add(entry);
							else
								upForEvictionNoWrite.add(entry);
						}
					}
				}
				if(getEvictionPressure() <= lowLimit)
					break;
			}

			for(BlockEntry entry : toRemove) {
				clearLive(entry);
			}

			sanityCheck();
			_lastEvictRun = System.currentTimeMillis();
		}

		for (BlockEntry entry : upForEvictionNeedsWrite)
			evict(entry, true);
		for (BlockEntry entry : upForEvictionNoWrite)
			evict(entry, false);

		if (cacheSizeDelta != 0)
			onCacheSizeChanged(cacheSizeDelta > 0);
	}

	private long getEvictionPressure() {
		return getChargedCacheSizeLocked() + _readBuffer - getChargedEvictingBytesLocked();
	}

	private boolean processPendingHandovers() {
		List<PendingHandover> committed = new ArrayList<>();
		synchronized(this) {
			long admittedBytes = 0;
			while(!_pendingHandovers.isEmpty()) {
				PendingHandover pending = _pendingHandovers.peekFirst();
				if(pending == null)
					break;
				if(pending.isCancelled()) {
					_pendingHandovers.pollFirst();
					continue;
				}
				long bytes = pending.getManagedBytes();
				if(!canAcceptHandoverLocked(admittedBytes + bytes))
					break;
				admittedBytes += bytes;
				_pendingHandovers.pollFirst();
				committed.add(pending);
			}
		}
		boolean progress = false;
		for(PendingHandover pending : committed) {
			if(commitHandover(pending))
				progress = true;
		}
		return progress;
	}

	private boolean processPendingBackingReleases() {
		List<PendingBackingRelease> committed = new ArrayList<>();
		synchronized(this) {
			long admittedBytes = 0;
			while(!_pendingBackingReleases.isEmpty()) {
				PendingBackingRelease pending = _pendingBackingReleases.peekFirst();
				if(pending == null)
					break;
				if(pending.isCancelled()) {
					_pendingBackingReleases.pollFirst();
					continue;
				}
				long bytes = pending.getManagedBytes();
				if(!canAcceptBackingReleaseLocked(admittedBytes + bytes))
					break;
				admittedBytes += bytes;
				_pendingBackingReleases.pollFirst();
				committed.add(pending);
			}
		}
		boolean progress = false;
		for(PendingBackingRelease pending : committed) {
			if(pending.commit())
				progress = true;
		}
		return progress;
	}

	private boolean onCacheSizeDecremented() {
		if(getChargedCacheSize() + 10000000 >= _hardLimit || _deferredReadCountHint == 0)
			return false;
		boolean allReserved = true;
		boolean reading = false;
		List<Tuple2<Integer, BlockEntry>> toRead;
		DeferredReadRequest req;
		synchronized(this) {
			if(getChargedCacheSizeLocked() + 10000000 >= _hardLimit || _deferredReadRequests.isEmpty())
				return false; // Nothing to do

			// Try to schedule the next disk read
			req = _deferredReadRequests.peek();
			toRead = new ArrayList<>(req.getEntries().size());

			for(int idx = 0; idx < req.getEntries().size(); idx++) {
				if(!req.actionRequired(idx))
					continue;

				BlockEntry entry = req.getEntries().get(idx);
				synchronized(entry) {
					if(entry.getState().isAvailable()) {
						if(pinEntryWithAccounting(entry) == 0)
							throw new IllegalStateException();
						req.setPinned(idx);
					}
					else if(entry.getState() == BlockState.HANDOVER_PENDING) {
						PendingHandover pending = (PendingHandover) entry.getDataUnsafe();
						pending.retainForCallback();
						req.setPinned(idx);
					}
					else if (entry.getState() == BlockState.READING) {
						req.schedule(idx);
						registerWaiter(entry.getKey(), req, idx);
						reading = true;
					}
					else {
						if(getChargedCacheSizeLocked() + entry.getSize() <= _hardLimit) {
							transitionMemState(entry, BlockState.READING);
							toRead.add(new Tuple2<>(idx, entry));
							req.schedule(idx);
							registerWaiter(entry.getKey(), req, idx);
							reading = true;
						}
						else {
							allReserved = false;
						}
					}
				}
			}

			if(allReserved) {
				_deferredReadRequests.poll();
				_deferredReadCountHint = _deferredReadRequests.size();
			}

			sanityCheck();
		}

		if(allReserved && !reading) {
			clearRetainHints(req);
			req.getFuture().complete(req.getEntries());
			return true;
		}
		else if(allReserved && reading && req.isComplete()) {
			clearRetainHints(req);
			synchronized(this) {
				_deferredReadRequests.remove(req);
				_deferredReadCountHint = _deferredReadRequests.size();
			}
			req.getFuture().complete(req.getEntries());
			return true;
		}

		for(Tuple2<Integer, BlockEntry> tpl : toRead) {
			final BlockEntry entry = tpl._2;
			CompletableFuture<BlockEntry> future = _ioHandler.scheduleRead(entry);
			future.whenComplete((r, t) -> {
				if(t != null) {
					BlockReadState state;
					synchronized(OOCLRUCacheScheduler.this) {
						state = _blockReads.remove(entry.getKey());

					}
					if(state != null) {
						for(DeferredReadWaiter waiter : state.waiters)
							waiter.request.getFuture().completeExceptionally(t);
					}
					else {
						LOG.error("Uncaught CacheError", t);
					}
					onCacheSizeChanged(false);
					return;
				}
				Set<DeferredReadRequest> completedRequests = new HashSet<>();
				synchronized(this) {
					synchronized(r) {
						transitionMemState(r, BlockState.WARM);
						setLive(r);
					}

					BlockReadState state = _blockReads.remove(r.getKey());
					if(state != null) {
						for(DeferredReadWaiter waiter : state.waiters) {
							synchronized(r) {
								if(pinEntryWithAccounting(r) == 0)
									throw new IllegalStateException();
								if(waiter.request.setPinned(waiter.index) || waiter.request.isComplete())
									completedRequests.add(waiter.request);
							}
						}
					}

					for(DeferredReadRequest done : completedRequests) {
						clearRetainHints(done);
						_deferredReadRequests.remove(done);
					}
					_deferredReadCountHint = _deferredReadRequests.size();

					sanityCheck();
				}
				for(DeferredReadRequest done : completedRequests)
					done.getFuture().complete(done.getEntries());
				onCacheSizeChanged(false);
			});
		}

		return false;
	}

	private void evict(final BlockEntry entry, boolean needsWrite) {
		if(!needsWrite) {
			onEvicted(entry);
			return;
		}
		CompletableFuture<Void> future = _ioHandler.scheduleEviction(entry);
		future.whenComplete((r, e) -> onEvicted(entry));
	}

	private void onEvicted(final BlockEntry entry) {
		long cacheSizeDelta;
		synchronized(this) {
			synchronized(entry) {
				if(entry.getState() == BlockState.REMOVED)
					return;
				if(entry.isPinned()) {
					transitionMemState(entry, BlockState.WARM);
					return; // Then we cannot clear the data
				}
				cacheSizeDelta = transitionMemState(entry, BlockState.COLD);
				entry.clear();
			}
			BlockEntry tmp = findEntry(entry.getKey());
			if(tmp != null && tmp != entry)
				throw new IllegalStateException();
			clearLive(entry);
			sanityCheck();
		}
		if (cacheSizeDelta != 0)
			onCacheSizeChanged(cacheSizeDelta > 0);
	}

	private void clearRetainHints(DeferredReadRequest request) {
		for (int i = 0; i < request.getEntries().size(); i++) {
			if (!request.isRetainHinted(i))
				continue;
			BlockEntry entry = request.getEntries().get(i);
			synchronized(entry) {
				entry.removeRetainHint();
			}
		}
	}

	/**
	 * Cleanly transitions state of a BlockEntry and handles accounting.
	 * Requires both the scheduler object and the entry to be locked:
	 */
	private long transitionMemState(BlockEntry entry, BlockState newState) {
		BlockState oldState = entry.getState();
		if (oldState == newState)
			return 0;

		long sz = entry.getSize();
		long oldCacheSize = _cacheSize;
		boolean pinned = entry.isPinned();
		boolean backed = entry.isBackedPinned();

		// Remove old contribution
		switch (oldState) {
			case REMOVED:
				throw new IllegalStateException();
			case HOT:
				_cacheSize -= sz;
				break;
			case WARM:
				_cacheSize -= sz;
				if(pinned)
					_warmPinnedBytes -= entry.getSize();
				break;
			case EVICTING:
				_cacheSize -= sz;
				_bytesUpForEviction -= sz;
				break;
			case READING:
				_cacheSize -= sz;
				_readingReservedBytes -= sz;
				break;
			case HANDOVER_PENDING:
				break;
			case COLD:
				break;
		}

		// Add new contribution
		switch (newState) {
			case REMOVED:
			case COLD:
				break;
			case HOT:
				_cacheSize += sz;
				break;
			case WARM:
				_cacheSize += sz;
				if(pinned)
					_warmPinnedBytes += entry.getSize();
				break;
			case EVICTING:
				_cacheSize += sz;
				_bytesUpForEviction += sz;
				break;
			case READING:
				_cacheSize += sz;
				_readingReservedBytes += sz;
				break;
			case HANDOVER_PENDING:
				break;
		}

		if(oldState == BlockState.EVICTING && entry.isPinned())
			_pinnedEvictingBytes -= sz;
		if(newState == BlockState.EVICTING && entry.isPinned())
			_pinnedEvictingBytes += sz;
		if(backed) {
			removeBackedStateContribution(oldState, pinned, sz);
			addBackedStateContribution(newState, pinned, sz);
		}
		if(_pinnedEvictingBytes < 0)
			throw new IllegalStateException();
		if(_pinnedEvictingBytes > _bytesUpForEviction)
			throw new IllegalStateException(_pinnedEvictingBytes + " > " + _bytesUpForEviction);
		checkBackedAccounting();

		entry.setState(newState);
		return _cacheSize - oldCacheSize;
	}

	private void addBackedStateContribution(BlockEntry entry) {
		addBackedStateContribution(entry.getState(), entry.isPinned(), entry.getSize());
	}

	private void removeBackedStateContribution(BlockEntry entry) {
		removeBackedStateContribution(entry.getState(), entry.isPinned(), entry.getSize());
	}

	private void addBackedStateContribution(BlockState state, boolean pinned, long size) {
		switch(state) {
			case HOT:
			case WARM:
			case EVICTING:
			case READING:
				_backedCacheBytes += size;
				break;
			case HANDOVER_PENDING:
			case COLD:
			case REMOVED:
				break;
		}
		if(state == BlockState.EVICTING)
			_backedEvictingBytes += size;
		if(state == BlockState.WARM && pinned)
			_backedWarmPinnedBytes += size;
		checkBackedAccounting();
	}

	private void removeBackedStateContribution(BlockState state, boolean pinned, long size) {
		switch(state) {
			case HOT:
			case WARM:
			case EVICTING:
			case READING:
				_backedCacheBytes -= size;
				break;
			case HANDOVER_PENDING:
			case COLD:
			case REMOVED:
				break;
		}
		if(state == BlockState.EVICTING)
			_backedEvictingBytes -= size;
		if(state == BlockState.WARM && pinned)
			_backedWarmPinnedBytes -= size;
		checkBackedAccounting();
	}

	private void checkBackedAccounting() {
		if(_backedCacheBytes < 0 || _backedEvictingBytes < 0 || _backedWarmPinnedBytes < 0)
			throw new IllegalStateException();
		if(_backedCacheBytes > _cacheSize)
			throw new IllegalStateException(_backedCacheBytes + " > " + _cacheSize);
		if(_backedEvictingBytes > _bytesUpForEviction)
			throw new IllegalStateException(_backedEvictingBytes + " > " + _bytesUpForEviction);
		if(_backedWarmPinnedBytes > _warmPinnedBytes)
			throw new IllegalStateException(_backedWarmPinnedBytes + " > " + _warmPinnedBytes);
	}

	/**
	 * Requires scheduler lock.
	 */
	private int pinEntryWithAccounting(BlockEntry entry) {
		int pinCount = entry.pin();
		if(pinCount == 1) {
			_pinnedBytes += entry.getSize();
			switch(entry.getState()) {
				case EVICTING:
					_pinnedEvictingBytes += entry.getSize();
					break;
				case WARM:
					_warmPinnedBytes += entry.getSize();
					break;
			}
		}
		return pinCount;
	}

	/**
	 * Requires scheduler lock and entry lock.
	 * @return true if this call transitioned pin count to zero.
	 */
	private boolean unpinEntryWithAccounting(BlockEntry entry) {
		boolean couldFree = entry.unpin();
		// Second check (entry.getDataUnsafe()...) is needed for potential forget(...) calls
		if(couldFree && entry.getDataUnsafe() != null) {
			_pinnedBytes -= entry.getSize();
			switch(entry.getState()) {
				case EVICTING:
					_pinnedEvictingBytes -= entry.getSize();
					break;
				case WARM:
					_warmPinnedBytes -= entry.getSize();
					break;
			}
		}
		if(_pinnedBytes < 0)
			throw new IllegalStateException();
		if(_pinnedEvictingBytes < 0)
			throw new IllegalStateException();
		return couldFree;
	}

	private void registerWaiter(BlockKey key, DeferredReadRequest request, int index) {
		BlockReadState state = _blockReads.computeIfAbsent(key, k -> new BlockReadState());
		state.waiters.add(new DeferredReadWaiter(request, index));
	}

	private void releaseRequestedEntry(BlockEntry entry) {
		synchronized(entry) {
			if(entry.getState() == BlockState.HANDOVER_PENDING) {
				PendingHandover pending = (PendingHandover) entry.getDataUnsafe();
				pending.releaseForCallback();
				return;
			}
		}
		unpin(entry);
	}

	private boolean commitHandover(PendingHandover pending) {
		InMemoryQueueCallback callback = pending.takeForCommit();
		if(callback == null)
			return false;
		try {
			IndexedMatrixValue value = callback.takeManagedResultForHandover();
			BlockEntry entry = pending.getEntry();
			boolean installed = false;
			synchronized(this) {
				synchronized(entry) {
					synchronized(pending) {
						if(entry.getState() == BlockState.HANDOVER_PENDING && entry.getDataUnsafe() == pending) {
							entry.replaceDataUnsafe(pending, value);
							transitionMemState(entry, BlockState.HOT);
							int refs = pending.markCommittedLocked(entry);
							for(int i = 0; i < refs; i++) {
								if(pinEntryWithAccounting(entry) == 0)
									throw new IllegalStateException();
							}
							installed = true;
						}
					}
				}
			}
			if(!installed) {
				callback.releaseManagedMemory();
				callback.close();
				pending.markCancelled();
				return false;
			}
			callback.releaseManagedMemory();
			callback.close();
			pending.completeCommitted();
			return true;
		}
		catch(Throwable t) {
			pending.markCancelled();
			callback.releaseManagedMemory();
			callback.close();
			throw t;
		}
	}

	private boolean canAcceptHandoverLocked(long bytes) {
		return bytes >= 0 && getChargedCacheSizeLocked() + bytes <= _hardLimit;
	}

	private boolean canAcceptBackingReleaseLocked(long bytes) {
		return bytes >= 0 && getChargedCacheSizeLocked() + bytes <= _hardLimit;
	}

	private static void validateBackedPinArgs(BlockEntry entry, MemoryAllowance backingAllowance, long logicalBytes) {
		if(entry == null)
			throw new IllegalArgumentException("Cannot create allowance-backed pin for null entry.");
		validateBackedPinArgs(entry.getKey(), backingAllowance, logicalBytes);
	}

	private static void validateBackedPinArgs(BlockKey key, MemoryAllowance backingAllowance, long logicalBytes) {
		if(key == null)
			throw new IllegalArgumentException("Cannot create allowance-backed pin for null key.");
		if(backingAllowance == null)
			throw new IllegalArgumentException("Cannot create allowance-backed pin without backing allowance.");
		if(logicalBytes < 0)
			throw new IllegalArgumentException("Logical bytes must not be negative.");
	}

	private static void reserveBackedLogicalBytes(MemoryAllowance allowance, long logicalBytes) {
		if(allowance instanceof CachedAllowance cached)
			cached.admitBlocking(logicalBytes);
		else
			allowance.reserveBlocking(logicalBytes);
	}

	private static final class AllowanceBackedPinImpl implements AllowanceBackedPin {
		private final BackedPinHandle _handle;
		private boolean _closed;

		private AllowanceBackedPinImpl(OOCLRUCacheScheduler scheduler, BlockEntry entry, MemoryAllowance allowance,
			long logicalBytes) {
			this(new BackedPinHandle(scheduler, entry, allowance, logicalBytes));
		}

		private AllowanceBackedPinImpl(BackedPinHandle handle) {
			_handle = handle;
			_closed = false;
		}

		@Override
		public BlockKey getKey() {
			return _handle._entry.getKey();
		}

		@Override
		public BlockEntry getEntry() {
			return _handle._entry;
		}

		@Override
		public MemoryAllowance getBackingAllowance() {
			return _handle._allowance;
		}

		@Override
		public long getLogicalBytes() {
			return _handle._logicalBytes;
		}

		@Override
		public synchronized AllowanceBackedPin keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot retain closed allowance-backed pin.");
			_handle.retain();
			return new AllowanceBackedPinImpl(_handle);
		}

		@Override
		public void close() {
			synchronized(this) {
				if(_closed)
					return;
				_closed = true;
			}
			_handle.release();
		}
	}

	public static void noteBackedPinEscape(AllowanceBackedPin pin, String escape) {
		if(pin instanceof AllowanceBackedPinImpl impl)
			BackedPinHandle.noteEscape(impl._handle, escape);
	}

	public static boolean hasLiveBackedPins() {
		return OOCDebug.TRACK_LIVE_STATE && !BackedPinHandle.LIVE_BACKED_PINS.isEmpty();
	}

	public static String dumpLiveBackedPins() {
		if(!OOCDebug.TRACK_LIVE_STATE)
			return "Live backed pins tracking disabled\n";
		StringBuilder sb = new StringBuilder();
		sb.append("Live backed pins: ").append(BackedPinHandle.LIVE_BACKED_PINS.size()).append('\n');
		BackedPinHandle.LIVE_BACKED_PINS.entrySet().stream()
			.sorted((l, r) -> Integer.compare(System.identityHashCode(l.getKey()), System.identityHashCode(r.getKey())))
			.forEach(e -> {
				BackedPinHandle h = e.getKey();
				BackedPinDebugInfo d = e.getValue();
				sb.append("  pin=").append(System.identityHashCode(h))
					.append(" key=").append(h._entry.getKey())
					.append(" allow=").append(h._allowance == null ? "null" :
						h._allowance.getClass().getSimpleName() + "@" + System.identityHashCode(h._allowance))
					.append(" bytes=").append(h._logicalBytes)
					.append(" refs=").append(h._refCount)
					.append(" released=").append(h._released)
					.append(" origin=").append(d._origin)
					.append(" lastEscape=").append(d._lastEscape)
					.append('\n');
			});
		return sb.toString();
	}

	private static final class BackedPinDebugInfo {
		private final String _origin;
		private volatile String _lastEscape = "unrecorded";

		private BackedPinDebugInfo(String origin) {
			_origin = origin;
		}
	}

	private static final class BackedPinHandle {
		private static final ConcurrentHashMap<BackedPinHandle, BackedPinDebugInfo> LIVE_BACKED_PINS =
			new ConcurrentHashMap<>();
		private final OOCLRUCacheScheduler _scheduler;
		private final BlockEntry _entry;
		private final MemoryAllowance _allowance;
		private final long _logicalBytes;
		private int _refCount;
		private boolean _released;

		private BackedPinHandle(OOCLRUCacheScheduler scheduler, BlockEntry entry, MemoryAllowance allowance,
			long logicalBytes) {
			_scheduler = scheduler;
			_entry = entry;
			_allowance = allowance;
			_logicalBytes = logicalBytes;
			_refCount = 1;
			_released = false;
			if(OOCDebug.TRACK_LIVE_STATE)
				LIVE_BACKED_PINS.put(this, new BackedPinDebugInfo(pinOrigin()));
		}

		private synchronized void retain() {
			if(_released)
				throw new IllegalStateException("Cannot retain released allowance-backed pin.");
			_refCount++;
		}

		private static void noteEscape(BackedPinHandle handle, String escape) {
			if(!OOCDebug.TRACK_LIVE_STATE)
				return;
			BackedPinDebugInfo info = LIVE_BACKED_PINS.get(handle);
			if(info != null)
				info._lastEscape = escape;
		}

		private static String pinOrigin() {
			StackTraceElement[] st = new Exception().getStackTrace();
			for(int i = 2; i < st.length; i++) {
				String cls = st[i].getClassName();
				if(!cls.equals(BackedPinHandle.class.getName()) && !cls.equals(AllowanceBackedPinImpl.class.getName()))
					return cls + ":" + st[i].getLineNumber();
			}
			return "unknown";
		}

		private void release() {
			boolean finalRelease;
			synchronized(this) {
				if(_released)
					return;
				_refCount--;
				if(_refCount < 0)
					throw new IllegalStateException();
				finalRelease = _refCount == 0;
				if(finalRelease)
					_released = true;
			}

			if(!finalRelease)
				return;
			if(OOCDebug.TRACK_LIVE_STATE)
				LIVE_BACKED_PINS.remove(this);

			RuntimeException releaseFailure = null;
			try {
				_scheduler.removeBackedPinWithAccounting(_entry);
			}
			catch(RuntimeException ex) {
				releaseFailure = ex;
			}
			try {
				_scheduler.unpin(_entry);
			}
			catch(RuntimeException ex) {
				if(releaseFailure == null)
					releaseFailure = ex;
			}
			finally {
				_allowance.release(_logicalBytes);
			}
			if(releaseFailure != null)
				throw releaseFailure;
		}
	}

	private static final class PendingBackingRelease implements BackingReleaseHandle {
		private final BlockKey _key;
		private final CompletableFuture<Boolean> _completionFuture;
		private AllowanceBackedPin _pin;
		private boolean _committed;
		private boolean _cancelled;

		private PendingBackingRelease(AllowanceBackedPin pin) {
			_key = pin.getKey();
			_completionFuture = new CompletableFuture<>();
			_pin = pin;
		}

		@Override
		public synchronized BlockKey getKey() {
			return _key;
		}

		@Override
		public synchronized boolean isCommitted() {
			return _committed;
		}

		@Override
		public synchronized CompletableFuture<Boolean> getCompletionFuture() {
			return _completionFuture;
		}

		@Override
		public synchronized AllowanceBackedPin reclaim() {
			if(_committed || _cancelled)
				return null;
			_cancelled = true;
			_completionFuture.complete(false);
			AllowanceBackedPin pin = _pin;
			_pin = null;
			return pin;
		}

		private synchronized long getManagedBytes() {
			return _pin == null ? 0 : _pin.getLogicalBytes();
		}

		private synchronized boolean isCancelled() {
			return _cancelled;
		}

		private boolean commit() {
			AllowanceBackedPin pin;
			synchronized(this) {
				if(_committed || _cancelled)
					return false;
				pin = _pin;
				_pin = null;
				_committed = true;
			}
			try {
				pin.close();
				_completionFuture.complete(true);
				return true;
			}
			catch(RuntimeException ex) {
				_completionFuture.completeExceptionally(ex);
				throw ex;
			}
		}
	}

	private static class BlockReadState {
		private double priority;
		private final List<DeferredReadWaiter> waiters;

		private BlockReadState() {
			this.priority = 0;
			this.waiters = new ArrayList<>();
		}
	}

	private static class DeferredReadWaiter {
		private final DeferredReadRequest request;
		private final int index;

		private DeferredReadWaiter(DeferredReadRequest request, int index) {
			this.request = request;
			this.index = index;
		}
	}

	private static class PendingHandover implements HandoverHandle {
		private final OOCLRUCacheScheduler _scheduler;
		private final BlockKey _key;
		private final CompletableFuture<Boolean> _completionFuture;
		private final long _bytes;
		private InMemoryQueueCallback _callback;
		private BlockEntry _entry;
		private int _callbackRefs;
		private boolean _committed;
		private boolean _cancelled;
		private boolean _committing;

		private PendingHandover(OOCLRUCacheScheduler scheduler, BlockKey key, InMemoryQueueCallback callback,
			int callbackRefs) {
			_scheduler = scheduler;
			_key = key;
			_completionFuture = new CompletableFuture<>();
			_bytes = callback.getManagedBytes();
			_callback = callback;
			_callbackRefs = callbackRefs;
		}

		private synchronized void attachEntry(BlockEntry entry) {
			_entry = entry;
		}

		@Override
		public synchronized BlockKey getKey() {
			return _key;
		}

		@Override
		public synchronized boolean isCommitted() {
			return _committed;
		}

		@Override
		public synchronized CompletableFuture<Boolean> getCompletionFuture() {
			return _completionFuture;
		}

		@Override
		public synchronized OOCStream.QueueCallback<IndexedMatrixValue> reclaim() {
			if(_committed || _committing)
				return null;
			_cancelled = true;
			_completionFuture.complete(false);
			OOCStream.QueueCallback<IndexedMatrixValue> callback = _callback;
			_callback = null;
			return callback;
		}

		@Override
		public synchronized long getManagedBytes() {
			if(_committed && _entry != null)
				return _entry.getSize();
			return _bytes;
		}

		private synchronized boolean isCancelled() {
			return _cancelled;
		}

		private synchronized BlockEntry getEntry() {
			return _entry;
		}

		private synchronized InMemoryQueueCallback takeForCommit() {
			if(_committed || _cancelled || _committing)
				return null;
			_committing = true;
			InMemoryQueueCallback callback = _callback;
			_callback = null;
			return callback;
		}

		private int markCommittedLocked(BlockEntry entry) {
			_committing = false;
			_committed = true;
			_entry = entry;
			return _callbackRefs;
		}

		private void completeCommitted() {
			_completionFuture.complete(true);
		}

		private synchronized void markCancelled() {
			if(_committed || _cancelled)
				return;
			_committing = false;
			_cancelled = true;
			_completionFuture.complete(false);
		}

		@Override
		public IndexedMatrixValue getCallbackData() {
			while(true) {
				CompletableFuture<Boolean> wait = null;
				synchronized(this) {
					if(_cancelled && _callbackRefs > 0) {
						return _callback.get();
					}
						//throw new IllegalStateException("Pending handover was cancelled: " + _key);
					if(_committed)
						return (IndexedMatrixValue) _entry.getData();
					if(!_committing && _callback != null)
						return _callback.get();
					wait = _completionFuture;
				}
				wait.join();
			}
		}

		@Override
		public BlockEntry retainForCallback() {
			synchronized(this) {
				if(_cancelled)
					throw new IllegalStateException("Pending handover was cancelled: " + _key);
				if(!_committed) {
					_callbackRefs++;
					return null;
				}
			}
			_scheduler.pin(_entry);
			return _entry;
		}

		@Override
		public void releaseForCallback() {
			BlockEntry entry = null;
			synchronized(this) {
				if(!_committed) {
					if(_callbackRefs <= 0)
						throw new IllegalStateException("Cannot release unopened pending handover callback.");
					_callbackRefs--;
					return;
				}
				entry = _entry;
			}
			_scheduler.unpin(entry);
		}

		@Override
		public synchronized BlockEntry getCommittedEntry() {
			if(!_committed)
				return null;
			return _entry;
		}

		@Override
		public AllowanceBackedPin transferToBacked(MemoryAllowance allowance) {
			InMemoryQueueCallback callback = null;
			BlockEntry entry;
			boolean pendingTransfer = false;
			CompletableFuture<Boolean> wait = null;

			synchronized(this) {
				if(_cancelled)
					throw new IllegalStateException("Pending handover was cancelled: " + _key);
				if(_committed) {
					entry = _entry;
				}
				else if(_committing) {
					wait = _completionFuture;
					entry = null;
				}
				else {
					if(_callbackRefs <= 0)
						throw new IllegalStateException("Cannot transfer unopened pending handover callback.");
					_callbackRefs--;
					_committing = true;
					callback = _callback;
					_callback = null;
					entry = _entry;
					pendingTransfer = true;
				}
			}

			if(wait != null) {
				wait.join();
				return transferToBacked(allowance);
			}

			if(!pendingTransfer) {
				reserveBackedLogicalBytes(allowance, entry.getSize());
				return _scheduler.adoptPinnedBacked(entry, allowance, entry.getSize());
			}

			long detachedBytes = 0;
			boolean installed = false;
			boolean reservationOwned = false;
			try {
				callback.transferOwnershipBlocking(allowance);
				detachedBytes = callback.detachManagedMemoryForHandover(allowance);
				reservationOwned = true;
				IndexedMatrixValue value = callback.takeManagedResultForHandover();

				synchronized(_scheduler) {
					synchronized(entry) {
						synchronized(this) {
							if(entry.getState() == BlockState.HANDOVER_PENDING && entry.getDataUnsafe() == this) {
								entry.replaceDataUnsafe(this, value);
								_scheduler.transitionMemState(entry, BlockState.HOT);
								for(int i = 0; i < _callbackRefs + 1; i++) {
									if(_scheduler.pinEntryWithAccounting(entry) == 0)
										throw new IllegalStateException();
								}
								_committing = false;
								_committed = true;
								installed = true;
							}
						}
					}
				}

				if(!installed)
					throw new IllegalStateException("Pending handover was already resolved: " + _key);

				try {
					reservationOwned = false; // adoptPinnedBacked owns release on success or failure.
					AllowanceBackedPin pin = _scheduler.adoptPinnedBacked(entry, allowance, detachedBytes);
					callback.close();
					_completionFuture.complete(true);
					return pin;
				}
				catch(RuntimeException ex) {
					_scheduler.unpin(entry);
					throw ex;
				}
			}
			catch(RuntimeException ex) {
				if(reservationOwned && detachedBytes > 0)
					allowance.release(detachedBytes);
				callback.close();
				if(installed)
					_completionFuture.completeExceptionally(ex);
				else
					markCancelled();
				throw ex;
			}
		}
	}
}
