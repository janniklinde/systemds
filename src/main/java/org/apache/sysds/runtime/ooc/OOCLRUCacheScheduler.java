package org.apache.sysds.runtime.ooc;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.util.CommonThreadPool;
import scala.Tuple2;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class OOCLRUCacheScheduler implements OOCCacheScheduler {
	private final IOHandlerOOC _ioHandler;
	private final LinkedHashMap<BlockKey, BlockEntry> _cache;
	private final HashMap<BlockKey, BlockEntry> _evictionCache;
	private final Deque<DeferredReadRequest> _deferredReadRequests;
	private final long _hardLimit;
	private final long _evictionLimit;
	private long _cacheSize;
	private long _bytesUpForEviction;

	public OOCLRUCacheScheduler(IOHandlerOOC ioHandler, long hardLimit, long evictionLimit) {
		this._ioHandler = ioHandler;
		this._cache = new LinkedHashMap<>(1024, 0.75f, true);
		this._evictionCache = new  HashMap<>();
		this._deferredReadRequests = new ArrayDeque<>();
		this._hardLimit = hardLimit;
		this._evictionLimit = evictionLimit;
		this._cacheSize = 0;
		this._bytesUpForEviction = 0;
	}

	@Override
	public CompletableFuture<BlockEntry> request(BlockKey key) {
		BlockEntry entry;
		boolean couldPin = false;
		synchronized(this) {
			entry = _cache.get(key);
			if (entry != null) {
				if (entry.pin() == 0)
					throw new IllegalStateException();
				couldPin = true;
			} else {
				entry = _evictionCache.get(key);
			}

			if (entry == null)
				throw new IllegalArgumentException("Could not find requested block with key " + key);

			if (!couldPin) {
				synchronized(entry) {
					if (entry.getState().readScheduled()) {
						final CompletableFuture<BlockEntry> future = new CompletableFuture<>();
						entry.getAvailabilityCallback().whenComplete((r, t) -> {
							if (r.pin() == 0)
								throw new IllegalStateException();
							CommonThreadPool.get().submit(() -> future.complete(r));
						});
						return future;
					} else if(_cacheSize + entry.getSize() <= _hardLimit) {
						transitionMemState(entry, BlockState.READING);
						onCacheSizeChanged(true);
					}
					else { // If not enough memory available, defer the read request
						transitionMemState(entry, BlockState.DEFERRED_READ);
						final CompletableFuture<BlockEntry> future = new CompletableFuture<>();
						final CompletableFuture<List<BlockEntry>> requestFuture = new CompletableFuture<>();
						requestFuture.whenComplete((r, t) -> future.complete(r.get(0)));
						_deferredReadRequests.add(
							new DeferredReadRequest(requestFuture, Collections.singletonList(entry)));
						return future;
					}
				}
			}
		}
		if (couldPin)
			return CompletableFuture.completedFuture(entry);
		// Otherwise we have to load the entry from disk
		final CompletableFuture<BlockEntry> future = _ioHandler.scheduleRead(entry);
		final BlockEntry finalEntry = entry;
		return future.whenComplete((r, e) -> {
			synchronized(this) {
				BlockEntry tmp = _evictionCache.remove(key);
				if (tmp != finalEntry)
					throw new IllegalStateException();
				synchronized(finalEntry) {
					transitionMemState(finalEntry, BlockState.WARM);
					if (finalEntry.pin() == 0)
						throw new IllegalStateException();
				}
				tmp = _cache.put(key, finalEntry);
				if (tmp != null)
					throw new IllegalStateException();
			}
		});
	}

	@Override
	public CompletableFuture<List<BlockEntry>> request(List<BlockKey> keys) {
		return null;
	}

	@Override
	public void put(BlockKey key, Object data, long size) {
		if (data == null)
			throw new IllegalArgumentException();
		BlockEntry entry = new BlockEntry(key, size, data);
		synchronized(this) {
			BlockEntry avail = _cache.putIfAbsent(key, entry);
			if (avail != null)
				throw new IllegalStateException("Cannot overwrite existing entries");
			_cacheSize += size;
			onCacheSizeChanged(true);
		}
	}

	@Override
	public void forget(BlockKey key) {
		BlockEntry entry;
		boolean shouldScheduleDeletion = false;
		synchronized(this) {
			entry = _cache.remove(key);

			if (entry == null)
				entry = _evictionCache.remove(key);

			if (entry != null) {
				long cacheSizeDelta;
				synchronized(entry) {
					shouldScheduleDeletion = entry.getState().isBackedByDisk()
						|| entry.getState() == BlockState.EVICTING;
					cacheSizeDelta = transitionMemState(entry, BlockState.REMOVED);
				}
				if (cacheSizeDelta != 0)
					onCacheSizeChanged(cacheSizeDelta > 0);
			}
		}
		if (shouldScheduleDeletion)
			_ioHandler.scheduleDeletion(entry);
	}

	@Override
	public void pin(BlockEntry entry) {
		int pinCount = entry.pin();
		if (pinCount == 0)
			throw new IllegalStateException("Could not pin the requested entry: " + entry.getKey());
		synchronized(this) {
			// Access element in cache for Lru
			_cache.get(entry.getKey());
		}
	}

	@Override
	public void unpin(BlockEntry entry) {
		boolean couldFree = entry.unpin();

		if (couldFree) {
			synchronized(this) {
				if (_cacheSize <= _evictionLimit)
					return; // Nothing to do
				long cacheSizeDelta = 0;

				synchronized(entry) {
					if (entry.isPinned())
						return; // Pin state changed so we cannot evict

					if (entry.getState().isAvailable() && entry.getState().isBackedByDisk()) {
						cacheSizeDelta =  transitionMemState(entry, BlockState.COLD);
						long cleared = entry.clear();
						if (cleared != entry.getSize())
							throw new IllegalStateException();
						_cache.remove(entry.getKey());
						_evictionCache.put(entry.getKey(), entry);
					} else if (entry.getState() == BlockState.HOT) {
						cacheSizeDelta = onUnpinnedHotBlockUnderMemoryPressure(entry);
					}
				}

				if (cacheSizeDelta != 0)
					onCacheSizeChanged(cacheSizeDelta > 0);
			}
		}
	}

	/**
	 * Must be called while this cache and the corresponding entry are locked
	 */
	private long onUnpinnedHotBlockUnderMemoryPressure(BlockEntry entry) {
		long cacheSizeDelta = transitionMemState(entry, BlockState.EVICTING);
		evict(entry);
		return cacheSizeDelta;
	}

	private synchronized void onCacheSizeChanged(boolean incr) {
		if (incr)
			onCacheSizeIncremented();
		else {
			while(onCacheSizeDecremented()) {}
		}
	}

	private synchronized void onCacheSizeIncremented() {
		if (_cacheSize - _bytesUpForEviction <= _evictionLimit)
			return; // Nothing to do

		// Scan for values that can be evicted
		Collection<BlockEntry> entries = _cache.values();
		List<BlockEntry> toRemove = new ArrayList<>();
		List<BlockEntry> upForEviction = new ArrayList<>();
		long cacheSizeDelta = 0;

		for (BlockEntry entry : entries) {
			if (_cacheSize - _bytesUpForEviction <= _evictionLimit)
				break;
			synchronized(entry) {
				if (entry.getState() == BlockState.WARM) {
					cacheSizeDelta += transitionMemState(entry, BlockState.COLD);
					long clearedBytes = entry.clear();
					if (clearedBytes == 0)
						throw new IllegalStateException();
					toRemove.add(entry);
				} else if (entry.getState() == BlockState.HOT) {
					cacheSizeDelta += transitionMemState(entry, BlockState.EVICTING);
					upForEviction.add(entry);
				}
			}
		}

		for (BlockEntry entry : toRemove) {
			_cache.remove(entry.getKey());
			_evictionCache.put(entry.getKey(), entry);
		}

		for (BlockEntry entry : upForEviction) {
			evict(entry);
		}

		if (cacheSizeDelta != 0)
			onCacheSizeChanged(cacheSizeDelta > 0);
	}

	private synchronized boolean onCacheSizeDecremented() {
		if (_cacheSize >= _hardLimit || _deferredReadRequests.isEmpty())
			return false; // Nothing to do

		// Try to schedule the next disk read
		DeferredReadRequest req = _deferredReadRequests.peek();
		List<Tuple2<Integer, BlockEntry>> toRead = new ArrayList<>(req.getEntries().size());
		boolean allReserved = true;

		for (int idx = 0; idx < req.getEntries().size(); idx++) {
			if (!req.actionRequired(idx))
				continue;

			BlockEntry entry = req.getEntries().get(idx);
			synchronized(entry) {
				if (entry.getState().isAvailable()) {
					if (entry.pin() == 0)
						throw new IllegalStateException();
					req.setPinned(idx);
				} else {
					if (_cacheSize + entry.getSize() <= _hardLimit) {
						transitionMemState(entry, BlockState.READING);
						toRead.add(new Tuple2<>(idx, entry));
						req.schedule(idx);
					} else {
						allReserved = false;
					}
				}
			}
		}

		if (allReserved && toRead.isEmpty()) {
			// Then the request is ready
			_deferredReadRequests.remove();
			// Run as separate task to avoid deadlocks
			CommonThreadPool.get().submit(() -> {
				req.getFuture().complete(req.getEntries());
			});
			return true;
		}

		for (Tuple2<Integer, BlockEntry> tpl : toRead) {
			final int idx = tpl._1;
			final BlockEntry entry = tpl._2;
			CompletableFuture<BlockEntry> future = _ioHandler.scheduleRead(entry);
			future.whenComplete((r, t) -> {
				boolean allAvailable;
				synchronized(this) {
					synchronized(r) {
						transitionMemState(r, BlockState.WARM);
						_evictionCache.remove(r.getKey());
						_cache.put(r.getKey(), r);
						allAvailable = req.setPinned(idx);
					}

					if (allAvailable) {
						_deferredReadRequests.remove();
					}
				}
				if (allAvailable) {
					// Run as separate task to avoid deadlocks
					CommonThreadPool.get().submit(() -> {
						req.getFuture().complete(req.getEntries());
					});
				}
			});
		}

		return false;
	}

	private synchronized boolean evict(final BlockEntry entry) {
		synchronized(entry) {
			if (entry.getState() != BlockState.EVICTING)
				throw new IllegalStateException();
		}
		CompletableFuture<Void> future = _ioHandler.scheduleEviction(entry);
		future.whenComplete((r, e) -> onEvicted(entry));
		return true;
	}

	private synchronized void onEvicted(final BlockEntry entry) {
		long clearedBytes;
		synchronized(entry) {
			if (entry.isPinned()) {
				transitionMemState(entry, BlockState.WARM);
				return; // Then we cannot clear the data
			}
			long cacheSizeDelta = transitionMemState(entry, BlockState.COLD);
			entry.clear();
		}
		BlockEntry tmp = _cache.remove(entry.getKey());
		if (tmp != null && tmp != entry)
			throw new IllegalStateException();
		tmp = _evictionCache.put(entry.getKey(), entry);
		if (tmp != null)
			throw new IllegalStateException();
		onCacheSizeChanged(false);
	}

	/**
	 * Cleanly transitions state of a BlockEntry and handles accounting.
	 * Requires both the scheduler object and the entry to be locked:
	 */
	private long transitionMemState(BlockEntry entry, BlockState newState)
			throws StateTransitionException {
		BlockState oldState = entry.getState();
		if (oldState == newState)
			return 0;

		long sz = entry.getSize();
		long oldCacheSize = _cacheSize;

		// Remove old contribution
		switch (oldState) {
			case REMOVED:
				throw new IllegalStateException();
			case HOT:
			case WARM:
				_cacheSize -= sz;
				break;
			case EVICTING:
				_cacheSize -= sz;
				_bytesUpForEviction -= sz;
				break;
			case READING:
				_cacheSize -= sz;
				break;
			case COLD:
			case DEFERRED_READ:
				break;
		}

		// Add new contribution
		switch (newState) {
			case REMOVED:
			case COLD:
			case DEFERRED_READ:
				break;
			case HOT:
			case WARM:
				_cacheSize += sz;
				break;
			case EVICTING:
				_cacheSize += sz;
				_bytesUpForEviction += sz;
				break;
			case READING:
				_cacheSize += sz;
				break;
		}

		entry.setState(newState);
		return _cacheSize - oldCacheSize;
	}



	private static class StateTransitionException extends DMLRuntimeException {
		public StateTransitionException(String string) {
			super(string);
		}
	}

	private static class DeferredReadRequest {
		private static short NOT_SCHEDULED = 0;
		private static short SCHEDULED = 1;
		private static short PINNED = 2;

		private final CompletableFuture<List<BlockEntry>> _future;
		private final List<BlockEntry> _entries;
		private final short[] _pinned;
		private AtomicInteger _availableCount;

		DeferredReadRequest(CompletableFuture<List<BlockEntry>> future, List<BlockEntry> entries) {
			this._future = future;
			this._entries = entries;
			this._pinned = new short[entries.size()];
			this._availableCount = new AtomicInteger(0);
		}

		CompletableFuture<List<BlockEntry>> getFuture() {
			return _future;
		}

		List<BlockEntry> getEntries() {
			return _entries;
		}

		public boolean actionRequired(int idx) {
			return _pinned[idx] == NOT_SCHEDULED;
		}

		public boolean setPinned(int idx) {
			if (_pinned[idx] == PINNED)
				return false; // already pinned
			_pinned[idx] = PINNED;
			return _availableCount.incrementAndGet() == _entries.size();
		}

		public void schedule(int idx) {
			_pinned[idx] = SCHEDULED;
		}
	}
}
