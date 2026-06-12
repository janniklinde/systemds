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

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.TeeOOCInstruction;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandlerTileStoreBackend;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.cache.io.TileStoreBackend;
import org.apache.sysds.runtime.ooc.cache.legacy.OOCCacheScheduler;
import org.apache.sysds.runtime.ooc.cache.legacy.OOCLRUCacheScheduler;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.utils.Statistics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OOCCacheManager {
	private static final double OOC_BUFFER_PERCENTAGE = 0.5;
	private static final double OOC_BUFFER_PERCENTAGE_HARD = 0.6;
	private static final long _evictionLimit;
	private static final long _hardLimit;

	private static final AtomicReference<OOCIOHandler> _ioHandler;
	private static final AtomicReference<OOCCacheScheduler> _scheduler;
	private static final AtomicReference<OOCPackedCache> _globalCache;
	private static final TileStoreBackend _tileStoreBackend;
	private static final ConcurrentHashMap<Object, BackedCallbackDebugInfo> LIVE_BACKED_CALLBACKS =
		new ConcurrentHashMap<>();

	static {
		_evictionLimit = (long)(Runtime.getRuntime().maxMemory() * OOC_BUFFER_PERCENTAGE);
		_hardLimit = (long)(Runtime.getRuntime().maxMemory() * OOC_BUFFER_PERCENTAGE_HARD);
		_ioHandler = new AtomicReference<>();
		_scheduler = new AtomicReference<>();
		_globalCache = new AtomicReference<>();
		_tileStoreBackend = new OOCIOHandlerTileStoreBackend();
	}

	public static void reset() {
		dumpOutstandingMemoryState("before-reset");
		TeeOOCInstruction.reset();
		OOCIOHandler ioHandler = _ioHandler.getAndSet(null);
		OOCCacheScheduler cacheScheduler = _scheduler.getAndSet(null);
		OOCPackedCache globalCache = _globalCache.getAndSet(null);
		if (ioHandler != null)
			ioHandler.shutdown();
		if (cacheScheduler != null)
			cacheScheduler.shutdown();
		if (globalCache != null)
			globalCache.shutdown();

		if (DMLScript.OOC_STATISTICS)
			Statistics.resetOOCEvictionStats();

		if (DMLScript.OOC_LOG_EVENTS) {
			try {
				String csv = OOCEventLog.getComputeEventsCSV();
				Files.writeString(Path.of(DMLScript.OOC_LOG_PATH, "ComputeEventLog.csv"), csv);
				csv = OOCEventLog.getDiskReadEventsCSV();
				Files.writeString(Path.of(DMLScript.OOC_LOG_PATH, "DiskReadEventLog.csv"), csv);
				csv = OOCEventLog.getDiskWriteEventsCSV();
				Files.writeString(Path.of(DMLScript.OOC_LOG_PATH, "DiskWriteEventLog.csv"), csv);
				csv = OOCEventLog.getCacheSizeEventsCSV();
				Files.writeString(Path.of(DMLScript.OOC_LOG_PATH, "CacheSizeEventLog.csv"), csv);
				csv = OOCEventLog.getRunSettingsCSV();
				Files.writeString(Path.of(DMLScript.OOC_LOG_PATH, "RunSettings.csv"), csv);
				System.out.println("Event logs written to: " + DMLScript.OOC_LOG_PATH);
			}
			catch(IOException e) {
				System.err.println("Could not write event logs: " + e.getMessage());
			}
			OOCEventLog.clear();
		}
		dumpOutstandingMemoryState("after-reset");
	}

	private static void dumpOutstandingMemoryState(String phase) {
		if(!OOCDebug.DUMP_ON_RESET)
			return;
		GlobalMemoryBroker broker = GlobalMemoryBroker.get();
		if(!broker.hasOutstandingUsage() && !InMemoryQueueCallback.hasLiveHandles() && !hasLiveBackedPins()
			&& !hasLiveBackedCallbacks())
			return;
		System.out.println("[WARN] Outstanding OOC memory state at OOCCacheManager.reset() phase=" + phase);
		System.out.print(broker.dumpOutstandingAllowances());
		System.out.print(InMemoryQueueCallback.dumpLiveHandles());
		System.out.print(dumpLiveBackedPins());
		System.out.print(dumpLiveBackedCallbacks());
	}

	public static OOCCacheScheduler getCache() {
		while (true) {
			OOCCacheScheduler scheduler = _scheduler.get();

			if(scheduler != null)
				return scheduler;

			OOCIOHandler ioHandler = new OOCMatrixIOHandler();
			scheduler = new OOCLRUCacheScheduler(ioHandler, _evictionLimit, _hardLimit, Math.max(40000000, (long)((_hardLimit - _evictionLimit) * 0.1)));

			if(_scheduler.compareAndSet(null, scheduler)) {
				_ioHandler.set(ioHandler);
				return scheduler;
			}
		}
	}

	/**
	 * Returns the current cache scheduler if already initialized, otherwise null.
	 * This method does not trigger lazy initialization.
	 */
	public static OOCCacheScheduler getCacheIfInitialized() {
		return _scheduler.get();
	}

	/**
	 * The global cache of the new OOC architecture ({@code OOCCache} pin/unpin/reference protocol over
	 * logical-to-physical packing). Migrated structures ({@code MaterializedStore},
	 * {@code OperatorStateTable}) share this one instance so eviction sees one population; the legacy
	 * {@link #getCache()} scheduler remains independent until the migration completes.
	 */
	public static OOCPackedCache getGlobalCache() {
		while(true) {
			OOCPackedCache cache = _globalCache.get();
			if(cache != null)
				return cache;
			cache = new OOCPackedCache(new OOCMatrixIOHandler(), _hardLimit, _evictionLimit);
			if(_globalCache.compareAndSet(null, cache))
				return cache;
			cache.shutdown();
		}
	}

	public static OOCIOHandler getIOHandler() {
		OOCIOHandler io = _ioHandler.get();
		if(io != null)
			return io;
		// Ensure initialization happens
		getCache();
		return _ioHandler.get();
	}

	public static TileStoreBackend getTileStoreBackend() {
		return _tileStoreBackend;
	}

	/**
	 * Removes a block from the cache without setting its data to null.
	 */
	public static void forget(long streamId, int blockId) {
		BlockKey key = new BlockKey(streamId, blockId);
		getCache().forget(key);
	}

	public static void forget(BlockKey key) {
		getCache().forget(key);
	}

	/**
	 * Store a block in the OOC cache (serialize once)
	 */
	public static void put(long streamId, int blockId, IndexedMatrixValue value) {
		BlockKey key = new BlockKey(streamId, blockId);
		getCache().put(key, value, ((MatrixBlock)value.getValue()).getExactSerializedSize());
	}

	/**
	 * Store a source-backed block in the OOC cache and register its source location.
	 */
	public static void putSourceBacked(long streamId, int blockId, IndexedMatrixValue value,
		OOCIOHandler.SourceBlockDescriptor descriptor) {
		BlockKey key = new BlockKey(streamId, blockId);
		getCache().putSourceBacked(key, value, ((MatrixBlock) value.getValue()).getExactSerializedSize(), descriptor);
	}

	public static void putRawSourceBacked(BlockKey key, Object data, long size, OOCIOHandler.SourceBlockDescriptor descriptor) {
		getCache().putSourceBacked(key, data, size, descriptor);
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> putAndPin(long streamId, int blockId, IndexedMatrixValue value) {
		BlockKey key = new BlockKey(streamId, blockId);
		return new CachedQueueCallback<>(getCache().putAndPin(key, value, ((MatrixBlock)value.getValue()).getExactSerializedSize()), null);
	}

	public static void putRaw(BlockKey key, Object data, long size) {
		getCache().put(key, data, size);
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> putAndPinRaw(BlockKey key, Object data, long size) {
		BlockEntry entry = getCache().putAndPin(key, data, size);
		if (data instanceof List)
			return new CachedGroupCallback<>(entry, null);
		return new CachedQueueCallback<>(entry, null);
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> putAndPinSourceBacked(long streamId, int blockId,
		IndexedMatrixValue value, OOCIOHandler.SourceBlockDescriptor descriptor) {
		BlockKey key = new BlockKey(streamId, blockId);
		return new CachedQueueCallback<>(
			getCache().putAndPinSourceBacked(key, value, ((MatrixBlock) value.getValue()).getExactSerializedSize(),
				descriptor), null);
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> putAndPinRawSourceBacked(BlockKey key, Object data, long size,
		OOCIOHandler.SourceBlockDescriptor descriptor) {
		BlockEntry entry = getCache().putAndPinSourceBacked(key, data, size, descriptor);
		if (data instanceof List)
			return new CachedGroupCallback<>(entry, null);
		return new CachedQueueCallback<>(entry, null);
	}

	public static void prioritize(BlockKey key, int priority) {
		getCache().prioritize(key, priority);
	}

	public static CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> requestBlock(long streamId, long blockId) {
		return requestBlock(new BlockKey(streamId, (int)blockId));
	}

	public static CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> requestBlock(BlockKey key) {
		return getCache().request(key).thenApply(e -> toCallback(e, key, null));
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> tryRequestBlock(long streamId, long blockId) {
		return tryRequestBlock(new BlockKey(streamId, (int) blockId));
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> tryRequestBlock(BlockKey key) {
		BlockEntry entry = getCache().tryRequest(key);
		return entry == null ? null : toCallback(entry, key, null);
	}

	public static CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> requestBlockBacked(BlockKey key,
		MemoryAllowance backingAllowance, long logicalBytes) {
		return getCache().requestBacked(key, backingAllowance, logicalBytes)
			.thenApply(pin -> {
				try {
					return toBackedCallback(pin, key, null);
				}
				catch(RuntimeException ex) {
					pin.close();
					throw ex;
				}
			});
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> tryRequestBlockBacked(BlockKey key,
		MemoryAllowance backingAllowance, long logicalBytes) {
		OOCCacheScheduler.AllowanceBackedPin pin = getCache().tryRequestBacked(key, backingAllowance, logicalBytes);
		if(pin == null)
			return null;
		try {
			return toBackedCallback(pin, key, null);
		}
		catch(RuntimeException ex) {
			pin.close();
			throw ex;
		}
	}

	public static CompletableFuture<List<OOCStream.QueueCallback<IndexedMatrixValue>>> requestManyBlocks(List<BlockKey> keys) {
		return getCache().request(keys).thenApply(
			l -> {
				List<OOCStream.QueueCallback<IndexedMatrixValue>> out = new ArrayList<>(l.size());
				for (int i = 0; i < l.size(); i++)
					out.add(toCallback(l.get(i), keys.get(i), null));
				return out;
			});
	}

	public static List<OOCStream.QueueCallback<IndexedMatrixValue>> tryRequestManyBlocks(List<BlockKey> keys) {
		List<BlockEntry> entries = getCache().tryRequest(keys);
		if(entries == null)
			return null;
		List<OOCStream.QueueCallback<IndexedMatrixValue>> out = new ArrayList<>(entries.size());
		for (int i = 0; i < entries.size(); i++)
			out.add(toCallback(entries.get(i), keys.get(i), null));
		return out;
	}

	public static CompletableFuture<List<OOCStream.QueueCallback<IndexedMatrixValue>>> requestAnyOf(List<BlockKey> keys, int n, List<BlockKey> sel) {
		return getCache().requestAnyOf(keys, n, sel)
			.thenApply(
				l -> {
					List<OOCStream.QueueCallback<IndexedMatrixValue>> out = new ArrayList<>(l.size());
					for (int i = 0; i < l.size(); i++) {
						BlockKey key = sel.size() == l.size() ? sel.get(i) : keys.get(i);
						out.add(toCallback(l.get(i), key, null));
					}
					return out;
				});
	}

	private static OOCStream.QueueCallback<IndexedMatrixValue> toCallback(BlockEntry entry, BlockKey key, DMLRuntimeException failure) {
		synchronized(entry) {
			if(entry.getState() == BlockState.HANDOVER_PENDING)
				return new HandoverCachedQueueCallback<>((OOCCacheScheduler.HandoverHandle) entry.getDataUnsafe(), failure);
		}
		if (entry.getData() instanceof List<?>) {
			CachedGroupCallback<IndexedMatrixValue> group = new CachedGroupCallback<>(entry, failure);
			if (key instanceof GroupedBlockKey gk) {
				OOCStream.QueueCallback<IndexedMatrixValue> sub = group.getCallback(gk.getGroupIndex());
				group.close(); // drop the group-level pin, sub keeps it pinned
				return sub;
			}
			return group;
		}
		return new CachedQueueCallback<>(entry, failure);
	}

	private static OOCStream.QueueCallback<IndexedMatrixValue> toBackedCallback(
		OOCCacheScheduler.AllowanceBackedPin pin, BlockKey key, DMLRuntimeException failure) {
		if(pin.getEntry().getData() instanceof List<?>) {
			BackedCachedGroupCallback<IndexedMatrixValue> group = new BackedCachedGroupCallback<>(pin, failure);
			if(key instanceof GroupedBlockKey gk) {
				OOCStream.QueueCallback<IndexedMatrixValue> sub = group.getCallback(gk.getGroupIndex());
				group.close(); // drop the group-level reference, sub keeps the backed lease alive
				return sub;
			}
			return group;
		}
		return new BackedCachedQueueCallback<>(pin, failure);
	}

	public static boolean canClaimMemory() {
		return getCache().isWithinLimits() && OOCInstruction.getComputeInFlight() <= OOCInstruction.getComputeBackpressureThreshold();
	}

	public static void noteBackedEscape(OOCStream.QueueCallback<?> cb, String escape) {
		OOCCacheScheduler.AllowanceBackedPin pin = cb == null ? null : cb.getBackingPin();
		if(pin != null)
			OOCLRUCacheScheduler.noteBackedPinEscape(pin, escape);
	}

	public static boolean hasLiveBackedPins() {
		return OOCLRUCacheScheduler.hasLiveBackedPins();
	}

	public static String dumpLiveBackedPins() {
		return OOCLRUCacheScheduler.dumpLiveBackedPins();
	}

	public static boolean hasLiveBackedCallbacks() {
		return OOCDebug.TRACK_LIVE_STATE && !LIVE_BACKED_CALLBACKS.isEmpty();
	}

	public static String dumpLiveBackedCallbacks() {
		if(!OOCDebug.TRACK_LIVE_STATE)
			return "Live backed callbacks tracking disabled\n";
		StringBuilder sb = new StringBuilder();
		sb.append("Live backed callbacks: ").append(LIVE_BACKED_CALLBACKS.size()).append('\n');
		LIVE_BACKED_CALLBACKS.entrySet().stream()
			.sorted((l, r) -> Integer.compare(System.identityHashCode(l.getKey()), System.identityHashCode(r.getKey())))
			.forEach(e -> {
				BackedCallbackDebugInfo d = e.getValue();
				sb.append("  cb=").append(System.identityHashCode(e.getKey()))
					.append(" type=").append(d._type)
					.append(" key=").append(d._key)
					.append(" origin=").append(d._origin)
					.append(" state=").append(d._state)
					.append('\n');
			});
		return sb.toString();
	}

	private static void registerBackedCallback(Object cb, String type, String key) {
		registerBackedCallback(cb, type, key, callbackOriginIfTracking());
	}

	private static void registerBackedCallback(Object cb, String type, String key, String origin) {
		if(!OOCDebug.TRACK_LIVE_STATE)
			return;
		LIVE_BACKED_CALLBACKS.put(cb, new BackedCallbackDebugInfo(type, key, origin));
	}

	private static void noteBackedCallbackState(Object cb, String state) {
		if(!OOCDebug.TRACK_LIVE_STATE)
			return;
		BackedCallbackDebugInfo info = LIVE_BACKED_CALLBACKS.get(cb);
		if(info != null)
			info._state = state;
	}

	private static void unregisterBackedCallback(Object cb) {
		if(!OOCDebug.TRACK_LIVE_STATE)
			return;
		LIVE_BACKED_CALLBACKS.remove(cb);
	}

	private static String callbackOrigin() {
		StackTraceElement[] st = new Exception().getStackTrace();
		for(int i = 2; i < st.length; i++) {
			String cls = st[i].getClassName();
			if(!cls.startsWith(OOCCacheManager.class.getName())
				&& !cls.startsWith(CompletableFuture.class.getName()))
				return cls + ":" + st[i].getLineNumber();
		}
		return "unknown";
	}

	private static String callbackOriginIfTracking() {
		return OOCDebug.TRACK_LIVE_STATE ? callbackOrigin() : null;
	}

	private static final class BackedCallbackDebugInfo {
		private final String _type;
		private final String _key;
		private final String _origin;
		private volatile String _state;

		private BackedCallbackDebugInfo(String type, String key, String origin) {
			_type = type;
			_key = key;
			_origin = origin;
			_state = "open";
		}
	}

	public static OOCCacheScheduler.HandoverHandle handover(BlockKey key, InMemoryQueueCallback callback) {
		return getCache().handover(key, callback);
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> handoverAndPin(BlockKey key,
		InMemoryQueueCallback callback) {
		return getCache().handoverAndPin(key, callback);
	}

	private static void pin(BlockEntry entry) {
		getCache().pin(entry);
	}

	private static void unpin(BlockEntry entry) {
		getCache().unpin(entry);
	}




	public static class BackedCachedQueueCallback<T> implements OOCStream.QueueCallback<T> {
		private OOCCacheScheduler.AllowanceBackedPin _pin;
		private final AtomicBoolean _pinned;
		private T _data;
		private DMLRuntimeException _failure;

		@SuppressWarnings("unchecked")
		BackedCachedQueueCallback(OOCCacheScheduler.AllowanceBackedPin pin, DMLRuntimeException failure) {
			this(pin, failure, callbackOriginIfTracking());
		}

		@SuppressWarnings("unchecked")
		BackedCachedQueueCallback(OOCCacheScheduler.AllowanceBackedPin pin, DMLRuntimeException failure, String origin) {
			_pin = pin;
			_data = (T)pin.getEntry().getData();
			_failure = failure;
			_pinned = new AtomicBoolean(true);
			registerBackedCallback(this, "BackedCachedQueueCallback", String.valueOf(pin.getKey()), origin);
		}

		@Override
		public T get() {
			if(_failure != null)
				throw _failure;
			if(!_pinned.get())
				throw new IllegalStateException("Cannot get cached item of a closed callback");
			return _data;
		}

		@Override
		public OOCStream.QueueCallback<T> keepOpen() {
			if(!_pinned.get())
				throw new IllegalStateException("Cannot keep open an already closed callback");
			String aliasOrigin = null;
			if(OOCDebug.TRACK_LIVE_STATE) {
				aliasOrigin = callbackOrigin();
				noteBackedCallbackState(this, "aliased-by@" + aliasOrigin);
			}
			return new BackedCachedQueueCallback<>(_pin.keepOpen(), _failure, aliasOrigin);
		}

		@Override
		public long getManagedBytes() {
			return _pin.getLogicalBytes();
		}

		@Override
		public OOCStream.QueueCallback<T> transferOwnershipBlocking(MemoryAllowance allowance) {
			if(!_pinned.get())
				throw new IllegalStateException("Cannot transfer ownership of an already closed callback");
			long bytes = _pin.getLogicalBytes();
			if(_pin.getBackingAllowance() == allowance)
				return this;
			if(allowance instanceof CachedAllowance cached)
				cached.admitBlocking(bytes);
			else
				allowance.reserveBlocking(bytes);
			OOCCacheScheduler.AllowanceBackedPin newPin = getCache().pinBacked(_pin.getEntry(), allowance, bytes);
			OOCCacheScheduler.AllowanceBackedPin oldPin = _pin;
			_pin = newPin;
			oldPin.close();
			return this;
		}

		@Override
		public OOCStream.QueueCallback<T> tryTransferOwnership(MemoryAllowance allowance) {
			if(!_pinned.get())
				throw new IllegalStateException("Cannot transfer ownership of an already closed callback");
			long bytes = _pin.getLogicalBytes();
			if(_pin.getBackingAllowance() == allowance)
				return this;
			if(allowance instanceof CachedAllowance)
				return null;
			if(!allowance.tryReserve(bytes))
				return null;
			OOCCacheScheduler.AllowanceBackedPin newPin = getCache().pinBacked(_pin.getEntry(), allowance, bytes);
			OOCCacheScheduler.AllowanceBackedPin oldPin = _pin;
			_pin = newPin;
			oldPin.close();
			return this;
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_failure = failure;
		}

		@Override
		public boolean isEos() {
			return get() == null;
		}

		@Override
		public boolean isFailure() {
			return _failure != null;
		}

		@Override
		public void close() {
			if(_pinned.compareAndSet(true, false)) {
				_data = null;
				_pin.close();
				unregisterBackedCallback(this);
			}
		}

		@Override
		public BlockKey getBlockKey() {
			return _pin.getKey();
		}

		@Override
		public OOCCacheScheduler.AllowanceBackedPin getBackingPin() {
			return _pin;
		}
	}

	public static class BackedCachedSubCallback<T> implements OOCStream.QueueCallback<T> {
		private final BackedCachedGroupCallback<T> _parent;
		private final AtomicBoolean _pinned;
		private T _data;
		private final int _groupIndex;

		BackedCachedSubCallback(BackedCachedGroupCallback<T> parent, T data, int groupIndex) {
			this(parent, data, groupIndex, callbackOriginIfTracking());
		}

		BackedCachedSubCallback(BackedCachedGroupCallback<T> parent, T data, int groupIndex, String origin) {
			_parent = parent;
			_data = data;
			_groupIndex = groupIndex;
			_pinned = new AtomicBoolean(true);
			registerBackedCallback(this, "BackedCachedSubCallback",
				parent.getBlockKey() + "#" + groupIndex, origin);
		}

		@Override
		public T get() {
			if(_parent.isFailure())
				throw _parent._failure;
			return _data;
		}

		@Override
		public OOCStream.QueueCallback<T> keepOpen() {
			_parent.registerQueueCallback();
			String aliasOrigin = null;
			if(OOCDebug.TRACK_LIVE_STATE) {
				aliasOrigin = callbackOrigin();
				noteBackedCallbackState(this, "aliased-by@" + aliasOrigin);
			}
			return new BackedCachedSubCallback<>(_parent, _data, _groupIndex, aliasOrigin);
		}

		@Override
		public void close() {
			if(_pinned.compareAndSet(true, false)) {
				_data = null;
				_parent.close();
				unregisterBackedCallback(this);
			}
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_parent.fail(failure);
		}

		@Override
		public boolean isEos() {
			return false;
		}

		@Override
		public boolean isFailure() {
			return _parent.isFailure();
		}

		public BackedCachedGroupCallback<T> getParent() {
			return _parent;
		}

		@Override
		public BlockKey getBlockKey() {
			return _parent.getBlockKey();
		}

		@Override
		public OOCCacheScheduler.AllowanceBackedPin getBackingPin() {
			return _parent.getBackingPin();
		}

		public int getGroupIndex() {
			return _groupIndex;
		}
	}

	public static class BackedCachedGroupCallback<T> implements OOCStream.GroupQueueCallback<T> {
		private final OOCCacheScheduler.AllowanceBackedPin _pin;
		private final AtomicInteger _pinCounter;
		private List<T> _data;
		private DMLRuntimeException _failure;

		@SuppressWarnings("unchecked")
		BackedCachedGroupCallback(OOCCacheScheduler.AllowanceBackedPin pin, DMLRuntimeException failure) {
			this(pin, failure, callbackOriginIfTracking());
		}

		@SuppressWarnings("unchecked")
		BackedCachedGroupCallback(OOCCacheScheduler.AllowanceBackedPin pin, DMLRuntimeException failure, String origin) {
			_pin = pin;
			_data = (List<T>)pin.getEntry().getData();
			_failure = failure;
			_pinCounter = new AtomicInteger(1);
			registerBackedCallback(this, "BackedCachedGroupCallback", String.valueOf(pin.getKey()), origin);
		}

		public OOCStream.QueueCallback<T> getCallback(int idx) {
			if(_pinCounter.get() <= 0)
				throw new IllegalStateException("Cannot open sub-callback on a closed GroupCallback");
			registerQueueCallback();
			String subOrigin = null;
			if(OOCDebug.TRACK_LIVE_STATE) {
				subOrigin = callbackOrigin();
				noteBackedCallbackState(this, "subcallback-opened@" + subOrigin);
			}
			return new BackedCachedSubCallback<>(this, _data.get(idx), idx, subOrigin);
		}

		public void registerQueueCallback() {
			if(_pinCounter.incrementAndGet() <= 1)
				throw new IllegalStateException();
		}

		@Override
		public T get() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int size() {
			return _data.size();
		}

		public T get(int idx) {
			return _data.get(idx);
		}

		@Override
		public OOCStream.QueueCallback<T> keepOpen() {
			if(_pinCounter.get() <= 0)
				throw new IllegalStateException("Cannot keep open an already closed callback");
			String aliasOrigin = null;
			if(OOCDebug.TRACK_LIVE_STATE) {
				aliasOrigin = callbackOrigin();
				noteBackedCallbackState(this, "aliased-by@" + aliasOrigin);
			}
			return new BackedCachedGroupCallback<>(_pin.keepOpen(), _failure, aliasOrigin);
		}

		@Override
		public void close() {
			int cnt = _pinCounter.decrementAndGet();
			if(cnt == 0) {
				_data = null;
				_pin.close();
				unregisterBackedCallback(this);
			}
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

		@Override
		public BlockKey getBlockKey() {
			return _pin.getKey();
		}

		@Override
		public OOCCacheScheduler.AllowanceBackedPin getBackingPin() {
			return _pin;
		}
	}

	public static class CachedQueueCallback<T> implements OOCStream.QueueCallback<T> {
		private final BlockEntry _result;
		private final AtomicBoolean _pinned;
		private T _data;
		private DMLRuntimeException _failure;

		@SuppressWarnings("unchecked")
		CachedQueueCallback(BlockEntry result, DMLRuntimeException failure) {
			this._result = result;
			this._data = (T)result.getData();
			if(_data == null)
				throw new IllegalArgumentException();
			this._failure = failure;
			this._pinned = new AtomicBoolean(true);
		}

		CachedQueueCallback(BlockEntry result, T data, DMLRuntimeException failure) {
			this._result = result;
			this._data = data;
			this._failure = failure;
			this._pinned = new AtomicBoolean(true);
		}

		@Override
		public T get() {
			if(_failure != null)
				throw _failure;
			if(!_pinned.get())
				throw new IllegalStateException("Cannot get cached item of a closed callback");
			return _data;
		}

		@Override
		public OOCStream.QueueCallback<T> keepOpen() {
			if(!_pinned.get())
				throw new IllegalStateException("Cannot keep open an already closed callback");
			pin(_result);
			return new CachedQueueCallback<>(_result, _data, _failure);
		}

		@Override
		public long getManagedBytes() {
			return _result.getSize();
		}

		@Override
		public OOCStream.QueueCallback<T> transferOwnershipBlocking(MemoryAllowance allowance) {
			long bytes = _result.getSize();
			if(allowance instanceof CachedAllowance cached)
				cached.admitBlocking(bytes);
			else
				allowance.reserveBlocking(bytes);
			if(!_pinned.compareAndSet(true, false)) {
				allowance.release(bytes);
				throw new IllegalStateException("Cannot transfer ownership of an already closed callback");
			}
			try {
				OOCCacheScheduler.AllowanceBackedPin pin = getCache().adoptPinnedBacked(_result, allowance, bytes);
				_data = null;
				return new BackedCachedQueueCallback<>(pin, _failure);
			}
			catch(RuntimeException ex) {
				unpin(_result);
				throw ex;
			}
		}

		@Override
		public OOCStream.QueueCallback<T> tryTransferOwnership(MemoryAllowance allowance) {
			if(!_pinned.get())
				throw new IllegalStateException("Cannot transfer ownership of an already closed callback");
			long bytes = _result.getSize();
			if(allowance instanceof CachedAllowance)
				return null;
			if(!allowance.tryReserve(bytes))
				return null;
			if(!_pinned.compareAndSet(true, false)) {
				allowance.release(bytes);
				throw new IllegalStateException("Cannot transfer ownership of an already closed callback");
			}
			try {
				OOCCacheScheduler.AllowanceBackedPin pin = getCache().adoptPinnedBacked(_result, allowance, bytes);
				_data = null;
				return new BackedCachedQueueCallback<>(pin, _failure);
			}
			catch(RuntimeException ex) {
				unpin(_result);
				throw ex;
			}
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			this._failure = failure;
		}

		@Override
		public boolean isEos() {
			return get() == null;
		}

		@Override
		public boolean isFailure() {
			return _failure != null;
		}

		@Override
		public void close() {
			if(_pinned.compareAndSet(true, false)) {
				_data = null;
				unpin(_result);
			}
		}

		@Override
		public BlockKey getBlockKey() {
			return _result.getKey();
		}
	}

	public static class HandoverCachedQueueCallback<T> implements OOCStream.QueueCallback<T> {
		private final OOCCacheScheduler.HandoverHandle _handover;
		private final AtomicBoolean _pinned;
		private DMLRuntimeException _failure;

		public HandoverCachedQueueCallback(OOCCacheScheduler.HandoverHandle handover, DMLRuntimeException failure) {
			_handover = handover;
			_failure = failure;
			_pinned = new AtomicBoolean(true);
		}

		@SuppressWarnings("unchecked")
		@Override
		public T get() {
			if(_failure != null)
				throw _failure;
			if(!_pinned.get())
				throw new IllegalStateException("Cannot get cached item of a closed callback");
			return (T) _handover.getCallbackData();
		}

		@Override
		public synchronized OOCStream.QueueCallback<T> keepOpen() {
			if(!_pinned.get())
				throw new IllegalStateException("Cannot keep open an already closed callback");
			BlockEntry entry = _handover.retainForCallback();
			if(entry != null)
				return new CachedQueueCallback<>(entry, _failure);
			return new HandoverCachedQueueCallback<>(_handover, _failure);
		}

		@Override
		public long getManagedBytes() {
			return _handover.getManagedBytes();
		}

		@Override
		public synchronized OOCStream.QueueCallback<T> transferOwnershipBlocking(MemoryAllowance allowance) {
			if(!_pinned.compareAndSet(true, false))
				throw new IllegalStateException("Cannot transfer ownership of an already closed callback");
			try {
				OOCCacheScheduler.AllowanceBackedPin pin = _handover.transferToBacked(allowance);
				return new BackedCachedQueueCallback<>(pin, _failure);
			}
			catch(RuntimeException ex) {
				throw ex;
			}
		}

		@Override
		public OOCStream.QueueCallback<T> tryTransferOwnership(MemoryAllowance allowance) {
			if(!_pinned.get())
				throw new IllegalStateException("Cannot transfer ownership of an already closed callback");
			if(!_handover.isCommitted())
				return null;
			BlockEntry entry = _handover.getCommittedEntry();
			long bytes = entry.getSize();
			if(allowance instanceof CachedAllowance)
				return null;
			if(!allowance.tryReserve(bytes))
				return null;
			if(!_pinned.compareAndSet(true, false)) {
				allowance.release(bytes);
				throw new IllegalStateException("Cannot transfer ownership of an already closed callback");
			}
			try {
				OOCCacheScheduler.AllowanceBackedPin pin = getCache().adoptPinnedBacked(entry, allowance, bytes);
				return new BackedCachedQueueCallback<>(pin, _failure);
			}
			catch(RuntimeException ex) {
				unpin(entry);
				throw ex;
			}
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_failure = failure;
		}

		@Override
		public boolean isEos() {
			return get() == null;
		}

		@Override
		public boolean isFailure() {
			return _failure != null;
		}

		@Override
		public synchronized void close() {
			if(_pinned.compareAndSet(true, false))
				_handover.releaseForCallback();
		}

		@Override
		public BlockKey getBlockKey() {
			return _handover.getKey();
		}
	}

	public static class CachedSubCallback<T> implements OOCStream.QueueCallback<T> {
		private final CachedGroupCallback<T> _parent;
		private final AtomicBoolean _pinned;
		private T _data;
		private final int _groupIndex;

		CachedSubCallback(CachedGroupCallback<T> parent, T data, int groupIndex) {
			_parent = parent;
			_data = data;
			_groupIndex = groupIndex;
			_pinned = new AtomicBoolean(true);
		}

		@Override
		public T get() {
			if(_parent.isFailure())
				throw _parent._failure;
			return _data;
		}

		@Override
		public OOCStream.QueueCallback<T> keepOpen() {
			_parent.registerQueueCallback();
			return new CachedSubCallback<>(_parent, _data, _groupIndex);
		}

		@Override
		public void close() {
			if(_pinned.compareAndSet(true, false)) {
				_data = null;
				_parent.close();
			}
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_parent.fail(failure);
		}

		@Override
		public boolean isEos() {
			return false;
		}

		@Override
		public boolean isFailure() {
			return _parent.isFailure();
		}

		public CachedGroupCallback<T> getParent() {
			return _parent;
		}

		public int getGroupIndex() {
			return _groupIndex;
		}
	}

	public static class CachedGroupCallback<T> implements OOCStream.GroupQueueCallback<T> {
		private final BlockEntry _result;
		private final AtomicInteger _pinCounter;
		private List<T> _data;
		private DMLRuntimeException _failure;

		@SuppressWarnings("unchecked")
		CachedGroupCallback(BlockEntry result, DMLRuntimeException failure) {
			this._result = result;
			this._data = (List<T>)result.getData();
			this._failure = failure;
			this._pinCounter = new AtomicInteger(1);
		}

		public OOCStream.QueueCallback<T> getCallback(int idx) {
			if(_pinCounter.get() <= 0)
				throw new IllegalStateException("Cannot open sub-callback on a closed GroupCallback");
			registerQueueCallback();
			return new CachedSubCallback<>(this, _data.get(idx), idx);
		}

		public void registerQueueCallback() {
			if(_pinCounter.incrementAndGet() <= 1)
				throw new IllegalStateException();
		}

		@Override
		public T get() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int size() {
			return _data.size();
		}

		public T get(int idx) {
			return _data.get(idx);
		}

		@Override
		public OOCStream.QueueCallback<T> keepOpen() {
			if(_pinCounter.get() <= 0)
				throw new IllegalStateException("Cannot keep open an already closed callback");
			pin(_result);
			return new CachedGroupCallback<>(_result, _failure);
		}

		@Override
		public void close() {
			int cnt = _pinCounter.decrementAndGet();
			if(cnt == 0) {
				_data = null;
				unpin(_result);
			}
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

		public BlockKey getBlockKey() {
			return _result.getKey();
		}
	}
}
