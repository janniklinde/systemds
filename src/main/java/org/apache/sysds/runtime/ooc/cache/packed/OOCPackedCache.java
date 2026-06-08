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

package org.apache.sysds.runtime.ooc.cache.packed;

import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.BlockState;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.collections.MaskedOnceArrayList;
import org.apache.sysds.runtime.ooc.cache.collections.SegmentedStreamTableList;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Logical-to-physical packing adapter. Small logical blocks are packed into larger physical cache entries
 * before being handed to OOCCacheImpl.
 */
public final class OOCPackedCache implements OOCCache {
	private static final int PACKED_STREAM_ID = 0;
	private static final long DEFAULT_PACK_THRESHOLD_BYTES = 64 * 1024;
	private static final long DEFAULT_PACK_TARGET_BYTES = 1L << 19; // 512 KB tile packing
	private static final long DEFAULT_MIXED_PACK_TARGET_BYTES = 256L << 10;
	private static final long DEFAULT_MAX_STAGING_BYTES = 64L << 20;
	private static final int DEFAULT_MAX_OPEN_BUILDERS = 64;
	private static final long DEFAULT_SEAL_DELAY_MS = 5;
	private static final long DEFAULT_PACK_RELEASE_DELAY_MS = 5;

	private final OOCCacheImpl _physical;
	private final long _packThresholdBytes;
	private final long _packTargetBytes;
	private final long _mixedPackTargetBytes;
	private final long _maxStagingBytes;
	private final int _maxOpenBuilders;
	private final long _sealDelayMs;
	private final long _packReleaseDelayMs;
	private final SegmentedStreamTableList<PackedCacheLocation> _locations;
	private final ScheduledExecutorService _sealExecutor;
	private final ExecutorService _releaseExecutor;
	private final ConcurrentLinkedQueue<PackedPinState> _releaseQueue;
	private final AtomicBoolean _releaseRunning;
	private final AtomicInteger _nextPackedId;

	private PackBuilder[] _builders;
	private PackBuilder _mixedBuilder;
	private long _stagingBytes;
	private int _openBuilderCount;
	private boolean _running;

	public OOCPackedCache(OOCIOHandler ioHandler, long hardLimit, long evictionLimit) {
		this(new OOCCacheImpl(ioHandler, hardLimit, evictionLimit), DEFAULT_PACK_THRESHOLD_BYTES,
			DEFAULT_PACK_TARGET_BYTES, DEFAULT_SEAL_DELAY_MS);
	}

	public OOCPackedCache(OOCCacheImpl physical) {
		this(physical, DEFAULT_PACK_THRESHOLD_BYTES, DEFAULT_PACK_TARGET_BYTES, DEFAULT_SEAL_DELAY_MS);
	}

	public OOCPackedCache(OOCCacheImpl physical, long packThresholdBytes, long packTargetBytes, long sealDelayMs) {
		this(physical, packThresholdBytes, packTargetBytes, sealDelayMs, DEFAULT_PACK_RELEASE_DELAY_MS);
	}

	public OOCPackedCache(OOCCacheImpl physical, long packThresholdBytes, long packTargetBytes, long sealDelayMs,
		long packReleaseDelayMs) {
		this(physical, packThresholdBytes, packTargetBytes, DEFAULT_MIXED_PACK_TARGET_BYTES,
			DEFAULT_MAX_STAGING_BYTES, DEFAULT_MAX_OPEN_BUILDERS, sealDelayMs, packReleaseDelayMs);
	}

	public OOCPackedCache(OOCCacheImpl physical, long packThresholdBytes, long packTargetBytes,
		long mixedPackTargetBytes, long maxStagingBytes, int maxOpenBuilders, long sealDelayMs,
		long packReleaseDelayMs) {
		if(packThresholdBytes <= 0 || packTargetBytes < packThresholdBytes)
			throw new IllegalArgumentException("Invalid pack sizes: threshold=" + packThresholdBytes +
				", target=" + packTargetBytes);
		_physical = physical;
		_packThresholdBytes = packThresholdBytes;
		_packTargetBytes = packTargetBytes;
		_mixedPackTargetBytes = Math.max(packThresholdBytes, mixedPackTargetBytes);
		_maxStagingBytes = Math.max(_mixedPackTargetBytes, maxStagingBytes);
		_maxOpenBuilders = Math.max(1, maxOpenBuilders);
		_sealDelayMs = sealDelayMs;
		_packReleaseDelayMs = packReleaseDelayMs;
		_locations = new SegmentedStreamTableList<>();
		_nextPackedId = new AtomicInteger();
		_releaseQueue = new ConcurrentLinkedQueue<>();
		_releaseRunning = new AtomicBoolean(false);
		_builders = new PackBuilder[16];
		_mixedBuilder = null;
		_stagingBytes = 0;
		_openBuilderCount = 0;
		_running = true;
		_sealExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "ooc-pack-sealer");
			t.setDaemon(true);
			return t;
		});
		_releaseExecutor = Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "ooc-pack-release");
			t.setDaemon(true);
			return t;
		});
	}

	@Override
	public BlockEntry putPinned(long sId, long tId, Object data, long size, MemoryAllowance allowance) {
		if(size >= _packThresholdBytes)
			return _physical.putPinned(sId, tId, data, size, allowance);

		PackBuilder builder;
		int slot;
		synchronized(this) {
			checkRunning();
			builder = getOpenBuilder(sId, allowance, size);
			slot = appendToBuilder(builder, sId, tId, data, size);
		}

		BlockEntry logical = new BlockEntry(new BlockKey(sId, tId), size, data, BlockState.REMOVED);
		logical.pin();
		logical.setCacheMeta(new PendingLogicalPin(builder, slot));
		return logical;
	}

	public BlockEntry[] putPackPinned(long sId, long[] tIds, Object[] data, long[] sizes,
		MemoryAllowance allowance) {
		return putPackPinned(sId, tIds, data, sizes, 0, tIds.length, allowance);
	}

	public BlockEntry[] putPackPinned(long sId, long[] tIds, Object[] data, long[] sizes, int off, int len,
		MemoryAllowance allowance) {
		BlockEntry[] entries = new BlockEntry[len];
		synchronized(this) {
			checkRunning();
			for(int i = 0; i < len; i++) {
				int p = off + i;
				long tId = tIds[p];
				long size = sizes[p];
				if(size >= _packThresholdBytes) {
					entries[i] = _physical.putPinned(sId, tId, data[p], size, allowance);
					continue;
				}
				PackBuilder builder = getOpenBuilder(sId, allowance, size);
				int slot = appendToBuilder(builder, sId, tId, data[p], size);
				BlockEntry logical = new BlockEntry(new BlockKey(sId, tId), size, data[p], BlockState.REMOVED);
				logical.pin();
				logical.setCacheMeta(new PendingLogicalPin(builder, slot));
				entries[i] = logical;
			}
		}
		return entries;
	}

	public BlockEntry putSealedPackPinned(long sId, long[] tIds, Object[] data, long[] sizes,
		MemoryAllowance allowance) {
		return putSealedPackPinned(sId, tIds, data, sizes, 0, tIds.length, allowance);
	}

	public BlockEntry putSealedPackPinned(long sId, long[] tIds, Object[] data, long[] sizes, int off, int len,
		MemoryAllowance allowance) {
		long totalSize = 0;
		Object[] packedData = new Object[len];
		long[] packedSizes = new long[len];
		for(int i = 0; i < len; i++) {
			int p = off + i;
			packedData[i] = data[p];
			packedSizes[i] = sizes[p];
			totalSize += sizes[p];
		}

		synchronized(this) {
			checkRunning();
			BlockEntry physicalEntry = putSealedBlockPinned(new PackedBlock(packedData, packedSizes, totalSize),
				allowance);
			PackedPinState state = new PackedPinState(physicalEntry);
			for(int i = 0; i < len; i++)
				putLocation(new BlockKey(sId, tIds[off + i]), new SealedPackLocation(state, i));
			return physicalEntry;
		}
	}

	@Override
	public OOCFuture<BlockEntry> pin(long sId, long tId, MemoryAllowance allowance) {
		PackedCacheLocation location = getLocation(sId, tId);
		if(location == null)
			return _physical.pin(sId, tId, allowance);
		if(location instanceof PendingPackLocation pending)
			location = forceSeal(pending);
		if(!(location instanceof SealedPackLocation packed))
			return _physical.pin(sId, tId, allowance);

		return packed.state.pin(_physical, allowance, false).map(physicalEntry -> {
			if(physicalEntry == null)
				return null;
			return createLogicalPin(new BlockKey(sId, tId), packed);
		});
	}

	@Override
	public BlockEntry pinIfLive(long sId, long tId, MemoryAllowance allowance) {
		PackedCacheLocation location = getLocation(sId, tId);
		if(location == null)
			return _physical.pinIfLive(sId, tId, allowance);
		if(location instanceof PendingPackLocation pending)
			location = forceSeal(pending);
		if(!(location instanceof SealedPackLocation packed))
			return _physical.pinIfLive(sId, tId, allowance);

		if(packed.state.pinIfLive(_physical, allowance) == null)
			return null;
		return createLogicalPin(new BlockKey(sId, tId), packed);
	}

	@Override
	public UnpinHandle unpin(BlockEntry entry, MemoryAllowance allowance) {
		Object meta = entry.getCacheMeta();
		if(meta instanceof PendingLogicalPin pending)
			return unpinPending(entry, pending, allowance);
		if(meta instanceof PackedLogicalPin packed)
			return unpinPacked(entry, packed, allowance);
		return _physical.unpin(entry, allowance);
	}

	@Override
	public int reference(BlockEntry entry) {
		Object meta = entry.getCacheMeta();
		if(meta instanceof PackedLogicalPin packed)
			return _physical.reference(packed.state.physicalEntry);
		if(meta instanceof PendingLogicalPin)
			return entry.addReference();
		return _physical.reference(entry);
	}

	@Override
	public int dereference(BlockEntry entry) {
		Object meta = entry.getCacheMeta();
		if(meta instanceof PackedLogicalPin packed)
			return _physical.dereference(packed.state.physicalEntry);
		if(meta instanceof PendingLogicalPin)
			return entry.forget();
		return _physical.dereference(entry);
	}

	@Override
	public void updateLimits(long hardLimit, long evictionLimit) {
		_physical.updateLimits(hardLimit, evictionLimit);
	}

	@Override
	public long getOwnedCacheSize() {
		return _physical.getOwnedCacheSize();
	}

	@Override
	public synchronized void shutdown() {
		if(!_running)
			return;
		_running = false;
		for(PackBuilder builder : _builders)
			if(builder != null)
				sealBuilder(builder);
		if(_mixedBuilder != null)
			sealBuilder(_mixedBuilder);
		_sealExecutor.shutdownNow();
		_releaseExecutor.shutdownNow();
		_physical.shutdown();
	}

	public synchronized void flushPacks() {
		for(PackBuilder builder : _builders)
			if(builder != null)
				sealBuilder(builder);
		if(_mixedBuilder != null)
			sealBuilder(_mixedBuilder);
	}

	private UnpinHandle unpinPending(BlockEntry entry, PendingLogicalPin pin, MemoryAllowance allowance) {
		if(entry.fastUnpin()) {
			allowance.release(entry.getSize());
			return ImmediatePackedUnpinHandle.committed(entry, allowance, entry.getSize());
		}
		synchronized(this) {
			if(entry.getPinCount() > 1) {
				entry.unpin();
				allowance.release(entry.getSize());
				return ImmediatePackedUnpinHandle.committed(entry, allowance, entry.getSize());
			}
			entry.unpin();
			entry.setCacheMeta(null);
			PackUnpinHandle handle = pin.builder.unpinProducer(entry, pin.slot, allowance);
			if(pin.builder.sealed && pin.builder.activePins == 0)
				pin.builder.transferProducerOwnership(_physical);
			scheduleSeal(pin.builder);
			return handle;
		}
	}

	private UnpinHandle unpinPacked(BlockEntry entry, PackedLogicalPin pin, MemoryAllowance allowance) {
		if(entry.fastUnpin())
			return ImmediatePackedUnpinHandle.committed(entry, allowance, entry.getSize());
		if(entry.getPinCount() > 1) {
			entry.unpin();
			return ImmediatePackedUnpinHandle.committed(entry, allowance, entry.getSize());
		}
		entry.unpin();
		entry.setCacheMeta(null);
		return pin.state.unpin(this, _packReleaseDelayMs, allowance);
	}

	void enqueueRelease(PackedPinState state) {
		if(!_running)
			return;
		if(state.markReleaseQueued()) {
			_releaseQueue.offer(state);
			scheduleReleaseMaintenance();
		}
	}

	private void enqueueReleaseNoSchedule(PackedPinState state) {
		if(_running && state.markReleaseQueued())
			_releaseQueue.offer(state);
	}

	private void scheduleReleaseMaintenance() {
		if(!_releaseRunning.compareAndSet(false, true))
			return;
		_releaseExecutor.execute(this::runReleaseMaintenance);
	}

	private void runReleaseMaintenance() {
		try {
			while(_running) {
				long nextDueNanos = Long.MAX_VALUE;
				ArrayList<PackedPinState> delayed = null;
				PackedPinState state;
				long nowNanos = System.nanoTime();
				while((state = _releaseQueue.poll()) != null) {
					state.clearReleaseQueued();
					long stateNextDue = state.releaseDuePins(_physical, nowNanos);
					if(stateNextDue != Long.MAX_VALUE) {
						if(delayed == null)
							delayed = new ArrayList<>();
						delayed.add(state);
						nextDueNanos = Math.min(nextDueNanos, stateNextDue);
					}
				}
				if(nextDueNanos == Long.MAX_VALUE)
					return;
				long waitNanos = nextDueNanos - System.nanoTime();
				if(waitNanos > 0)
					LockSupport.parkNanos(waitNanos);
				if(delayed != null)
					for(PackedPinState delayedState : delayed)
						enqueueReleaseNoSchedule(delayedState);
			}
		}
		finally {
			_releaseRunning.set(false);
			if(_running && !_releaseQueue.isEmpty())
				scheduleReleaseMaintenance();
		}
	}

	private SealedPackLocation forceSeal(PendingPackLocation pending) {
		synchronized(this) {
			sealBuilder(pending.builder);
			PackedCacheLocation location = getLocation(pending.builder.streamIds[pending.slot],
				pending.builder.tileIds[pending.slot]);
			return (SealedPackLocation)location;
		}
	}

	private static BlockEntry createLogicalPin(BlockKey logicalKey, SealedPackLocation location) {
		PackedBlock block = (PackedBlock)location.state.physicalEntry.getDataUnsafe();
		Object data = block.values[location.slot];
		long size = block.sizes[location.slot];
		BlockEntry logical = new BlockEntry(logicalKey, size, data, BlockState.REMOVED);
		logical.pin();
		logical.setCacheMeta(new PackedLogicalPin(location.state));
		return logical;
	}

	private PackBuilder getOpenBuilder(long streamId, MemoryAllowance allowance, long nextSize) {
		int sid = asIntStreamId(streamId);
		PackBuilder builder = sid < _builders.length ? _builders[sid] : null;
		if(builder != null && (builder.sealed || builder.allowance != allowance)) {
			sealBuilder(builder);
			builder = null;
		}
		if(builder != null)
			return builder;
		if(canOpenBuilder(nextSize)) {
			ensureBuilderCapacity(sid);
			builder = new PackBuilder(sid, allowance, _packTargetBytes);
			_builders[sid] = builder;
			_openBuilderCount++;
			return builder;
		}
		return getMixedBuilder(allowance);
	}

	private PackBuilder getMixedBuilder(MemoryAllowance allowance) {
		if(_mixedBuilder != null && (_mixedBuilder.sealed || _mixedBuilder.allowance != allowance))
			sealBuilder(_mixedBuilder);
		if(_mixedBuilder == null) {
			_mixedBuilder = new PackBuilder(-1, allowance, _mixedPackTargetBytes);
			_openBuilderCount++;
		}
		return _mixedBuilder;
	}

	private boolean canOpenBuilder(long nextSize) {
		return _openBuilderCount < _maxOpenBuilders && _stagingBytes + nextSize <= _maxStagingBytes;
	}

	private int appendToBuilder(PackBuilder builder, long streamId, long tileId, Object data, long size) {
		int slot = builder.append(streamId, tileId, data, size);
		_stagingBytes += size;
		putLocation(new BlockKey(streamId, tileId), new PendingPackLocation(builder, slot));
		if(builder.getBytes() >= builder.packTargetBytes)
			sealBuilder(builder);
		else
			enforceStagingBudget();
		return slot;
	}

	private void enforceStagingBudget() {
		while(_stagingBytes > _maxStagingBytes || _openBuilderCount > _maxOpenBuilders) {
			PackBuilder builder = findLargestOpenBuilder();
			if(builder == null)
				return;
			sealBuilder(builder);
		}
	}

	private PackBuilder findLargestOpenBuilder() {
		PackBuilder largest = null;
		for(PackBuilder builder : _builders)
			if(builder != null && !builder.sealed && (largest == null || builder.getBytes() > largest.getBytes()))
				largest = builder;
		if(_mixedBuilder != null && !_mixedBuilder.sealed &&
			(largest == null || _mixedBuilder.getBytes() > largest.getBytes()))
			largest = _mixedBuilder;
		return largest;
	}

	private void sealBuilder(PackBuilder builder) {
		if(builder.sealed || builder.count == 0)
			return;
		builder.sealed = true;
		_stagingBytes -= builder.getBytes();
		_openBuilderCount--;
		if(builder.streamSlot >= 0 && builder.streamSlot < _builders.length && _builders[builder.streamSlot] == builder)
			_builders[builder.streamSlot] = null;
		if(_mixedBuilder == builder)
			_mixedBuilder = null;

		PackedBlock block = builder.createBlock();
		BlockEntry physicalEntry = putSealedBlockPinned(block, builder.allowance);
		PackedPinState state = new PackedPinState(physicalEntry);
		builder.state = state;

		for(int i = 0; i < builder.count; i++)
			putLocation(new BlockKey(builder.streamIds[i], builder.tileIds[i]), new SealedPackLocation(state, i));

		if(builder.activePins == 0)
			builder.transferProducerOwnership(_physical);
	}

	private BlockEntry putSealedBlockPinned(PackedBlock block, MemoryAllowance allowance) {
		BlockKey packedKey = new BlockKey(PACKED_STREAM_ID, _nextPackedId.getAndIncrement());
		return _physical.putPinned(packedKey, block, block.totalSize, allowance);
	}

	private void scheduleSeal(PackBuilder builder) {
		if(builder.sealScheduled || builder.sealed || _sealDelayMs < 0)
			return;
		builder.sealScheduled = true;
		_sealExecutor.schedule(() -> {
			synchronized(OOCPackedCache.this) {
				builder.sealScheduled = false;
				sealBuilder(builder);
			}
		}, _sealDelayMs, TimeUnit.MILLISECONDS);
	}

	private PackedCacheLocation getLocation(long sId, long tId) {
		MaskedOnceArrayList<PackedCacheLocation> stream = _locations.get(sId);
		return stream == null ? null : stream.get(blockIndex(tId));
	}

	private void putLocation(BlockKey key, PackedCacheLocation location) {
		_locations.getOrCreate(key.getStreamId()).put(blockIndex(key.getSequenceNumber()), location);
	}

	private void ensureBuilderCapacity(int streamId) {
		if(streamId < _builders.length)
			return;
		int len = _builders.length;
		while(streamId >= len)
			len <<= 1;
		PackBuilder[] bigger = new PackBuilder[len];
		System.arraycopy(_builders, 0, bigger, 0, _builders.length);
		_builders = bigger;
	}

	private static int asIntStreamId(long streamId) {
		if(streamId < 0 || streamId > Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("Invalid streamId: " + streamId);
		return (int)streamId;
	}

	private static int blockIndex(long sequenceNumber) {
		if(sequenceNumber < 0 || sequenceNumber > Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("Invalid block index: " + sequenceNumber);
		return (int)sequenceNumber;
	}

	private void checkRunning() {
		if(!_running)
			throw new IllegalStateException("Cache has been shut down.");
	}
}
