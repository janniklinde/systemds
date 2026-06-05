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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Logical-to-physical packing adapter. Small logical blocks are packed into larger physical cache entries
 * before being handed to OOCCacheImpl.
 */
public final class OOCPackedCache implements OOCCache {
	private static final int PACKED_STREAM_ID = 0;
	private static final long DEFAULT_PACK_THRESHOLD_BYTES = 64 * 1024;
	private static final long DEFAULT_PACK_TARGET_BYTES = 1L << 19; //512 KB tile packing
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
	private final SegmentedStreamTableList<LogicalLocation> _locations;
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
				putLocation(new BlockKey(sId, tIds[off + i]), new PackedLocation(state, i));
			return physicalEntry;
		}
	}

	@Override
	public CompletableFuture<BlockEntry> pin(long sId, long tId, MemoryAllowance allowance) {
		LogicalLocation location = getLocation(sId, tId);
		if(location == null)
			return _physical.pin(sId, tId, allowance);
		if(location instanceof PendingLocation pending)
			location = forceSeal(pending);
		if(!(location instanceof PackedLocation packed))
			return _physical.pin(sId, tId, allowance);

		PackedLocation packedLocation = packed;
		return packedLocation.state.pin(_physical, allowance, false).thenApply(physicalEntry -> {
			if(physicalEntry == null)
				return null;
			return createLogicalPin(new BlockKey(sId, tId), packedLocation);
		});
	}

	public PackedPin pinPacked(long sId, long tId, MemoryAllowance allowance) {
		LogicalLocation location = getLocation(sId, tId);
		if(location == null)
			return PackedPin.fromFuture(_physical.pin(sId, tId, allowance));
		if(location instanceof PendingLocation pending)
			location = forceSeal(pending);
		if(!(location instanceof PackedLocation packed))
			return PackedPin.fromFuture(_physical.pin(sId, tId, allowance));

		PackedLocation packedLocation = packed;
		BlockKey logicalKey = new BlockKey(sId, tId);
		return packedLocation.state.pinLight(_physical, allowance, logicalKey, packedLocation);
	}

	@Override
	public BlockEntry pinIfLive(long sId, long tId, MemoryAllowance allowance) {
		LogicalLocation location = getLocation(sId, tId);
		if(location == null)
			return _physical.pinIfLive(sId, tId, allowance);
		if(location instanceof PendingLocation pending)
			location = forceSeal(pending);
		if(!(location instanceof PackedLocation packed))
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
			return ImmediateUnpinHandle.committed(entry, allowance, entry.getSize());
		}
		synchronized(this) {
			if(entry.getPinCount() > 1) {
				entry.unpin();
				allowance.release(entry.getSize());
				return ImmediateUnpinHandle.committed(entry, allowance, entry.getSize());
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
			return ImmediateUnpinHandle.committed(entry, allowance, entry.getSize());
		if(entry.getPinCount() > 1) {
			entry.unpin();
			return ImmediateUnpinHandle.committed(entry, allowance, entry.getSize());
		}
		entry.unpin();
		entry.setCacheMeta(null);
		return pin.state.unpin(this, _packReleaseDelayMs, allowance);
	}

	private void enqueueRelease(PackedPinState state) {
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

	private PackedLocation forceSeal(PendingLocation pending) {
		synchronized(this) {
			sealBuilder(pending.builder);
			LogicalLocation location = getLocation(pending.builder.streamIds[pending.slot],
				pending.builder.tileIds[pending.slot]);
			return (PackedLocation)location;
		}
	}

	private static BlockEntry createLogicalPin(BlockKey logicalKey, PackedLocation location) {
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
		putLocation(new BlockKey(streamId, tileId), new PendingLocation(builder, slot));
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
			putLocation(new BlockKey(builder.streamIds[i], builder.tileIds[i]), new PackedLocation(state, i));

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

	private LogicalLocation getLocation(long sId, long tId) {
		MaskedOnceArrayList<LogicalLocation> stream = _locations.get(sId);
		return stream == null ? null : stream.get(blockIndex(tId));
	}

	private void putLocation(BlockKey key, LogicalLocation location) {
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

	private interface LogicalLocation {
	}

	private static final class PendingLocation implements LogicalLocation {
		private final PackBuilder builder;
		private final int slot;

		private PendingLocation(PackBuilder builder, int slot) {
			this.builder = builder;
			this.slot = slot;
		}
	}

	private static final class PackedLocation implements LogicalLocation {
		private final PackedPinState state;
		private final int slot;

		private PackedLocation(PackedPinState state, int slot) {
			this.state = state;
			this.slot = slot;
		}
	}

	private static final class PendingLogicalPin {
		private final PackBuilder builder;
		private final int slot;

		private PendingLogicalPin(PackBuilder builder, int slot) {
			this.builder = builder;
			this.slot = slot;
		}
	}

	private static final class PackedLogicalPin {
		private final PackedPinState state;

		private PackedLogicalPin(PackedPinState state) {
			this.state = state;
		}
	}

	private static final class PackBuilder {
		private final int streamSlot;
		private final MemoryAllowance allowance;
		private final long packTargetBytes;
		private final List<PackUnpinHandle> deferredUnpins = new ArrayList<>();
		private long[] streamIds = new long[16];
		private long[] tileIds = new long[16];
		private Object[] values = new Object[16];
		private long[] sizes = new long[16];
		private long bytes;
		private int count;
		private int activePins;
		private boolean sealed;
		private boolean sealScheduled;
		private boolean producerTransferred;
		private PackedPinState state;

		private PackBuilder(int streamSlot, MemoryAllowance allowance, long packTargetBytes) {
			this.streamSlot = streamSlot;
			this.allowance = allowance;
			this.packTargetBytes = packTargetBytes;
		}

		private int append(long streamId, long tileId, Object value, long size) {
			ensureCapacity(count + 1);
			int slot = count++;
			streamIds[slot] = streamId;
			tileIds[slot] = tileId;
			values[slot] = value;
			sizes[slot] = size;
			bytes += size;
			activePins++;
			return slot;
		}

		private void ensureCapacity(int minSize) {
			if(minSize <= values.length)
				return;
			int len = values.length;
			while(minSize > len)
				len <<= 1;
			streamIds = Arrays.copyOf(streamIds, len);
			tileIds = Arrays.copyOf(tileIds, len);
			values = Arrays.copyOf(values, len);
			sizes = Arrays.copyOf(sizes, len);
		}

		private long getBytes() {
			return bytes;
		}

		private PackedBlock createBlock() {
			return new PackedBlock(Arrays.copyOf(values, count), Arrays.copyOf(sizes, count), bytes);
		}

		private PackUnpinHandle unpinProducer(BlockEntry entry, int slot, MemoryAllowance allowance) {
			activePins--;
			PackUnpinHandle handle = new PackUnpinHandle(entry, this, slot, allowance, sizes[slot]);
			deferredUnpins.add(handle);
			return handle;
		}

		private void transferProducerOwnership(OOCCacheImpl physical) {
			if(state == null || physical == null || producerTransferred)
				return;
			producerTransferred = true;
			OOCCache.UnpinHandle physicalUnpin = physical.unpin(state.physicalEntry, allowance);
			if(physicalUnpin.isCommitted()) {
				completeDeferredUnpins(true);
				return;
			}
			physicalUnpin.getCompletionFuture().whenComplete((committed, ex) -> completeDeferredUnpins(ex == null && committed));
		}

		private void completeDeferredUnpins(boolean committed) {
			for(PackUnpinHandle handle : deferredUnpins)
				handle.complete(committed);
			deferredUnpins.clear();
		}
	}

	static final class PackedBlock implements SpillableObject {
		private Object[] values;
		private long[] sizes;
		private long totalSize;

		PackedBlock() {
			values = null;
			sizes = null;
			totalSize = 0;
		}

		private PackedBlock(Object[] values, long[] sizes, long totalSize) {
			this.values = values;
			this.sizes = sizes;
			this.totalSize = totalSize;
		}

		@Override
		public boolean tryWrite(DataOutput out) throws IOException {
			out.writeInt(values.length);
			for(int i = 0; i < values.length; i++) {
				out.writeLong(sizes[i]);
				Object value = values[i];
				if(!(value instanceof SpillableObject spillable))
					return false;
				if(!SpillableObjectRegistry.tryWrite(out, spillable))
					return false;
			}
			return true;
		}

		@Override
		public void read(DataInput in) throws IOException {
			int count = in.readInt();
			values = new Object[count];
			sizes = new long[count];
			totalSize = 0;
			for(int i = 0; i < count; i++) {
				sizes[i] = in.readLong();
				values[i] = SpillableObjectRegistry.read(in);
				totalSize += sizes[i];
			}
		}
	}

	private static final class PackedPinState {
		private final BlockEntry physicalEntry;
		private MemoryAllowance[] allowances;
		private int[] counts;
		private CompletableFuture<BlockEntry>[] futures;
		private Subscriber[] subscribers;
		private long[] releaseDueNanos;
		private DelayedPackedUnpinHandle[] releaseHandles;
		private int size;
		private boolean releaseQueued;

		@SuppressWarnings("unchecked")
		private PackedPinState(BlockEntry physicalEntry) {
			this.physicalEntry = physicalEntry;
			allowances = new MemoryAllowance[2];
			counts = new int[2];
			futures = new CompletableFuture[2];
			subscribers = new Subscriber[2];
			releaseDueNanos = new long[2];
			releaseHandles = new DelayedPackedUnpinHandle[2];
			size = 0;
			releaseQueued = false;
		}

		private synchronized CompletableFuture<BlockEntry> pin(OOCCacheImpl physical, MemoryAllowance allowance,
			boolean liveOnly) {
			int ix = indexOf(allowance);
			if(ix >= 0) {
				cancelRelease(ix);
				counts[ix]++;
				return futures[ix];
			}
			CompletableFuture<BlockEntry> future = liveOnly ?
				CompletableFuture.completedFuture(physical.pinIfLive(physicalEntry.getKey().getStreamId(),
					physicalEntry.getKey().getSequenceNumber(), allowance)) :
				physical.pin(physicalEntry.getKey(), allowance);
			addAllowance(allowance, future);
			future.whenComplete((entry, ex) -> {
				completeSubscribers(allowance, future, entry, ex);
				if(entry == null || ex != null)
					removeFailedAllowance(allowance, future);
			});
			return future;
		}

		private PackedPin pinLight(OOCCacheImpl physical, MemoryAllowance allowance, BlockKey logicalKey,
			PackedLocation location) {
			CompletableFuture<BlockEntry> future;
			synchronized(this) {
				int ix = indexOf(allowance);
				if(ix >= 0) {
					cancelRelease(ix);
					counts[ix]++;
					BlockEntry entry = getNowOrNull(futures[ix]);
					if(entry != null)
						return PackedPin.completed(createLogicalPin(logicalKey, location));
					return new SubscribedPackedPin(this, allowance, futures[ix], logicalKey, location);
				}
				future = physical.pin(physicalEntry.getKey(), allowance);
				addAllowance(allowance, future);
			}
			future.whenComplete((entry, ex) -> {
				completeSubscribers(allowance, future, entry, ex);
				if(entry == null || ex != null)
					removeFailedAllowance(allowance, future);
			});
			BlockEntry entry = getNowOrNull(future);
			if(entry != null)
				return PackedPin.completed(createLogicalPin(logicalKey, location));
			return new SubscribedPackedPin(this, allowance, future, logicalKey, location);
		}

		private BlockEntry pinIfLive(OOCCacheImpl physical, MemoryAllowance allowance) {
			try {
				return pin(physical, allowance, true).getNow(null);
			}
			catch(RuntimeException ex) {
				return null;
			}
		}

		private synchronized UnpinHandle unpin(OOCPackedCache owner, long releaseDelayMs, MemoryAllowance allowance) {
			int ix = indexOf(allowance);
			if(ix < 0)
				return ImmediateUnpinHandle.committed(physicalEntry, allowance, physicalEntry.getSize());
			counts[ix]--;
			if(counts[ix] > 0)
				return ImmediateUnpinHandle.committed(physicalEntry, allowance, physicalEntry.getSize());
			DelayedPackedUnpinHandle handle = new DelayedPackedUnpinHandle(physicalEntry, allowance);
			releaseHandles[ix] = handle;
			releaseDueNanos[ix] = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, releaseDelayMs));
			owner.enqueueRelease(this);
			return handle;
		}

		private int indexOf(MemoryAllowance allowance) {
			for(int i = 0; i < size; i++)
				if(allowances[i] == allowance)
					return i;
			return -1;
		}

		private synchronized void addAllowance(MemoryAllowance allowance, CompletableFuture<BlockEntry> future) {
			if(size == allowances.length) {
				MemoryAllowance[] biggerAllowances = new MemoryAllowance[size * 2];
				int[] biggerCounts = new int[size * 2];
				@SuppressWarnings("unchecked")
				CompletableFuture<BlockEntry>[] biggerFutures = new CompletableFuture[size * 2];
				Subscriber[] biggerSubscribers = new Subscriber[size * 2];
				long[] biggerReleaseDueNanos = new long[size * 2];
				DelayedPackedUnpinHandle[] biggerReleaseHandles = new DelayedPackedUnpinHandle[size * 2];
				System.arraycopy(allowances, 0, biggerAllowances, 0, size);
				System.arraycopy(counts, 0, biggerCounts, 0, size);
				System.arraycopy(futures, 0, biggerFutures, 0, size);
				System.arraycopy(subscribers, 0, biggerSubscribers, 0, size);
				System.arraycopy(releaseDueNanos, 0, biggerReleaseDueNanos, 0, size);
				System.arraycopy(releaseHandles, 0, biggerReleaseHandles, 0, size);
				allowances = biggerAllowances;
				counts = biggerCounts;
				futures = biggerFutures;
				subscribers = biggerSubscribers;
				releaseDueNanos = biggerReleaseDueNanos;
				releaseHandles = biggerReleaseHandles;
			}
			allowances[size] = allowance;
			counts[size] = 1;
			futures[size] = future;
			size++;
		}

		private void cancelRelease(int ix) {
			releaseDueNanos[ix] = 0;
			DelayedPackedUnpinHandle handle = releaseHandles[ix];
			if(handle != null) {
				releaseHandles[ix] = null;
				handle.complete(false);
			}
		}

		private long releaseDuePins(OOCCacheImpl physical, long nowNanos) {
			ArrayList<PackedRelease> due = null;
			long nextDueNanos = Long.MAX_VALUE;
			synchronized(this) {
				for(int i = 0; i < size;) {
					DelayedPackedUnpinHandle handle = releaseHandles[i];
					if(handle == null || counts[i] > 0) {
						i++;
						continue;
					}
					long dueNanos = releaseDueNanos[i];
					if(dueNanos > nowNanos) {
						nextDueNanos = Math.min(nextDueNanos, dueNanos);
						i++;
						continue;
					}
					if(due == null)
						due = new ArrayList<>();
					due.add(new PackedRelease(allowances[i], handle));
					removeAt(i);
				}
			}
			if(due != null)
				for(PackedRelease release : due)
					releasePhysicalPin(physical, release.allowance, release.handle);
			return nextDueNanos;
		}

		private void releasePhysicalPin(OOCCacheImpl physical, MemoryAllowance allowance,
			DelayedPackedUnpinHandle handle) {
			UnpinHandle physicalHandle = physical.unpin(physicalEntry, allowance);
			if(physicalHandle.isCommitted()) {
				handle.complete(true);
				return;
			}
			physicalHandle.getCompletionFuture().whenComplete((committed, ex) ->
				handle.complete(ex == null && Boolean.TRUE.equals(committed)));
		}

		private synchronized void removeFailedAllowance(MemoryAllowance allowance, CompletableFuture<BlockEntry> future) {
			int ix = indexOf(allowance);
			if(ix >= 0 && futures[ix] == future)
				removeAt(ix);
		}

		private void removeAt(int ix) {
			int last = --size;
			allowances[ix] = allowances[last];
			counts[ix] = counts[last];
			futures[ix] = futures[last];
			subscribers[ix] = subscribers[last];
			releaseDueNanos[ix] = releaseDueNanos[last];
			releaseHandles[ix] = releaseHandles[last];
			allowances[last] = null;
			counts[last] = 0;
			futures[last] = null;
			subscribers[last] = null;
			releaseDueNanos[last] = 0;
			releaseHandles[last] = null;
		}

		private synchronized boolean markReleaseQueued() {
			if(releaseQueued)
				return false;
			releaseQueued = true;
			return true;
		}

		private synchronized void clearReleaseQueued() {
			releaseQueued = false;
		}

		private void addSubscriber(MemoryAllowance allowance, CompletableFuture<BlockEntry> future,
			Subscriber subscriber) {
			boolean stale = false;
			boolean completeNow = false;
			synchronized(this) {
				int ix = indexOf(allowance);
				if(ix < 0 || futures[ix] != future) {
					stale = true;
				}
				else {
					subscriber.next = subscribers[ix];
					subscribers[ix] = subscriber;
					completeNow = future.isDone();
				}
			}
			if(stale) {
				subscriber.accept(null);
				return;
			}
			if(completeNow) {
				BlockEntry entry;
				Throwable ex = null;
				try {
					entry = getNowOrNull(future);
				}
				catch(Throwable t) {
					entry = null;
					ex = t;
				}
				completeSubscribers(allowance, future, entry, ex);
			}
		}

		private void completeSubscribers(MemoryAllowance allowance, CompletableFuture<BlockEntry> future,
			BlockEntry physicalEntry, Throwable ex) {
			Subscriber head;
			synchronized(this) {
				int ix = indexOf(allowance);
				if(ix < 0 || futures[ix] != future)
					return;
				head = subscribers[ix];
				subscribers[ix] = null;
			}
			BlockEntry entry = ex == null ? physicalEntry : null;
			while(head != null) {
				Subscriber next = head.next;
				head.accept(entry);
				head = next;
			}
		}
	}

	private static final class SubscribedPackedPin implements PackedPin {
		private final PackedPinState state;
		private final MemoryAllowance allowance;
		private final CompletableFuture<BlockEntry> future;
		private final BlockKey logicalKey;
		private final PackedLocation location;

		private SubscribedPackedPin(PackedPinState state, MemoryAllowance allowance, CompletableFuture<BlockEntry> future,
			BlockKey logicalKey, PackedLocation location) {
			this.state = state;
			this.allowance = allowance;
			this.future = future;
			this.logicalKey = logicalKey;
			this.location = location;
		}

		@Override
		public void thenAccept(Consumer<BlockEntry> action) {
			BlockEntry physicalEntry = getNowOrNull(future);
			if(physicalEntry != null) {
				action.accept(createLogicalPin(logicalKey, location));
				return;
			}
			state.addSubscriber(allowance, future, new Subscriber(logicalKey, location, action));
		}
	}

	private static final class Subscriber {
		private final BlockKey logicalKey;
		private final PackedLocation location;
		private final Consumer<BlockEntry> action;
		private Subscriber next;

		private Subscriber(BlockKey logicalKey, PackedLocation location, Consumer<BlockEntry> action) {
			this.logicalKey = logicalKey;
			this.location = location;
			this.action = action;
		}

		private void accept(BlockEntry physicalEntry) {
			action.accept(physicalEntry == null ? null : createLogicalPin(logicalKey, location));
		}
	}

	private record PackedRelease(MemoryAllowance allowance, DelayedPackedUnpinHandle handle) {
	}

	private static BlockEntry getNowOrNull(CompletableFuture<BlockEntry> future) {
		try {
			return future.getNow(null);
		}
		catch(Throwable t) {
			return null;
		}
	}

	public interface PackedPin {
		void thenAccept(Consumer<BlockEntry> action);

		static PackedPin completed(BlockEntry entry) {
			return action -> action.accept(entry);
		}

		static PackedPin fromFuture(CompletableFuture<BlockEntry> future) {
			return action -> future.thenAccept(action);
		}
	}

	private static final class DelayedPackedUnpinHandle implements UnpinHandle {
		private final BlockEntry entry;
		private final MemoryAllowance allowance;
		private final CompletableFuture<Boolean> future = new CompletableFuture<>();

		private DelayedPackedUnpinHandle(BlockEntry entry, MemoryAllowance allowance) {
			this.entry = entry;
			this.allowance = allowance;
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
			return entry.getSize();
		}

		@Override
		public boolean isCommitted() {
			return Boolean.TRUE.equals(future.getNow(false));
		}

		@Override
		public CompletableFuture<Boolean> getCompletionFuture() {
			return future;
		}

		@Override
		public BlockEntry reclaim() {
			return null;
		}

		private void complete(boolean committed) {
			future.complete(committed);
		}
	}

	private static final class PackUnpinHandle implements UnpinHandle {
		private final BlockEntry entry;
		private final PackBuilder builder;
		private final int slot;
		private final MemoryAllowance allowance;
		private final long bytes;
		private final CompletableFuture<Boolean> future = new CompletableFuture<>();

		private PackUnpinHandle(BlockEntry entry, PackBuilder builder, int slot, MemoryAllowance allowance, long bytes) {
			this.entry = entry;
			this.builder = builder;
			this.slot = slot;
			this.allowance = allowance;
			this.bytes = bytes;
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
			return Boolean.TRUE.equals(future.getNow(false));
		}

		@Override
		public CompletableFuture<Boolean> getCompletionFuture() {
			return future;
		}

		@Override
		public BlockEntry reclaim() {
			return null;
		}

		private void complete(boolean committed) {
			future.complete(committed);
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
}
