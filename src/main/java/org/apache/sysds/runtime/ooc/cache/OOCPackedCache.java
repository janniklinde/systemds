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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Logical-to-physical packing adapter. Small logical blocks are packed into larger physical cache entries
 * before being handed to OOCCacheImpl.
 */
public final class OOCPackedCache implements OOCCache {
	private static final int PACKED_STREAM_ID = 0;
	private static final long DEFAULT_PACK_THRESHOLD_BYTES = 64 * 1024;
	private static final long DEFAULT_PACK_TARGET_BYTES = 4L << 17;
	private static final long DEFAULT_SEAL_DELAY_MS = 5;

	private final OOCCacheImpl _physical;
	private final long _packThresholdBytes;
	private final long _packTargetBytes;
	private final long _sealDelayMs;
	private final SegmentedStreamTableList<LogicalLocation> _locations;
	private final ScheduledExecutorService _sealExecutor;
	private final AtomicInteger _nextPackedId;

	private PackBuilder[] _builders;
	private boolean _running;

	public OOCPackedCache(OOCIOHandler ioHandler, long hardLimit, long evictionLimit) {
		this(new OOCCacheImpl(ioHandler, hardLimit, evictionLimit), DEFAULT_PACK_THRESHOLD_BYTES,
			DEFAULT_PACK_TARGET_BYTES, DEFAULT_SEAL_DELAY_MS);
	}

	public OOCPackedCache(OOCCacheImpl physical) {
		this(physical, DEFAULT_PACK_THRESHOLD_BYTES, DEFAULT_PACK_TARGET_BYTES, DEFAULT_SEAL_DELAY_MS);
	}

	public OOCPackedCache(OOCCacheImpl physical, long packThresholdBytes, long packTargetBytes, long sealDelayMs) {
		if(packThresholdBytes <= 0 || packTargetBytes < packThresholdBytes)
			throw new IllegalArgumentException("Invalid pack sizes: threshold=" + packThresholdBytes +
				", target=" + packTargetBytes);
		_physical = physical;
		_packThresholdBytes = packThresholdBytes;
		_packTargetBytes = packTargetBytes;
		_sealDelayMs = sealDelayMs;
		_locations = new SegmentedStreamTableList<>();
		_nextPackedId = new AtomicInteger();
		_builders = new PackBuilder[16];
		_running = true;
		_sealExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "ooc-pack-sealer");
			t.setDaemon(true);
			return t;
		});
	}

	@Override
	public BlockEntry putPinned(long sId, long tId, Object data, long size, MemoryAllowance allowance) {
		if(size >= _packThresholdBytes)
			return _physical.putPinned(sId, tId, data, size, allowance);

		PackBuilder builder;
		PendingLocation location;
		int slot;
		synchronized(this) {
			checkRunning();
			builder = getOpenBuilder(sId, allowance);
			slot = builder.append(tId, data, size);
			location = new PendingLocation(builder, slot);
			putLocation(new BlockKey(sId, tId), location);
			if(builder.getBytes() >= _packTargetBytes)
				sealBuilder(builder);
		}

		BlockEntry logical = new BlockEntry(new BlockKey(sId, tId), size, data, BlockState.REMOVED);
		logical.pin();
		logical.setCacheMeta(new PendingLogicalPin(builder, slot));
		return logical;
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
			return createLogicalPin(new BlockKey(sId, tId), packedLocation, allowance);
		});
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
		return createLogicalPin(new BlockKey(sId, tId), packed, allowance);
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
		_sealExecutor.shutdownNow();
		_physical.shutdown();
	}

	public synchronized void flushPacks() {
		for(PackBuilder builder : _builders)
			if(builder != null)
				sealBuilder(builder);
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
		return pin.state.unpin(_physical, allowance);
	}

	private PackedLocation forceSeal(PendingLocation pending) {
		synchronized(this) {
			sealBuilder(pending.builder);
			LogicalLocation location = getLocation(pending.builder.streamId, pending.builder.tileIds[pending.slot]);
			return (PackedLocation)location;
		}
	}

	private BlockEntry createLogicalPin(BlockKey logicalKey, PackedLocation location, MemoryAllowance allowance) {
		PackedBlock block = (PackedBlock)location.state.physicalEntry.getDataUnsafe();
		Object data = block.values[location.slot];
		long size = block.sizes[location.slot];
		BlockEntry logical = new BlockEntry(logicalKey, size, data, BlockState.REMOVED);
		logical.pin();
		logical.setCacheMeta(new PackedLogicalPin(location.state, allowance));
		return logical;
	}

	private PackBuilder getOpenBuilder(long streamId, MemoryAllowance allowance) {
		int sid = asIntStreamId(streamId);
		ensureBuilderCapacity(sid);
		PackBuilder builder = _builders[sid];
		if(builder == null || builder.sealed || builder.allowance != allowance) {
			if(builder != null)
				sealBuilder(builder);
			builder = new PackBuilder(streamId, allowance);
			_builders[sid] = builder;
		}
		return builder;
	}

	private void sealBuilder(PackBuilder builder) {
		if(builder.sealed || builder.count == 0)
			return;
		builder.sealed = true;
		BlockKey packedKey = new BlockKey(PACKED_STREAM_ID, _nextPackedId.getAndIncrement());
		PackedBlock block = builder.createBlock();
		BlockEntry physicalEntry = _physical.putPinned(packedKey, block, block.totalSize, builder.allowance);
		PackedPinState state = new PackedPinState(physicalEntry);
		builder.state = state;

		for(int i = 0; i < builder.count; i++)
			putLocation(new BlockKey(builder.streamId, builder.tileIds[i]), new PackedLocation(state, i));

		if(builder.activePins == 0)
			builder.transferProducerOwnership(_physical);
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
		@SuppressWarnings("unused")
		private final MemoryAllowance allowance;

		private PackedLogicalPin(PackedPinState state, MemoryAllowance allowance) {
			this.state = state;
			this.allowance = allowance;
		}
	}

	private static final class PackBuilder {
		private final long streamId;
		private final MemoryAllowance allowance;
		private final List<PackUnpinHandle> deferredUnpins = new ArrayList<>();
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

		private PackBuilder(long streamId, MemoryAllowance allowance) {
			this.streamId = streamId;
			this.allowance = allowance;
		}

		private int append(long tileId, Object value, long size) {
			ensureCapacity(count + 1);
			int slot = count++;
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

	private static final class PackedBlock implements SpillableObject {
		private final Object[] values;
		private final long[] sizes;
		private final long totalSize;

		private PackedBlock(Object[] values, long[] sizes, long totalSize) {
			this.values = values;
			this.sizes = sizes;
			this.totalSize = totalSize;
		}

		@Override
		public boolean tryWrite(DataOutput out) throws IOException {
			out.writeInt(values.length);
			for(Object value : values) {
				if(!(value instanceof SpillableObject spillable))
					return false;
				if(!spillable.tryWrite(out))
					return false;
			}
			return true;
		}

		@Override
		public void read(DataInput in) throws IOException {
			throw new IOException("Packed OOC block reads are not implemented yet.");
		}
	}

	private static final class PackedPinState {
		private final BlockEntry physicalEntry;
		private MemoryAllowance[] allowances;
		private int[] counts;
		private CompletableFuture<BlockEntry>[] futures;
		private int size;

		@SuppressWarnings("unchecked")
		private PackedPinState(BlockEntry physicalEntry) {
			this.physicalEntry = physicalEntry;
			allowances = new MemoryAllowance[2];
			counts = new int[2];
			futures = new CompletableFuture[2];
			size = 0;
		}

		private synchronized CompletableFuture<BlockEntry> pin(OOCCacheImpl physical, MemoryAllowance allowance,
			boolean liveOnly) {
			int ix = indexOf(allowance);
			if(ix >= 0) {
				counts[ix]++;
				return futures[ix];
			}
			CompletableFuture<BlockEntry> future = liveOnly ?
				CompletableFuture.completedFuture(physical.pinIfLive(physicalEntry.getKey().getStreamId(),
					physicalEntry.getKey().getSequenceNumber(), allowance)) :
				physical.pin(physicalEntry.getKey(), allowance);
			addAllowance(allowance, future);
			future.whenComplete((entry, ex) -> {
				if(entry == null || ex != null)
					removeFailedAllowance(allowance, future);
			});
			return future;
		}

		private BlockEntry pinIfLive(OOCCacheImpl physical, MemoryAllowance allowance) {
			try {
				return pin(physical, allowance, true).getNow(null);
			}
			catch(RuntimeException ex) {
				return null;
			}
		}

		private synchronized UnpinHandle unpin(OOCCacheImpl physical, MemoryAllowance allowance) {
			int ix = indexOf(allowance);
			if(ix < 0)
				return ImmediateUnpinHandle.committed(physicalEntry, allowance, physicalEntry.getSize());
			counts[ix]--;
			if(counts[ix] > 0)
				return ImmediateUnpinHandle.committed(physicalEntry, allowance, physicalEntry.getSize());
			removeAt(ix);
			return physical.unpin(physicalEntry, allowance);
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
				CompletableFuture<BlockEntry>[] biggerFutures = new CompletableFuture[size * 2];
				System.arraycopy(allowances, 0, biggerAllowances, 0, size);
				System.arraycopy(counts, 0, biggerCounts, 0, size);
				System.arraycopy(futures, 0, biggerFutures, 0, size);
				allowances = biggerAllowances;
				counts = biggerCounts;
				futures = biggerFutures;
			}
			allowances[size] = allowance;
			counts[size] = 1;
			futures[size] = future;
			size++;
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
			allowances[last] = null;
			counts[last] = 0;
			futures[last] = null;
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
