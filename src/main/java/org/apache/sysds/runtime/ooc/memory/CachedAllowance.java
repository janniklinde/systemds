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

package org.apache.sysds.runtime.ooc.memory;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.util.OOCCacheUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class CachedAllowance extends SyncMemoryAllowance {
	private static final int INITIAL_SLOTS = 64;
	private static final long MIN_HANDOVER_SLACK = 1_000_000L;
	private static final long MAX_HANDOVER_SLACK = 128_000_000L;
	private static final long MIN_HANDOVER_BATCH = 1_000_000L;

	private final long _streamId;
	private final AtomicLong _nextBlockId;
	private volatile AtomicReferenceArray<SlotEntry> _slots;
	private long _pendingHandoverBytes;
	private int _highestPopulatedIndex;
	private boolean _handoverScheduling;
	private boolean _handoverSchedulingRequested;
	private long _handoverSchedulingRequestedBytes;

	public CachedAllowance(MemoryBroker broker) {
		this(broker, INITIAL_SLOTS);
	}

	public CachedAllowance(MemoryBroker broker, int numTiles) {
		super(broker);
		_streamId = CachingStream._streamSeq.getNextID();
		_slots = new AtomicReferenceArray<>(numTiles);
		_nextBlockId = new AtomicLong(0);
	}

	public void handover(OOCStream.QueueCallback<IndexedMatrixValue> callback, int index) {
		OOCDebug.trace(() -> "[CACHE-HANDOVER-BEGIN] cache=" + dbgId() + " idx=" + index
			+ " cb=" + cbId(callback) + " managed=" + callback.getManagedBytes()
			+ " blockKey=" + callback.getBlockKey() + " backingPin=" + (callback.getBackingPin() != null));
		OOCStream.QueueCallback<IndexedMatrixValue> owned = OOCCacheUtils.handover(callback, this).join();
		OOCStream.QueueCallback<IndexedMatrixValue> root = OOCCacheUtils.retainLocal(owned.keepOpen());
		owned.close();

		SlotEntry entry = new SlotEntry(root);
		synchronized(this) {
			ensureCapacity(index);
			AtomicReferenceArray<SlotEntry> slots = _slots;
			if(slots.get(index) != null) {
				try {
					root.close();
				}
				finally {
					root.forget();
				}
				throw new IllegalStateException("Cached allowance slot " + index + " already occupied.");
			}
			slots.set(index, entry);
			if(index > _highestPopulatedIndex)
				_highestPopulatedIndex = index;
		}
		OOCDebug.trace(() -> "[CACHE-HANDOVER-END] cache=" + dbgId() + " idx=" + index
			+ " root=" + cbId(root) + " slots=" + slotCount());
		maybeScheduleHandovers(MIN_HANDOVER_BATCH);
	}

	public CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> handoverOrTakeExisting(
		OOCStream.QueueCallback<IndexedMatrixValue> callback, int index) {
		SlotEntry reservation = tryReserveCacheSlot(index);
		if(reservation != null)
			return finishReservedHandover(callback, index, reservation);

		return take(index).thenCompose(existing -> {
			if(existing != null)
				return CompletableFuture.completedFuture(existing);
			return handoverOrTakeExisting(callback, index);
		});
	}

	public OOCStream.QueueCallback<IndexedMatrixValue> tryGet(int index) {
		SlotEntry entry = getSlot(index);
		if(entry == null)
			return null;

		OOCCacheUtils.TileHandle handle = null;
		synchronized(entry) {
			if(entry._local != null)
				return entry._local.keepOpen();
			if(entry._state == SlotEntry.STATE_HANDLE && entry._handle != null && entry._handle.softLocal() != null)
				handle = entry._handle;
			else
				return null;
		}

		if(!super.tryReserve(handle.bytes()))
			return null;

		try {
			CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future = handle.read(this);
			if(!future.isDone()) {
				release(handle.bytes());
				return null;
			}

			OOCStream.QueueCallback<IndexedMatrixValue> callback = future.join();
			if(callback == null) {
				release(handle.bytes());
				return null;
			}
			OOCStream.QueueCallback<IndexedMatrixValue> installed = installReadResult(entry, handle, callback);
			if(installed != null) {
				OOCCacheUtils.noteEscape(installed, "CachedAllowance.tryGet[index=" + index + "]");
				return installed;
			}
		}
		catch(RuntimeException ex) {
			release(handle.bytes());
			throw ex;
		}

		return null;
	}

	public CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> get(int index) {
		OOCStream.QueueCallback<IndexedMatrixValue> immediate = tryGet(index);
		if(immediate != null)
			return CompletableFuture.completedFuture(immediate);

		SlotEntry entry = getSlot(index);
		if(entry == null)
			return CompletableFuture.completedFuture(null);

		CompletableFuture<Void> reservationFuture = null;
		CompletableFuture<OOCCacheUtils.TileHandle> spillFuture = null;
		OOCCacheUtils.TileHandle handle = null;
		synchronized(entry) {
			if(entry._local != null)
				return CompletableFuture.completedFuture(entry._local.keepOpen());
			if(entry._state == SlotEntry.STATE_RESERVED)
				reservationFuture = entry._reservationFuture;
			else if(entry._state == SlotEntry.STATE_SPILLING)
				spillFuture = entry._spillFuture;
			else if(entry._state == SlotEntry.STATE_HANDLE)
				handle = entry._handle;
		}

		if(reservationFuture != null)
			return reservationFuture.thenCompose(ignored -> get(index));
		if(spillFuture != null)
			return spillFuture.handle((ignored, ex) -> true).thenCompose(ignored -> get(index));
		if(handle == null)
			return CompletableFuture.completedFuture(null);

		final OOCCacheUtils.TileHandle mHandle = handle;
		return reserve(mHandle.bytes())
			.thenCompose(ignored -> mHandle.read(this))
			.thenCompose(callback -> {
				if(callback == null) {
					release(mHandle.bytes());
					return CompletableFuture.completedFuture(null);
				}

				OOCStream.QueueCallback<IndexedMatrixValue> installed = installReadResult(entry, mHandle, callback);
				if(installed != null) {
					OOCCacheUtils.noteEscape(installed, "CachedAllowance.get[index=" + index + "]");
					return CompletableFuture.completedFuture(installed);
				}
				return get(index);
			});
	}

	public CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> take(int index) {
		SlotEntry expectedEntry = getSlot(index);
		if(expectedEntry == null)
			return CompletableFuture.completedFuture(null);

		return get(index).thenApply(callback -> {
			if(callback == null)
				return null;

			SlotEntry entry = removeSlot(index, expectedEntry);
			if(entry == null) {
				callback.close();
				return null;
			}

			OOCStream.QueueCallback<IndexedMatrixValue> localToClose;
			OOCCacheUtils.TileHandle handle;
			synchronized(entry) {
				localToClose = entry._local;
				handle = entry._handle;
				entry._local = null;
				entry._handle = null;
				entry._state = SlotEntry.STATE_CLEARED;
			}

			if(localToClose != null) {
				if(localToClose != callback)
					localToClose.close();
				localToClose.forget();
			}
			if(handle != null)
				handle.close();
			OOCDebug.trace(() -> "[CACHE-TAKE] cache=" + dbgId() + " idx=" + index
				+ " returned=" + cbId(callback) + " hadLocal=" + (localToClose != null)
				+ " hadHandle=" + (handle != null) + " slots=" + slotCount());
			OOCCacheUtils.noteEscape(callback, "CachedAllowance.take[index=" + index + "]");
			return callback;
		});
	}

	public void clear(int index) {
		SlotEntry entry = removeSlot(index, null);
		if(entry == null)
			return;

		OOCStream.QueueCallback<IndexedMatrixValue> localToClose;
		OOCCacheUtils.TileHandle handle;
		CompletableFuture<Void> reservationFuture;
		boolean discardPayload;
		synchronized(entry) {
			localToClose = entry._local;
			handle = entry._handle;
			reservationFuture = entry._reservationFuture;
			discardPayload = localToClose instanceof InMemoryQueueCallback;
			entry._local = null;
			entry._handle = null;
			entry._state = SlotEntry.STATE_CLEARED;
		}

		if(discardPayload && localToClose instanceof InMemoryQueueCallback inMemory)
			inMemory.get().discard();
		if(localToClose != null) {
			localToClose.close();
			localToClose.forget();
		}
		if(handle != null)
			handle.close();
		if(reservationFuture != null)
			reservationFuture.complete(null);
		OOCDebug.trace(() -> "[CACHE-CLEAR] cache=" + dbgId() + " idx=" + index
			+ " hadLocal=" + (localToClose != null) + " hadHandle=" + (handle != null)
			+ " slots=" + slotCount());
	}

	@Override
	public boolean tryReserve(long bytes) {
		return super.tryReserve(bytes);
	}

	@Override
	public void reserveBlocking(long bytes) {
		admitBlocking(bytes);
	}

	@Override
	public void setTargetMemory(long targetMemory) {
		super.setTargetMemory(targetMemory);
		maybeScheduleHandovers(0);
	}

	public void admitBlocking(long bytes) {
		long requestedBytes = Math.max(bytes, MIN_HANDOVER_BATCH);
		while(true) {
			if(super.tryReserve(bytes))
				return;
			maybeScheduleHandovers(requestedBytes);
			if(super.tryReserve(bytes))
				return;
			if(_shutdown || _destroyed)
				throw new IllegalStateException("Cannot reserve memory on closed allowance.");
			synchronized(this) {
				if(_shutdown || _destroyed)
					throw new IllegalStateException("Cannot reserve memory on closed allowance.");
				try {
					wait();
				}
				catch(InterruptedException e) {
					throw new DMLRuntimeException(e);
				}
			}
		}
	}

	void onFinishedHandover(long bytes) {
		synchronized(this) {
			_pendingHandoverBytes -= bytes;
			if(_pendingHandoverBytes < 0)
				throw new IllegalStateException();
			notifyAll();
		}
		OOCDebug.trace(() -> "[CACHE-HANDOVER-FINISHED] cache=" + dbgId() + " bytes=" + bytes
			+ " pending=" + _pendingHandoverBytes + " slots=" + slotCount());
		maybeScheduleHandovers(0);
	}

	private OOCStream.QueueCallback<IndexedMatrixValue> installReadResult(SlotEntry entry, OOCCacheUtils.TileHandle handle,
		OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		OOCStream.QueueCallback<IndexedMatrixValue> existing = null;
		OOCStream.QueueCallback<IndexedMatrixValue> retained = OOCCacheUtils.retainLocal(callback);
		boolean install = false;
		synchronized(entry) {
			if(entry._state == SlotEntry.STATE_HANDLE && entry._handle == handle && entry._local == null) {
				entry._local = retained.keepOpen();
				entry._handle = null;
				entry._state = SlotEntry.STATE_LOCAL;
				install = true;
			}
			else if(entry._local != null) {
				existing = entry._local.keepOpen();
			}
		}

		if(install) {
			handle.close();
			OOCDebug.trace(() -> "[CACHE-READ-INSTALL] cache=" + dbgId() + " handle=" + handle
				+ " local=" + cbId(retained) + " slots=" + slotCount());
			return retained;
		}

		retained.close();
		retained.forget();
		return existing;
	}

	private void maybeScheduleHandovers(long requestedBytes) {
		synchronized(this) {
			_handoverSchedulingRequested = true;
			_handoverSchedulingRequestedBytes = Math.max(_handoverSchedulingRequestedBytes, requestedBytes);
			if(_handoverScheduling)
				return;
			_handoverScheduling = true;
		}

		try {
			while(true) {
				long reclaimGoal;
				long effectiveRequestedBytes;
				int startIndex;
				synchronized(this) {
					_handoverSchedulingRequested = false;
					effectiveRequestedBytes = Math.max(requestedBytes, _handoverSchedulingRequestedBytes);
					_handoverSchedulingRequestedBytes = 0;
					if(_shutdown || _destroyed)
						return;
					long capacity = Math.min(_targetBytes, _grantedBytes);
					long excess = _usedBytes + effectiveRequestedBytes - capacity - _pendingHandoverBytes;
					if(excess <= 0) {
						if(!_handoverSchedulingRequested)
							return;
						continue;
					}

					long slack = Math.max(MIN_HANDOVER_SLACK, Math.min(MAX_HANDOVER_SLACK, _targetBytes / 16));
					reclaimGoal = Math.max(excess + slack, MIN_HANDOVER_BATCH);
					startIndex = _highestPopulatedIndex;
				}

				AtomicReferenceArray<SlotEntry> slots = _slots;
				int newHighest = startIndex;
				for(int i = Math.min(startIndex, slots.length() - 1); i >= 0; i--) {
					if(slots.get(i) != null) {
						newHighest = i;
						break;
					}
				}

				for(int i = newHighest; i >= 0 && reclaimGoal > 0; i--) {
					long bytes = tryStartCacheHandover(slots.get(i));
					if(bytes <= 0)
						continue;
					reclaimGoal -= bytes;
				}

				synchronized(this) {
					if(newHighest < _highestPopulatedIndex)
						_highestPopulatedIndex = newHighest;
					if(!_handoverSchedulingRequested)
						return;
				}
			}
		}
		finally {
			synchronized(this) {
				_handoverScheduling = false;
			}
		}
	}

	private long tryStartCacheHandover(SlotEntry entry) {
		if(entry == null)
			return 0;

		OOCStream.QueueCallback<IndexedMatrixValue> local;
		long bytes;
		CompletableFuture<OOCCacheUtils.TileHandle> future;
		BlockKey targetKey;
		synchronized(entry) {
			local = entry._local;
			if(local == null || entry._state != SlotEntry.STATE_LOCAL)
				return 0;
			bytes = local.getManagedBytes();
			if(bytes <= 0)
				return 0;

			targetKey = local.getBackingPin() != null ? local.getBlockKey() :
				new BlockKey(_streamId, _nextBlockId.getAndIncrement());
			future = OOCCacheUtils.spill(local, targetKey, bytes);
			entry._state = SlotEntry.STATE_SPILLING;
			entry._spillFuture = future;
			entry._spillBytes = bytes;
		}

		synchronized(this) {
			_pendingHandoverBytes += bytes;
		}
		OOCDebug.trace(() -> "[CACHE-SPILL-START] cache=" + dbgId() + " local=" + cbId(local)
			+ " bytes=" + bytes + " targetKey=" + targetKey + " pending=" + _pendingHandoverBytes);
		future.whenComplete((handle, ex) -> onSpillCompleted(entry, future, handle, ex));
		return bytes;
	}

	private void onSpillCompleted(SlotEntry entry, CompletableFuture<OOCCacheUtils.TileHandle> future,
		OOCCacheUtils.TileHandle handle, Throwable ex) {
		OOCStream.QueueCallback<IndexedMatrixValue> localToClose = null;
		OOCCacheUtils.TileHandle handleToClose = null;
		long pendingBytes;
		synchronized(entry) {
			if(entry._spillFuture != future)
				return;

			pendingBytes = entry._spillBytes;
			entry._spillBytes = 0;
			entry._spillFuture = null;

			if(entry._state == SlotEntry.STATE_CLEARED) {
				if(ex == null && handle != null)
					handleToClose = handle;
			}
			else if(ex == null && handle != null) {
				localToClose = entry._local;
				entry._local = null;
				entry._handle = handle;
				entry._state = SlotEntry.STATE_HANDLE;
			}
			else {
				entry._state = SlotEntry.STATE_LOCAL;
			}
		}

		if(localToClose != null) {
			localToClose.close();
			localToClose.forget();
		}
		if(handleToClose != null)
			handleToClose.close();
		final var fLocalToClose = localToClose;
		OOCDebug.trace(() -> "[CACHE-SPILL-DONE] cache=" + dbgId() + " success=" + (ex == null && handle != null)
			+ " handle=" + handle + " hadLocal=" + (fLocalToClose != null)
			+ " pendingBytes=" + pendingBytes + " slots=" + slotCount());
		if(pendingBytes > 0)
			onFinishedHandover(pendingBytes);
	}

	private SlotEntry getSlot(int index) {
		AtomicReferenceArray<SlotEntry> slots = _slots;
		if(index < 0 || index >= slots.length())
			return null;
		return slots.get(index);
	}

	private SlotEntry removeSlot(int index, SlotEntry expectedEntry) {
		synchronized(this) {
			AtomicReferenceArray<SlotEntry> slots = _slots;
			if(index < 0 || index >= slots.length())
				return null;
			SlotEntry entry = slots.get(index);
			if(expectedEntry != null && entry != expectedEntry)
				return null;
			if(entry != null)
				slots.set(index, null);
			return entry;
		}
	}

	private SlotEntry tryReserveCacheSlot(int index) {
		synchronized(this) {
			ensureCapacity(index);
			AtomicReferenceArray<SlotEntry> slots = _slots;
			if(slots.get(index) != null)
				return null;
			SlotEntry reservation = new SlotEntry();
			slots.set(index, reservation);
			if(index > _highestPopulatedIndex)
				_highestPopulatedIndex = index;
			return reservation;
		}
	}

	private CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> finishReservedHandover(
		OOCStream.QueueCallback<IndexedMatrixValue> callback, int index, SlotEntry reservation) {
		return OOCCacheUtils.handover(callback, this).thenApply(owned -> {
			OOCStream.QueueCallback<IndexedMatrixValue> retained = OOCCacheUtils.retainLocal(owned.keepOpen());
			owned.close();
			CompletableFuture<Void> stateFuture;
			synchronized(reservation) {
				stateFuture = reservation._reservationFuture;
				reservation._local = retained;
				reservation._reservationFuture = null;
				reservation._state = SlotEntry.STATE_LOCAL;
			}
			if(stateFuture != null)
				stateFuture.complete(null);
			maybeScheduleHandovers(MIN_HANDOVER_BATCH);
			return (OOCStream.QueueCallback<IndexedMatrixValue>) null;
		}).exceptionally(ex -> {
			Throwable cause = ex.getCause() == null ? ex : ex.getCause();
			RuntimeException failure = cause instanceof RuntimeException ? (RuntimeException) cause :
				new RuntimeException(cause);
			failReservedCacheSlot(index, reservation, failure);
			throw failure;
		});
	}

	private void failReservedCacheSlot(int index, SlotEntry reservation, RuntimeException ex) {
		synchronized(this) {
			AtomicReferenceArray<SlotEntry> slots = _slots;
			if(index >= 0 && index < slots.length() && slots.get(index) == reservation)
				slots.set(index, null);
		}

		CompletableFuture<Void> stateFuture;
		synchronized(reservation) {
			stateFuture = reservation._reservationFuture;
			reservation._reservationFuture = null;
			reservation._state = SlotEntry.STATE_CLEARED;
		}
		if(stateFuture != null)
			stateFuture.completeExceptionally(ex);
	}

	private void ensureCapacity(int index) {
		AtomicReferenceArray<SlotEntry> slots = _slots;
		if(index < slots.length())
			return;
		int newLen = slots.length();
		while(index >= newLen)
			newLen *= 2;
		AtomicReferenceArray<SlotEntry> grown = new AtomicReferenceArray<>(newLen);
		for(int i = 0; i < slots.length(); i++)
			grown.set(i, slots.get(i));
		_slots = grown;
	}

	private static final class SlotEntry {
		private static final byte STATE_LOCAL = 0;
		private static final byte STATE_SPILLING = 1;
		private static final byte STATE_HANDLE = 2;
		private static final byte STATE_CLEARED = 3;
		private static final byte STATE_RESERVED = 4;

		private byte _state;
		private OOCStream.QueueCallback<IndexedMatrixValue> _local;
		private OOCCacheUtils.TileHandle _handle;
		private CompletableFuture<Void> _reservationFuture;
		private CompletableFuture<OOCCacheUtils.TileHandle> _spillFuture;
		private long _spillBytes;

		private SlotEntry() {
			_state = STATE_RESERVED;
			_reservationFuture = new CompletableFuture<>();
		}

		private SlotEntry(OOCStream.QueueCallback<IndexedMatrixValue> local) {
			_state = STATE_LOCAL;
			_local = local;
		}
	}

	private String dbgId() {
		return getClass().getSimpleName() + "@" + System.identityHashCode(this);
	}

	private static String cbId(OOCStream.QueueCallback<IndexedMatrixValue> cb) {
		if(cb == null)
			return "null";
		return cb.getClass().getSimpleName() + "@" + System.identityHashCode(cb);
	}

	private int slotCount() {
		int count = 0;
		AtomicReferenceArray<SlotEntry> slots = _slots;
		for(int i = 0; i < slots.length(); i++) {
			if(slots.get(i) != null)
				count++;
		}
		return count;
	}
}
