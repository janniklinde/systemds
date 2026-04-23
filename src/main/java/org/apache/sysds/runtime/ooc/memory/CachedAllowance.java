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
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCCacheScheduler;

import java.lang.ref.SoftReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
		_pendingHandoverBytes = 0;
		_highestPopulatedIndex = -1;
		_handoverScheduling = false;
		_handoverSchedulingRequested = false;
		_handoverSchedulingRequestedBytes = 0;
	}

	public void handover(OOCStream.QueueCallback<IndexedMatrixValue> callback, int index) {
		OOCStream.QueueCallback<IndexedMatrixValue> owned = callback.transferOwnershipBlocking(this);
		OOCStream.QueueCallback<IndexedMatrixValue> root = owned.keepOpen();
		owned.close();
		if(root instanceof InMemoryQueueCallback inMemoryRoot)
			inMemoryRoot.getHandle().attachCachedAllowance(this, index);

		SlotEntry entry = new SlotEntry(root);
		synchronized(this) {
			ensureCapacity(index);
			AtomicReferenceArray<SlotEntry> slots = _slots;
			if(slots.get(index) != null) {
				closeRoot(root);
				throw new IllegalStateException("Cached allowance slot " + index + " already occupied.");
			}
			slots.set(index, entry);
			if(index > _highestPopulatedIndex)
				_highestPopulatedIndex = index;
		}
		maybeScheduleHandovers(MIN_HANDOVER_BATCH);
	}

	public OOCStream.QueueCallback<IndexedMatrixValue> tryGet(int index) {
		SlotEntry entry = getSlot(index);
		if(entry == null)
			return null;

		while(true) {
			BlockKey schedulerKey = null;
			byte state;
			CompletableFuture<Void> spillFuture = null;
			OOCStream.QueueCallback<IndexedMatrixValue> local = null;
			long bytes = 0;

			synchronized(entry) {
				if(entry._local != null)
					local = entry._local.keepOpen();
				else if(entry._softLocal != null) {
					IndexedMatrixValue softLocal = entry._softLocal.get();
					if(softLocal != null)
						local = new OOCStream.SimpleQueueCallback<>(softLocal, null);
					else
						entry._softLocal = null;
				}
				if(local == null) {
					state = entry._state;
					if(state == SlotEntry.STATE_DIRECT_SPILLING) {
						spillFuture = entry._spillFuture;
					}
					else if(state == SlotEntry.STATE_SCHEDULER_BACKED) {
						schedulerKey = entry._key;
						bytes = entry._bytes;
					}
					else if(state != SlotEntry.STATE_BACKEND_SPILLED)
						return null;
				}
			}

			if(local != null)
				return local;

			if(spillFuture != null) {
				if(!spillFuture.isDone())
					return null;
				try {
					spillFuture.join();
				}
				catch(CompletionException ex) {
					throw DMLRuntimeException.of(ex.getCause() == null ? ex : ex.getCause());
				}
				finishSpill(entry, spillFuture);
				continue;
			}

			if(schedulerKey != null && bytes > 0) {
				if(!super.tryReserve(bytes))
					return null;
				OOCStream.QueueCallback<IndexedMatrixValue> callback =
					OOCCacheManager.tryRequestBlockBacked(schedulerKey, this, bytes);
				if(callback == null) {
					release(bytes);
					return null;
				}
				synchronized(entry) {
					if(entry._state == SlotEntry.STATE_SCHEDULER_BACKED && schedulerKey.equals(entry._key) && entry._local == null) {
						entry._local = callback.keepOpen();
						entry._state = SlotEntry.STATE_LOCAL;
						entry._key = null;
						entry._bytes = 0;
					}
				}
				return callback;
			}

			return null;
		}
	}

	public CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> get(int index) {
		OOCStream.QueueCallback<IndexedMatrixValue> immediate = tryGet(index);
		if(immediate != null)
			return CompletableFuture.completedFuture(immediate);

		SlotEntry entry = getSlot(index);
		if(entry == null)
			return CompletableFuture.completedFuture(null);

		CompletableFuture<Void> spillFuture;
		BlockKey key;
		byte state;
		long bytes;
		synchronized(entry) {
			if(entry._local != null)
				return CompletableFuture.completedFuture(entry._local.keepOpen());
			if(entry._softLocal != null) {
				IndexedMatrixValue softLocal = entry._softLocal.get();
				if(softLocal != null)
					return CompletableFuture.completedFuture(new OOCStream.SimpleQueueCallback<>(softLocal, null));
				entry._softLocal = null;
			}
			state = entry._state;
			spillFuture = entry._spillFuture;
			key = entry._key;
			bytes = entry._bytes;
		}

		if(state == SlotEntry.STATE_DIRECT_SPILLING && spillFuture != null) {
			return spillFuture.handle((ignored, ex) -> {
				if(ex != null)
					throw DMLRuntimeException.of(ex.getCause() == null ? ex : ex.getCause());
				return true;
			}).thenCompose(ignored -> {
				BlockKey mKey = finishSpill(entry, spillFuture);
				if(mKey == null)
					return get(index);
				return readFromBackend(entry, mKey);
			});
		}

		if(state == SlotEntry.STATE_BACKEND_SPILLED && key != null)
			return readFromBackend(entry, key);
		if(state == SlotEntry.STATE_SCHEDULER_BACKED && key != null && bytes > 0)
			return readFromScheduler(entry, key, bytes);
		return CompletableFuture.completedFuture(null);
	}

	public void clear(int index) {
		SlotEntry entry = removeSlot(index);
		if(entry == null)
			return;

		CompletableFuture<Void> spillFuture;
		BlockKey forgetKey;
		OOCStream.QueueCallback<IndexedMatrixValue> localToClose;
		OOCCacheScheduler.BackingReleaseHandle backingReleaseHandle;
		long pendingBytes;

		synchronized(entry) {
			pendingBytes = takePendingBytes(entry);
			localToClose = entry._local;
			entry._local = null;
			spillFuture = entry._spillFuture;
			entry._spillFuture = null;
			backingReleaseHandle = entry._backingReleaseHandle;
			entry._backingReleaseHandle = null;
			forgetKey = entry._state == SlotEntry.STATE_DIRECT_SPILLING || entry._state == SlotEntry.STATE_BACKEND_SPILLED ?
				entry._key : null;
			entry._state = SlotEntry.STATE_CLEARED;
			entry._key = null;
			entry._bytes = 0;
			entry._softLocal = null;
		}

		if(spillFuture != null)
			spillFuture.cancel(false);
		if(backingReleaseHandle != null) {
			OOCCacheScheduler.AllowanceBackedPin reclaimed = backingReleaseHandle.reclaim();
			if(reclaimed != null)
				reclaimed.close();
		}
		if(localToClose instanceof InMemoryQueueCallback inMemory)
			discardPayload(inMemory);
		closeLocalAndFinishHandover(localToClose, pendingBytes);
		if(forgetKey != null)
			OOCCacheManager.getTileStoreBackend().delete(forgetKey);
	}

	@Override
	public boolean tryReserve(long bytes) {
		throw new UnsupportedOperationException("CachedAllowance does not support direct reservations. Use handover(...).");
	}

	@Override
	public void reserveBlocking(long bytes) {
		throw new UnsupportedOperationException("CachedAllowance does not support direct reservations. Use handover(...).");
	}

	@Override
	public void setTargetMemory(long targetMemory) {
		super.setTargetMemory(targetMemory);
		maybeScheduleHandovers(0);
	}

	void onFinishedHandover(long bytes) {
		synchronized(this) {
			_pendingHandoverBytes -= bytes;
			if(_pendingHandoverBytes < 0)
				throw new IllegalStateException();
			notifyAll();
		}
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
				// Find highes non-null entry
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
		synchronized(entry) {
			local = entry._local;
			if(local == null || entry._state != SlotEntry.STATE_LOCAL)
				return 0;
		}

		if(local instanceof InMemoryQueueCallback inMemory)
			return tryStartDirectCacheHandover(entry, inMemory);

		OOCCacheScheduler.AllowanceBackedPin pin = local.getBackingPin();
		if(pin != null)
			return tryStartBackingRelease(entry, local, pin);
		return 0;
	}

	private long tryStartDirectCacheHandover(SlotEntry entry, InMemoryQueueCallback local) {
		synchronized(entry) {
			if(entry._local != local || entry._state != SlotEntry.STATE_LOCAL || !local.getHandle().isExclusiveToRoot())
				return 0;

			long bytes = local.getManagedBytes();
			if(bytes <= 0)
				return 0;

			InMemoryQueueCallback retained = (InMemoryQueueCallback) local.keepOpen();
			try {
				entry._state = SlotEntry.STATE_DIRECT_SPILLING;
				entry._key = new BlockKey(_streamId, _nextBlockId.getAndIncrement());
				entry._spillFuture = OOCCacheManager.getTileStoreBackend().spill(entry._key, retained.get());
				entry._bytes = bytes;
				synchronized(this) {
					_pendingHandoverBytes += bytes;
				}
				retained.close();
				entry._spillFuture.whenComplete((ignored, ex) -> onHandoverCompleted(entry));
				return bytes;
			}
			catch(RuntimeException ex) {
				entry._state = SlotEntry.STATE_LOCAL;
				entry._key = null;
				entry._spillFuture = null;
				entry._bytes = 0;
				retained.close();
				throw ex;
			}
		}
	}

	private long tryStartBackingRelease(SlotEntry entry, OOCStream.QueueCallback<IndexedMatrixValue> local,
		OOCCacheScheduler.AllowanceBackedPin pin) {
		BlockKey key;
		long bytes;
		synchronized(entry) {
			if(entry._local != local || entry._state != SlotEntry.STATE_LOCAL || local.getBackingPin() != pin)
				return 0;
			key = local.getBlockKey();
			bytes = local.getManagedBytes();
			if(key == null || bytes <= 0)
				return 0;
		}

		OOCCacheScheduler.AllowanceBackedPin releasePin = pin.keepOpen();
		OOCCacheScheduler.BackingReleaseHandle handle;
		try {
			handle = OOCCacheManager.getCache().releaseBacking(releasePin);
		}
		catch(RuntimeException ex) {
			releasePin.close();
			throw ex;
		}

		OOCCacheScheduler.AllowanceBackedPin reclaimed = null;
		boolean abandoned = false;
		synchronized(entry) {
			if(entry._local != local || entry._state != SlotEntry.STATE_LOCAL || local.getBackingPin() != pin) {
				reclaimed = handle.reclaim();
				abandoned = true;
			}
			else if(!handle.isCommitted()) {
				entry._state = SlotEntry.STATE_BACKING_RELEASE_PENDING;
				entry._key = key;
				entry._backingReleaseHandle = handle;
				entry._bytes = bytes;
				synchronized(this) {
					_pendingHandoverBytes += bytes;
				}
				handle.getCompletionFuture()
					.whenComplete((committed, ex) -> onBackingReleaseCompleted(entry, handle, key, bytes, committed, ex));
				return bytes;
			}
			else {
				entry._state = SlotEntry.STATE_SCHEDULER_BACKED;
				entry._key = key;
				entry._bytes = bytes;
				entry._local = null;
			}
		}
		if(reclaimed != null) {
			reclaimed.close();
			return 0;
		}
		if(abandoned)
			return 0;
		local.close();
		return bytes;
	}

	private void onBackingReleaseCompleted(SlotEntry entry, OOCCacheScheduler.BackingReleaseHandle handle, BlockKey key,
		long bytes, Boolean committed, Throwable ex) {
		OOCStream.QueueCallback<IndexedMatrixValue> localToClose = null;
		long pendingBytes;
		synchronized(entry) {
			if(entry._backingReleaseHandle != handle)
				return;
			pendingBytes = takePendingBytes(entry);
			entry._backingReleaseHandle = null;
			if(ex == null && Boolean.TRUE.equals(committed)) {
				localToClose = entry._local;
				entry._local = null;
				entry._state = SlotEntry.STATE_SCHEDULER_BACKED;
				entry._key = key;
				entry._bytes = bytes;
			}
			else {
				entry._state = SlotEntry.STATE_LOCAL;
				entry._key = null;
			}
		}
		closeLocalAndFinishHandover(localToClose, pendingBytes);
	}

	private void onHandoverCompleted(SlotEntry entry) {
		OOCStream.QueueCallback<IndexedMatrixValue> localToClose = null;
		long pendingBytes;
		synchronized(entry) {
			if(entry._bytes <= 0)
				return;
			pendingBytes = takePendingBytes(entry);
			if(entry._state == SlotEntry.STATE_DIRECT_SPILLING && entry._spillFuture != null &&
				entry._spillFuture.isDone() && !entry._spillFuture.isCompletedExceptionally()) {
				localToClose = entry._local;
				if(localToClose instanceof InMemoryQueueCallback inMemory)
					retainSoftLocal(entry, inMemory);
				entry._local = null;
				entry._spillFuture = null;
				entry._state = SlotEntry.STATE_BACKEND_SPILLED;
			}
		}
		closeLocalAndFinishHandover(localToClose, pendingBytes);
	}

	private BlockKey finishSpill(SlotEntry entry, CompletableFuture<Void> spillFuture) {
		OOCStream.QueueCallback<IndexedMatrixValue> localToClose = null;
		long pendingBytes;
		BlockKey key;
		synchronized(entry) {
			if(entry._spillFuture != spillFuture)
				return null;
			pendingBytes = takePendingBytes(entry);
			localToClose = entry._local;
			if(localToClose instanceof InMemoryQueueCallback inMemory)
				retainSoftLocal(entry, inMemory);
			entry._local = null;
			entry._spillFuture = null;
			entry._state = SlotEntry.STATE_BACKEND_SPILLED;
			key = entry._key;
		}
		closeLocalAndFinishHandover(localToClose, pendingBytes);
		return key;
	}

	private CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> readFromBackend(SlotEntry entry, BlockKey key) {
		return OOCCacheManager.getTileStoreBackend().read(key)
			.thenApply(imv -> {
				if(imv == null)
					return null;
				synchronized(entry) {
					if(entry._state == SlotEntry.STATE_BACKEND_SPILLED && key.equals(entry._key))
						entry._softLocal = new SoftReference<>(imv);
				}
				return new OOCStream.SimpleQueueCallback<>(imv, null);
			});
	}

	private CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> readFromScheduler(SlotEntry entry,
		BlockKey key, long bytes) {
		admitBlocking(bytes);
		return OOCCacheManager.requestBlockBacked(key, this, bytes)
			.thenApply(callback -> {
				if(callback == null)
					return null;
				synchronized(entry) {
					if(entry._state == SlotEntry.STATE_SCHEDULER_BACKED && key.equals(entry._key) && entry._local == null) {
						entry._local = callback.keepOpen();
						entry._state = SlotEntry.STATE_LOCAL;
						entry._key = null;
						entry._bytes = 0;
					}
				}
				return callback;
			});
	}

	private void retainSoftLocal(SlotEntry entry, InMemoryQueueCallback local) {
		if(local == null)
			return;
		entry._softLocal = new SoftReference<>(local.get());
	}

	private long takePendingBytes(SlotEntry entry) {
		if(entry._state != SlotEntry.STATE_DIRECT_SPILLING && entry._state != SlotEntry.STATE_BACKING_RELEASE_PENDING)
			return 0;
		if(entry._bytes <= 0)
			return 0;
		long bytes = entry._bytes;
		entry._bytes = 0;
		return bytes;
	}

	private void closeLocalAndFinishHandover(OOCStream.QueueCallback<IndexedMatrixValue> localToClose, long pendingBytes) {
		RuntimeException closeFailure = null;
		try {
			if(localToClose != null)
				closeRoot(localToClose);
		}
		catch(RuntimeException ex) {
			closeFailure = ex;
		}
		finally {
			if(pendingBytes > 0)
				onFinishedHandover(pendingBytes);
		}
		if(closeFailure != null)
			throw closeFailure;
	}

	private void discardPayload(InMemoryQueueCallback local) {
		if(local == null)
			return;
		IndexedMatrixValue imv = local.get();
		imv.discard();
	}

	private void closeRoot(OOCStream.QueueCallback<IndexedMatrixValue> local) {
		if(local instanceof InMemoryQueueCallback inMemory)
			inMemory.getHandle().detachCachedAllowance();
		local.close();
	}

	private SlotEntry getSlot(int index) {
		AtomicReferenceArray<SlotEntry> slots = _slots;
		if(index < 0 || index >= slots.length())
			return null;
		return slots.get(index);
	}

	private SlotEntry removeSlot(int index) {
		synchronized(this) {
			AtomicReferenceArray<SlotEntry> slots = _slots;
			if(index < 0 || index >= slots.length())
				return null;
			SlotEntry entry = slots.get(index);
			if(entry != null)
				slots.set(index, null);
			return entry;
		}
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
		private final static byte STATE_LOCAL = 0;
		private final static byte STATE_DIRECT_SPILLING = 1;
		private final static byte STATE_BACKEND_SPILLED = 2;
		private final static byte STATE_BACKING_RELEASE_PENDING = 3;
		private final static byte STATE_SCHEDULER_BACKED = 4;
		private final static byte STATE_CLEARED = 5;

		private byte _state;
		private OOCStream.QueueCallback<IndexedMatrixValue> _local;
		private SoftReference<IndexedMatrixValue> _softLocal;
		private BlockKey _key;
		private CompletableFuture<Void> _spillFuture;
		private OOCCacheScheduler.BackingReleaseHandle _backingReleaseHandle;
		private long _bytes;

		private SlotEntry(OOCStream.QueueCallback<IndexedMatrixValue> local) {
			_state = STATE_LOCAL;
			_local = local;
		}
	}
}
