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
		super(broker);
		_streamId = CachingStream._streamSeq.getNextID();
		_slots = new AtomicReferenceArray<>(INITIAL_SLOTS);
		_nextBlockId = new AtomicLong(0);
		_pendingHandoverBytes = 0;
		_highestPopulatedIndex = -1;
		_handoverScheduling = false;
		_handoverSchedulingRequested = false;
		_handoverSchedulingRequestedBytes = 0;
	}

	public void handover(InMemoryQueueCallback callback, int index) {
		if(callback == null)
			throw new IllegalArgumentException("Cannot hand over null callback.");
		callback.transferOwnershipBlocking(this);

		InMemoryQueueCallback root = (InMemoryQueueCallback) callback.keepOpen();
		callback.close();
		root.getHandle().attachCachedAllowance(this, index);

		SlotEntry entry = new SlotEntry(root);
		synchronized(this) {
			ensureCapacity(index);
			AtomicReferenceArray<SlotEntry> slots = _slots;
			if(slots.get(index) != null) {
				root.getHandle().detachCachedAllowance();
				root.close();
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
			BlockKey cacheKey = null;
			CompletableFuture<Void> spillFuture = null;
			OOCStream.QueueCallback<IndexedMatrixValue> local = null;

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
				if(local == null && entry._spillFuture != null) {
					spillFuture = entry._spillFuture;
					cacheKey = entry._cacheKey;
				}
				else if(local == null && entry._cacheKey != null)
					cacheKey = entry._cacheKey;
				else if(local == null)
					return null;
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
		BlockKey cacheKey;
		synchronized(entry) {
			if(entry._local != null)
				return CompletableFuture.completedFuture(entry._local.keepOpen());
			if(entry._softLocal != null) {
				IndexedMatrixValue softLocal = entry._softLocal.get();
				if(softLocal != null)
					return CompletableFuture.completedFuture(new OOCStream.SimpleQueueCallback<>(softLocal, null));
				entry._softLocal = null;
			}
			spillFuture = entry._spillFuture;
			cacheKey = entry._cacheKey;
		}

		if(spillFuture != null) {
			return spillFuture.handle((ignored, ex) -> {
				if(ex != null)
					throw DMLRuntimeException.of(ex.getCause() == null ? ex : ex.getCause());
				return true;
				}).thenCompose(ignored -> {
					BlockKey key = finishSpill(entry, spillFuture);
					if(key == null)
						return get(index);
					return readFromBackend(entry, key);
				});
			}

			if(cacheKey != null)
				return readFromBackend(entry, cacheKey);
		return CompletableFuture.completedFuture(null);
	}

	public void clear(int index) {
		SlotEntry entry = removeSlot(index);
		if(entry == null)
			return;

		CompletableFuture<Void> spillFuture;
		BlockKey forgetKey;
		InMemoryQueueCallback localToClose;
		long pendingBytes;

		synchronized(entry) {
			pendingBytes = takePendingHandoverBytes(entry);
			localToClose = entry._local;
			entry._local = null;
			spillFuture = entry._spillFuture;
			entry._spillFuture = null;
			forgetKey = entry._cacheKey;
			entry._cacheKey = null;
			entry._softLocal = null;
		}

		if(spillFuture != null)
			spillFuture.cancel(false);
		discardPayload(localToClose);
		closeRootAndFinishHandover(localToClose, pendingBytes);
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

	void admitBlocking(long bytes) {
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
		synchronized(entry) {
			if(entry._local == null || entry._spillFuture != null || !entry._local.getHandle().isExclusiveToRoot())
				return 0;

			long bytes = entry._local.getManagedBytes();
			if(bytes <= 0)
				return 0;

			InMemoryQueueCallback retained = (InMemoryQueueCallback) entry._local.keepOpen();
			try {
				entry._cacheKey = new BlockKey(_streamId, _nextBlockId.getAndIncrement());
				entry._spillFuture = OOCCacheManager.getTileStoreBackend().spill(entry._cacheKey, retained.get());
				entry._pendingBytes = bytes;
				synchronized(this) {
					_pendingHandoverBytes += bytes;
				}
				retained.close();
				entry._spillFuture.whenComplete((ignored, ex) -> onHandoverCompleted(entry));
				return bytes;
			}
			catch(RuntimeException ex) {
				entry._cacheKey = null;
				entry._spillFuture = null;
				entry._pendingBytes = 0;
				retained.close();
				throw ex;
			}
		}
	}

	private void onHandoverCompleted(SlotEntry entry) {
		InMemoryQueueCallback localToClose = null;
		long pendingBytes;
		synchronized(entry) {
			if(entry._pendingBytes <= 0)
				return;
			pendingBytes = takePendingHandoverBytes(entry);
			if(entry._spillFuture != null && entry._spillFuture.isDone() && !entry._spillFuture.isCompletedExceptionally()) {
				localToClose = entry._local;
				retainSoftLocal(entry, localToClose);
				entry._local = null;
				entry._spillFuture = null;
			}
		}
		closeRootAndFinishHandover(localToClose, pendingBytes);
	}

	private BlockKey finishSpill(SlotEntry entry, CompletableFuture<Void> spillFuture) {
		InMemoryQueueCallback localToClose = null;
		long pendingBytes;
		BlockKey key;
		synchronized(entry) {
			if(entry._spillFuture != spillFuture)
				return null;
			pendingBytes = takePendingHandoverBytes(entry);
			localToClose = entry._local;
			retainSoftLocal(entry, localToClose);
			entry._local = null;
			entry._spillFuture = null;
			key = entry._cacheKey;
		}
		closeRootAndFinishHandover(localToClose, pendingBytes);
		return key;
	}

	private CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> readFromBackend(SlotEntry entry, BlockKey key) {
		return OOCCacheManager.getTileStoreBackend().read(key)
			.thenApply(imv -> {
				if(imv == null)
					return null;
				synchronized(entry) {
					if(key.equals(entry._cacheKey))
						entry._softLocal = new SoftReference<>(imv);
				}
				return new OOCStream.SimpleQueueCallback<>(imv, null);
			});
	}

	private void retainSoftLocal(SlotEntry entry, InMemoryQueueCallback local) {
		if(local == null)
			return;
		entry._softLocal = new SoftReference<>(local.get());
	}

	private long takePendingHandoverBytes(SlotEntry entry) {
		if(entry._pendingBytes <= 0)
			return 0;
		long bytes = entry._pendingBytes;
		entry._pendingBytes = 0;
		return bytes;
	}

	private void closeRootAndFinishHandover(InMemoryQueueCallback localToClose, long pendingBytes) {
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

	private void closeRoot(InMemoryQueueCallback local) {
		local.getHandle().detachCachedAllowance();
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
		private InMemoryQueueCallback _local;
		private SoftReference<IndexedMatrixValue> _softLocal;
		private BlockKey _cacheKey;
		private CompletableFuture<Void> _spillFuture;
		private long _pendingBytes;

		private SlotEntry(InMemoryQueueCallback local) {
			_local = local;
		}
	}
}
