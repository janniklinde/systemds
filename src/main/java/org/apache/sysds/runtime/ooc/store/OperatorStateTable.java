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

package org.apache.sysds.runtime.ooc.store;

import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mutable online indexed coordination over the global cache: join rendezvous, reduction accumulators,
 * and second-pass retention. The table owns only atomic slot semantics; spilling, loading, eviction
 * order, and resident ownership remain exclusively within {@link OOCCache}. Each table maps its slots
 * onto one cache stream id (so eviction policy sees the table as one population), and slot contents are
 * swapped by key replacement with a per-table generation counter, never by mutating a cache entry.
 *
 * Private values enter through {@link ManagedPayload} (bytes stay charged to the producer allowance
 * until the cache unpin commits). Shared canonical values increment the logical reference of an
 * already pinned cache entry. Every installed slot stores only a {@link BlockKey}; values leave as
 * {@link StateLease}s pinned under the table's region allowance.
 *
 * Slot transitions are serialized on the table monitor; they are O(1) metadata updates, and all cache
 * I/O (putPinned, pin loading, unpin) happens outside the lock. If the single monitor becomes a
 * bottleneck under many independent slots, per-slot striping is the intended follow-up optimization.
 */
public final class OperatorStateTable<T extends SpillableObject> implements AutoCloseable {
	private static final int INITIAL_SLOTS = 64;

	private final OOCCache _cache;
	private final long _streamId;
	private final MemoryAllowance _allowance;
	private final AtomicLong _nextGeneration;
	private Slot[] _slots;
	private volatile boolean _closed;

	public OperatorStateTable(OOCCache cache, long streamId, MemoryAllowance allowance) {
		this(cache, streamId, allowance, INITIAL_SLOTS);
	}

	public OperatorStateTable(OOCCache cache, long streamId, MemoryAllowance allowance, int numSlots) {
		_cache = cache;
		_streamId = streamId;
		_allowance = allowance;
		_nextGeneration = new AtomicLong();
		_slots = new Slot[Math.max(1, numSlots)];
	}

	/**
	 * Installs the payload into an empty slot, transferring its reservation into the cache ownership
	 * protocol. Throws if the slot is occupied (including by a concurrent in-flight install).
	 */
	public void install(int index, ManagedPayload<T> payload) {
		Slot slot;
		synchronized(this) {
			checkOpen();
			ensureCapacity(index);
			if(_slots[index] != null)
				throw new IllegalStateException("State table slot " + index + " is already occupied.");
			slot = new Slot();
			_slots[index] = slot;
		}
		finishInstall(index, slot, payload);
	}

	/**
	 * Retains an already pinned canonical cache entry in an empty slot. The caller remains responsible
	 * for unpinning the supplied entry; this method adds only a logical lifetime reference.
	 */
	public void installReference(int index, BlockEntry pinned) {
		Slot slot;
		synchronized(this) {
			checkOpen();
			ensureCapacity(index);
			if(_slots[index] != null)
				throw new IllegalStateException("State table slot " + index + " is already occupied.");
			slot = new Slot();
			_slots[index] = slot;
		}
		finishReferenceInstall(index, slot, pinned);
	}

	/**
	 * Atomic install-or-take: if the slot is empty the payload is installed and the future completes
	 * with null; otherwise the previously installed value is removed from the slot and returned as a
	 * lease while the payload remains owned by the caller. Slot transitions are atomic (on the table
	 * monitor), which makes this the rendezvous primitive for joins and the merge primitive for
	 * reduction accumulators (merge outside, retry with the merged payload). A call racing an in-flight
	 * install chains behind it instead of failing.
	 */
	public OOCFuture<StateLease<T>> installOrTake(int index, ManagedPayload<T> payload) {
		Slot installing = null;
		Slot taken = null;
		OOCFuture<Void> waitFor = null;
		synchronized(this) {
			checkOpen();
			ensureCapacity(index);
			Slot existing = _slots[index];
			if(existing == null) {
				installing = new Slot();
				_slots[index] = installing;
			}
			else if(existing._state == Slot.INSTALLED) {
				_slots[index] = null;
				taken = existing;
			}
			else {
				waitFor = existing._installFuture;
			}
		}
		if(installing != null) {
			finishInstall(index, installing, payload);
			return OOCFuture.completed(null);
		}
		if(taken != null)
			return pinTaken(taken);
		OOCFuture<StateLease<T>> result = new OOCFuture<>();
		waitFor.whenComplete((ignored, error) -> retry(() -> installOrTake(index, payload), result));
		return result;
	}

	/**
	 * Atomic reference install-or-take. If the slot is empty, the pinned logical entry is retained and
	 * the future completes with null. Otherwise the existing slot value is removed and returned while
	 * the supplied pinned entry remains untouched. The caller must keep the supplied entry pinned until
	 * the returned future completes.
	 */
	public OOCFuture<StateLease<T>> installReferenceOrTake(int index, BlockEntry pinned) {
		Slot installing = null;
		Slot taken = null;
		OOCFuture<Void> waitFor = null;
		synchronized(this) {
			checkOpen();
			ensureCapacity(index);
			Slot existing = _slots[index];
			if(existing == null) {
				installing = new Slot();
				_slots[index] = installing;
			}
			else if(existing._state == Slot.INSTALLED) {
				_slots[index] = null;
				taken = existing;
			}
			else {
				waitFor = existing._installFuture;
			}
		}
		if(installing != null) {
			finishReferenceInstall(index, installing, pinned);
			return OOCFuture.completed(null);
		}
		if(taken != null)
			return pinTaken(taken);
		OOCFuture<StateLease<T>> result = new OOCFuture<>();
		waitFor.whenComplete((ignored, error) ->
			retry(() -> installReferenceOrTake(index, pinned), result));
		return result;
	}

	/**
	 * Removes the slot and returns its value as a pinned lease, or null if the slot is empty. A call
	 * racing an in-flight install chains behind it. The lease charges the table's region allowance;
	 * closing it releases the value entirely (exactly-once consumption).
	 */
	public OOCFuture<StateLease<T>> take(int index) {
		Slot taken = null;
		OOCFuture<Void> waitFor = null;
		synchronized(this) {
			checkOpen();
			if(index < 0 || index >= _slots.length)
				return OOCFuture.completed(null);
			Slot existing = _slots[index];
			if(existing == null)
				return OOCFuture.completed(null);
			if(existing._state == Slot.INSTALLED) {
				_slots[index] = null;
				taken = existing;
			}
			else {
				waitFor = existing._installFuture;
			}
		}
		if(taken != null)
			return pinTaken(taken);
		OOCFuture<StateLease<T>> result = new OOCFuture<>();
		waitFor.whenComplete((ignored, error) -> retry(() -> take(index), result));
		return result;
	}

	/**
	 * Returns a pinned lease on the slot's value without removing it, if the value is resident and
	 * admissible right now; null otherwise. Never blocks and never schedules I/O.
	 */
	public StateLease<T> peek(int index) {
		BlockKey key;
		synchronized(this) {
			checkOpen();
			if(index < 0 || index >= _slots.length)
				return null;
			Slot slot = _slots[index];
			if(slot == null || slot._state != Slot.INSTALLED)
				return null;
			key = slot._key;
		}
		BlockEntry entry = _cache.pinIfLive(key.getStreamId(), key.getSequenceNumber(), _allowance);
		return entry == null ? null : new TableLease(entry);
	}

	/**
	 * Removes the slot and drops its value.
	 */
	public void clear(int index) {
		Slot removed = null;
		synchronized(this) {
			if(index < 0 || index >= _slots.length)
				return;
			Slot slot = _slots[index];
			if(slot == null)
				return;
			_slots[index] = null;
			if(slot._state == Slot.INSTALLED) {
				slot._state = Slot.REMOVED;
				removed = slot;
			}
			else if(slot._state == Slot.INSTALLING) {
				slot._cleared = true;
			}
		}
		if(removed != null)
			releaseSlot(removed);
	}

	@Override
	public void close() {
		List<Slot> toRelease = new ArrayList<>();
		synchronized(this) {
			if(_closed)
				return;
			_closed = true;
			for(int i = 0; i < _slots.length; i++) {
				Slot slot = _slots[i];
				if(slot == null)
					continue;
				_slots[i] = null;
				if(slot._state == Slot.INSTALLED) {
					slot._state = Slot.REMOVED;
					toRelease.add(slot);
				}
				else if(slot._state == Slot.INSTALLING) {
					slot._cleared = true;
				}
			}
		}
		for(Slot slot : toRelease)
			releaseSlot(slot);
	}

	public boolean isClosed() {
		return _closed;
	}

	private void finishInstall(int index, Slot slot, ManagedPayload<T> payload) {
		BlockKey key = new BlockKey(_streamId, _nextGeneration.getAndIncrement());
		BlockEntry entry;
		try {
			payload.transfer();
		}
		catch(RuntimeException ex) {
			failInstall(index, slot, ex);
			throw ex;
		}
		try {
			entry = _cache.putPinned(key.getStreamId(), key.getSequenceNumber(), payload.value(), payload.bytes(),
				payload.owner());
		}
		catch(RuntimeException ex) {
			//the payload was already marked transferred; return the bytes to the producer directly
			if(payload.bytes() > 0)
				payload.owner().release(payload.bytes());
			failInstall(index, slot, ex);
			throw ex;
		}
		_cache.unpin(entry, payload.owner());

		boolean cleared;
		OOCFuture<Void> installFuture;
		synchronized(this) {
			slot._key = key;
			cleared = slot._cleared;
			slot._state = cleared ? Slot.REMOVED : Slot.INSTALLED;
			installFuture = slot._installFuture;
			slot._installFuture = null;
		}
		if(cleared)
			_cache.dereference(key);
		installFuture.complete(null);
	}

	private void finishReferenceInstall(int index, Slot slot, BlockEntry pinned) {
		try {
			_cache.reference(pinned);
		}
		catch(RuntimeException ex) {
			failInstall(index, slot, ex);
			throw ex;
		}

		boolean cleared;
		OOCFuture<Void> installFuture;
		synchronized(this) {
			slot._key = pinned.getKey();
			cleared = slot._cleared;
			slot._state = cleared ? Slot.REMOVED : Slot.INSTALLED;
			installFuture = slot._installFuture;
			slot._installFuture = null;
		}
		if(cleared)
			_cache.dereference(pinned.getKey());
		installFuture.complete(null);
	}

	private void failInstall(int index, Slot slot, RuntimeException ex) {
		OOCFuture<Void> installFuture;
		synchronized(this) {
			if(index < _slots.length && _slots[index] == slot)
				_slots[index] = null;
			slot._state = Slot.REMOVED;
			installFuture = slot._installFuture;
			slot._installFuture = null;
		}
		if(installFuture != null)
			installFuture.completeExceptionally(ex);
	}

	private OOCFuture<StateLease<T>> pinTaken(Slot slot) {
		OOCFuture<BlockEntry> pinned = new OOCFuture<>();
		StorePinRetry.pinWithRetry(_cache, slot._key.getStreamId(), slot._key.getSequenceNumber(), _allowance,
			() -> _closed, pinned);
		//complete a fresh future exactly once; a mapped view would re-run the side effects per read
		OOCFuture<StateLease<T>> result = new OOCFuture<>();
		pinned.whenComplete((entry, error) -> {
			Throwable completionError = error;
			try {
				releaseSlot(slot);
			}
			catch(Throwable releaseError) {
				if(completionError == null)
					completionError = releaseError;
			}
			if(completionError != null) {
				if(entry != null) {
					try {
						_cache.unpin(entry, _allowance);
					}
					catch(Throwable ignored) {
						// Preserve the original pin/release failure.
					}
				}
				result.completeExceptionally(completionError);
				return;
			}
			result.complete(entry == null ? null : new TableLease(entry));
		});
		return result;
	}

	private void releaseSlot(Slot slot) {
		_cache.dereference(slot._key);
	}

	private void checkOpen() {
		if(_closed)
			throw new IllegalStateException("State table is closed.");
	}

	private void ensureCapacity(int index) {
		if(index < 0)
			throw new IndexOutOfBoundsException("Invalid slot index: " + index);
		if(index < _slots.length)
			return;
		int newLength = _slots.length;
		while(index >= newLength)
			newLength *= 2;
		Slot[] grown = new Slot[newLength];
		System.arraycopy(_slots, 0, grown, 0, _slots.length);
		_slots = grown;
	}

	/**
	 * Re-runs a chained slot operation after an in-flight install resolved, forwarding its outcome.
	 * Synchronous failures (e.g. a concurrently closed table) must complete the caller's future instead
	 * of vanishing inside the install future's callback.
	 */
	private static <T> void retry(java.util.function.Supplier<OOCFuture<T>> operation, OOCFuture<T> to) {
		OOCFuture<T> from;
		try {
			from = operation.get();
		}
		catch(RuntimeException ex) {
			to.completeExceptionally(ex);
			return;
		}
		from.whenComplete((value, error) -> {
			if(error != null)
				to.completeExceptionally(error);
			else
				to.complete(value);
		});
	}

	public interface StateLease<T> extends AutoCloseable {
		T value();

		long bytes();

		@Override
		void close();
	}

	private final class TableLease implements StateLease<T> {
		private final BlockEntry _entry;
		private boolean _open;

		private TableLease(BlockEntry entry) {
			_entry = entry;
			_open = true;
		}

		@SuppressWarnings("unchecked")
		@Override
		public synchronized T value() {
			if(!_open)
				throw new IllegalStateException("Lease is closed");
			return (T)_entry.getData();
		}

		@Override
		public long bytes() {
			return _entry.getSize();
		}

		@Override
		public synchronized void close() {
			if(!_open)
				return;
			_open = false;
			_cache.unpin(_entry, _allowance);
		}
	}

	private static final class Slot {
		private static final byte INSTALLING = 0;
		private static final byte INSTALLED = 1;
		private static final byte REMOVED = 2;

		private byte _state;
		private boolean _cleared;
		private BlockKey _key;
		private OOCFuture<Void> _installFuture;

		private Slot() {
			_state = INSTALLING;
			_installFuture = new OOCFuture<>();
		}
	}
}
