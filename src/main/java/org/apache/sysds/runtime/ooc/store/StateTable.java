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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntToLongFunction;
import java.util.function.Supplier;

public final class StateTable<T extends SpillableObject> implements AutoCloseable {
	private static final int INITIAL_SLOTS = 64;

	private final OOCCache _cache;
	private final long _streamId;
	private final AtomicLong _nextGeneration = new AtomicLong();
	private final CopyOnWriteArrayList<IntToLongFunction> _evictionPolicies = new CopyOnWriteArrayList<>();
	private final AtomicBoolean _evictionPolicyInstalled = new AtomicBoolean(false);
	private volatile AtomicIntegerArray _generationSlots;
	private Slot[] _slots;
	private volatile boolean _closed;

	public StateTable(OOCCache cache, long streamId) {
		this(cache, streamId, INITIAL_SLOTS);
	}

	public StateTable(OOCCache cache, long streamId, int numSlots) {
		_cache = cache;
		_streamId = streamId;
		_generationSlots = new AtomicIntegerArray(numSlots);
		_slots = new Slot[numSlots];
	}

	public long getStreamId() {
		return _streamId;
	}

	public void addEvictionPolicy(IntToLongFunction slotPolicy) {
		_evictionPolicies.add(slotPolicy);
		if(_evictionPolicyInstalled.compareAndSet(false, true))
			_cache.addEvictionPolicy(_streamId, this::scoreTableEntry);
	}

	public void install(int index, ManagedPayload<T> payload) {
		installSlot(index, slot -> finishOwnedInstall(index, slot, payload));
	}

	public void installReference(int index, BlockEntry pinned) {
		installSlot(index, slot -> finishReferenceInstall(index, slot, pinned));
	}

	private void installSlot(int index, Consumer<Slot> installer) {
		Slot slot = new Slot();
		synchronized(this) {
			ensureCapacity(index);
			if(_slots[index] != null)
				throw new IllegalStateException("State table slot " + index + " is already occupied.");
			_slots[index] = slot;
		}
		installer.accept(slot);
	}

	public OOCFuture<StateLease<T>> installOrTake(int index, ManagedPayload<T> payload, MemoryAllowance leaseAllowance) {
		return installSlotOrTake(index, leaseAllowance, slot -> finishOwnedInstall(index, slot, payload));
	}

	public OOCFuture<StateLease<T>> installReferenceOrTake(int index, BlockEntry pinned, MemoryAllowance leaseAllowance) {
		return installSlotOrTake(index, leaseAllowance, slot -> finishReferenceInstall(index, slot, pinned));
	}

	private OOCFuture<StateLease<T>> installSlotOrTake(int index, MemoryAllowance leaseAllowance, Consumer<Slot> installer) {
		Slot installing = null;
		Slot taken = null;
		OOCFuture<Void> waitFor = null;
		synchronized(this) {
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
			installer.accept(installing);
			return OOCFuture.completed(null);
		}
		if(taken != null)
			return pinTaken(taken, leaseAllowance);
		OOCFuture<StateLease<T>> result = new OOCFuture<>();
		waitFor.whenComplete((ignored, error) ->
			retry(() -> installSlotOrTake(index, leaseAllowance, installer), result));
		return result;
	}

	public OOCFuture<StateLease<T>> take(int index, MemoryAllowance leaseAllowance) {
		Slot taken = null;
		OOCFuture<Void> waitFor = null;
		synchronized(this) {
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
			return pinTaken(taken, leaseAllowance);
		OOCFuture<StateLease<T>> result = new OOCFuture<>();
		waitFor.whenComplete((ignored, error) -> retry(() -> take(index, leaseAllowance), result));
		return result;
	}

	public StateLease<T> peek(int index, MemoryAllowance leaseAllowance) {
		BlockKey key;
		synchronized(this) {
			if(index < 0 || index >= _slots.length)
				return null;
			Slot slot = _slots[index];
			if(slot == null || slot._state != Slot.INSTALLED)
				return null;
			key = slot._key;
		}
		BlockEntry entry = _cache.pinIfLive(key.getStreamId(), key.getSequenceNumber(), leaseAllowance);
		return entry == null ? null : new TableLease(entry, leaseAllowance);
	}

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

	private void finishOwnedInstall(int index, Slot slot, ManagedPayload<T> payload) {
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
			if(payload.bytes() > 0)
				payload.owner().release(payload.bytes());
			failInstall(index, slot, ex);
			throw ex;
		}
		boolean cleared;
		OOCFuture<Void> installFuture;
		synchronized(this) {
			slot._key = key;
			slot._tableOwnedKey = true;
			registerSlotKey(key, index);
			cleared = slot._cleared;
			slot._state = cleared ? Slot.REMOVED : Slot.INSTALLED;
			installFuture = slot._installFuture;
			slot._installFuture = null;
		}
		_cache.unpin(entry, payload.owner());
		if(cleared)
			releaseSlot(slot);
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
			slot._tableOwnedKey = false;
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

	private OOCFuture<StateLease<T>> pinTaken(Slot slot, MemoryAllowance leaseAllowance) {
		OOCFuture<BlockEntry> pinned = new OOCFuture<>();
		StorePinRetry.pinWithRetry(_cache, slot._key.getStreamId(), slot._key.getSequenceNumber(), leaseAllowance,
			() -> _closed, pinned);
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
						_cache.unpin(entry, leaseAllowance);
					}
					catch(Throwable ignored) {
						// Preserve the original pin/release failure.
					}
				}
				result.completeExceptionally(completionError);
				return;
			}
			result.complete(entry == null ? null : new TableLease(entry, leaseAllowance));
		});
		return result;
	}

	private void releaseSlot(Slot slot) {
		if(slot._tableOwnedKey)
			clearSlotKey(slot._key);
		_cache.dereference(slot._key);
	}

	private void registerSlotKey(BlockKey key, int index) {
		int generation = (int)key.getSequenceNumber();
		ensureGenerationCapacity(generation);
		_generationSlots.set(generation, index + 1);
	}

	private void clearSlotKey(BlockKey key) {
		int generation = (int)key.getSequenceNumber();
		AtomicIntegerArray slots = _generationSlots;
		if(generation < slots.length())
			slots.set(generation, 0);
	}

	private long scoreTableEntry(long generation) {
		int index = (int)generation;
		AtomicIntegerArray slots = _generationSlots;
		if(index >= slots.length())
			return Long.MAX_VALUE;
		int encodedSlot = slots.get(index);
		if(encodedSlot == 0)
			return Long.MAX_VALUE;
		int slot = encodedSlot - 1;
		long score = Long.MAX_VALUE;
		for(IntToLongFunction policy : _evictionPolicies)
			score = Math.min(score, policy.applyAsLong(slot));
		return score;
	}

	private void ensureGenerationCapacity(int index) {
		AtomicIntegerArray slots = _generationSlots;
		if(index < slots.length())
			return;
		int newLength = slots.length();
		while(index >= newLength) {
			if(newLength > Integer.MAX_VALUE / 2)
				throw new IllegalStateException("State table generation map capacity overflow");
			newLength <<= 1;
		}
		AtomicIntegerArray grown = new AtomicIntegerArray(newLength);
		for(int i = 0; i < slots.length(); i++)
			grown.set(i, slots.get(i));
		_generationSlots = grown;
	}

	private void ensureCapacity(int index) {
		if(index < 0)
			throw new IndexOutOfBoundsException("Invalid slot index: " + index);
		if(_closed)
			throw new IllegalStateException("State table is closed.");
		if(index < _slots.length)
			return;
		int newLength = _slots.length;
		while(index >= newLength)
			newLength *= 2;
		Slot[] grown = new Slot[newLength];
		System.arraycopy(_slots, 0, grown, 0, _slots.length);
		_slots = grown;
	}

	private static <T> void retry(Supplier<OOCFuture<T>> operation, OOCFuture<T> to) {
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

	private final class TableLease implements StateLease<T> {
		private final BlockEntry _entry;
		private final MemoryAllowance _allowance;
		private boolean _open;

		private TableLease(BlockEntry entry, MemoryAllowance allowance) {
			_entry = entry;
			_allowance = allowance;
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
		private boolean _tableOwnedKey;
		private BlockKey _key;
		private OOCFuture<Void> _installFuture;

		private Slot() {
			_state = INSTALLING;
			_installFuture = new OOCFuture<>();
		}
	}
}
