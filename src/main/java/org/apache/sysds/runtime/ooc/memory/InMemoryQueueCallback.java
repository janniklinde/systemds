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
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryQueueCallback<T> implements OOCStream.QueueCallback<T> {
	/** Logical cache stream holding all force-parked payloads. */
	private static final long PARK_STREAM_ID = CachingStream._streamSeq.getNextID();
	private static final AtomicLong PARK_SEQ = new AtomicLong(0);
	private static volatile MemoryAllowance REVIVE_ALLOWANCE;

	private CallbackHandle<T> _handle;
	private boolean _closed;

	/**
	 * Allowance used to pull parked payloads back into memory. It holds nothing while the engine runs normally, and
	 * it is exempt from the strict-mode fair share so that a revive is admitted as soon as the broker has free bytes
	 * - which a purge run is what produces in the first place.
	 */
	static MemoryAllowance getReviveAllowance() {
		MemoryAllowance allowance = REVIVE_ALLOWANCE;
		if(allowance != null)
			return allowance;
		synchronized(InMemoryQueueCallback.class) {
			if(REVIVE_ALLOWANCE == null) {
				REVIVE_ALLOWANCE = new SyncMemoryAllowance(GlobalMemoryBroker.get()) {
					@Override
					public boolean isAdmissionExempt() {
						return true;
					}
				};
			}
			return REVIVE_ALLOWANCE;
		}
	}

	/**
	 * Force-parks the payload into the cache and hands its bytes back to the broker. Only callable while the caller
	 * guarantees that the payload has not been handed to a consumer yet (i.e. while holding the monitor of the queue
	 * the callback is still buffered in).
	 *
	 * @param cache the cache to park into
	 * @return the number of bytes released, or 0 if this callback is not parkable
	 */
	public long tryPark(OOCCache cache) {
		return _handle.tryPark(cache);
	}

	public InMemoryQueueCallback(T result, DMLRuntimeException failure, MemoryAllowance allow, long reservedBytes) {
		_handle = new CallbackHandle<>(result, failure, allow, reservedBytes);
		_closed = false;
	}

	public InMemoryQueueCallback(ManagedPayload<T> payload) {
		this(payload.value(), null, payload.owner(), payload.bytes());
		payload.transfer();
	}

	private InMemoryQueueCallback(CallbackHandle<T> handle) {
		_handle = handle;
		_closed = false;
	}

	@Override
	public T get() {
		return _handle.get();
	}

	@Override
	public synchronized InMemoryQueueCallback<T> keepOpen() {
		if(_closed)
			throw new IllegalStateException("Cannot keep open a closed callback");
		_handle._refCtr.incrementAndGet();
		return new InMemoryQueueCallback<>(_handle);
	}

	@Override
	public void fail(DMLRuntimeException failure) {
		_handle._failure = failure;
	}

	public long getManagedBytes() {
		synchronized(_handle) {
			return _handle._reservedBytes;
		}
	}

	public boolean tryTransferOwnership(MemoryAllowance allowance) {
		synchronized(_handle) {
			long bytes = _handle._reservedBytes;
			if(bytes <= 0 || _handle._allow == allowance)
				return true;
			if(_handle._cacheIdx >= 0)
				return false;
			if(!allowance.tryReserve(bytes))
				return false;
			_handle._allow.release(bytes);
			_handle._allow = allowance;
			return true;
		}
	}

	public void transferOwnershipBlocking(MemoryAllowance allowance) {
		synchronized(_handle) {
			long bytes = _handle._reservedBytes;
			if(bytes <= 0 || _handle._allow == allowance)
				return;
			if(_handle._cacheIdx >= 0)
				throw new IllegalStateException("Cannot transfer ownership of a cached allowance callback.");
			if(allowance instanceof CachedAllowance cached)
				cached.admitBlocking(bytes);
			else
				allowance.reserveBlocking(bytes);
			_handle._allow.release(bytes);
			_handle._allow = allowance;
		}
	}

	public long releaseManagedMemory() {
		synchronized(_handle) {
			long bytes = _handle._reservedBytes;
			if(bytes <= 0)
				return 0;
			_handle._reservedBytes = 0;
			_handle._allow.release(bytes);
			return bytes;
		}
	}

	@Override
	public synchronized void close() {
		if(_closed)
			return;
		_closed = true;
		if(_handle._refCtr.decrementAndGet() == 0)
			_handle.closeFinal();
		_handle = null;
	}

	@Override
	public boolean isEos() {
		return _handle.isEos();
	}

	public boolean isParked() {
		return _handle.isParked();
	}

	@Override
	public boolean isFailure() {
		return _handle._failure != null;
	}

	CallbackHandle<T> getHandle() {
		return _handle;
	}

	static final class CallbackHandle<T> {
		private volatile T _result;
		private final AtomicInteger _refCtr;
		private MemoryAllowance _allow;
		private long _reservedBytes;
		private volatile DMLRuntimeException _failure;
		private int _cacheIdx;
		/** Non-null once the payload has been force-parked into the cache; stays set after a revive. */
		private volatile BlockKey _parkKey;
		/** Non-null while a parked payload is revived, i.e. pinned in the cache and charged to {@link #_allow}. */
		private BlockEntry _parkEntry;
		/** Allowance that owned the payload before it was parked; carries the parked bytes as passive memory. */
		private MemoryAllowance _parkOwner;
		private long _parkBytes;

		private CallbackHandle(T result, DMLRuntimeException failure, MemoryAllowance allow, long reservedBytes) {
			_result = result;
			_failure = failure;
			_refCtr = new AtomicInteger(1);
			_allow = allow;
			_reservedBytes = reservedBytes;
			_cacheIdx = -1;
			_parkKey = null;
			_parkEntry = null;
			_parkOwner = null;
			_parkBytes = 0;
		}

		private T get() {
			if(_failure != null)
				throw _failure;
			T result = _result;
			if(result == null && _parkKey != null)
				return revive();
			return result;
		}

		private boolean isParked() {
			return _parkKey != null && _result == null;
		}

		private boolean isEos() {
			return _failure == null && _result == null && _parkKey == null;
		}

		private long tryPark(OOCCache cache) {
			synchronized(this) {
				long bytes = _reservedBytes;
				if(_failure != null || _cacheIdx >= 0 || _parkKey != null || bytes <= 0)
					return 0;
				if(!(_result instanceof SpillableObject) || _refCtr.get() != 1)
					return 0;
				BlockKey key = new BlockKey(PARK_STREAM_ID, PARK_SEQ.getAndIncrement());
				BlockEntry entry = cache.putPinned(key, _result, bytes, _allow);
				_parkKey = key;
				_parkOwner = _allow;
				_parkBytes = bytes;
				_result = null;
				_reservedBytes = 0;
				//keep the backlog on the producer's books as passive memory so that freeing these bytes does not
				//hand it fresh task admission; this is what keeps a purge from acting as a materialization sink
				_parkOwner.addPassiveMemory(bytes);
				//hands the bytes from the owning allowance over to the cache; the cache may spill them to disk
				cache.unpin(entry, _allow);
				return bytes;
			}
		}

		private synchronized T revive() {
			if(_result != null || _parkKey == null)
				return _result;
			MemoryAllowance revive = getReviveAllowance();
			OOCCache cache = OOCCacheManager.getGlobalCache();
			BlockEntry entry;
			try {
				entry = cache.pinAdmitted(_parkKey, revive).get();
			}
			catch(InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new DMLRuntimeException(e);
			}
			catch(ExecutionException e) {
				throw DMLRuntimeException.of(e.getCause());
			}
			if(entry == null)
				throw new DMLRuntimeException("Parked callback payload is no longer available: " + _parkKey);
			@SuppressWarnings("unchecked")
			T result = (T) entry.getData();
			_parkEntry = entry;
			_allow = revive;
			_result = result;
			releaseParkedBacklog();
			//drop the reference taken by putPinned so that the final unpin removes the entry for good
			cache.dereference(entry);
			return result;
		}

		/** Drops the passive backlog charge of a parked payload once it is consumed or discarded. */
		private void releaseParkedBacklog() {
			if(_parkOwner == null)
				return;
			_parkOwner.removePassiveMemory(_parkBytes);
			_parkOwner = null;
			_parkBytes = 0;
		}

		synchronized void attachCachedAllowance(CachedAllowance allowance, int index) {
			if(_allow != allowance)
				throw new IllegalStateException("Callback ownership must already belong to the cached allowance.");
			if(_cacheIdx >= 0 && _cacheIdx != index)
				throw new IllegalStateException("Callback is already attached to a different cached slot.");
			_cacheIdx = index;
		}

		synchronized void detachCachedAllowance() {
			_cacheIdx = -1;
		}

		boolean isExclusiveToRoot() {
			return _refCtr.get() == 1;
		}

		private synchronized T takeManagedResultForHandover() {
			T result = _result;
			_result = null;
			return result;
		}

		private synchronized void closeFinal() {
			_result = null;
			if(_parkEntry != null) {
				//releases the revived bytes and removes the now consumed cache entry
				OOCCacheManager.getGlobalCache().unpin(_parkEntry, _allow);
				_parkEntry = null;
				_parkKey = null;
			}
			else if(_parkKey != null) {
				OOCCacheManager.getGlobalCache().dereference(_parkKey);
				_parkKey = null;
				releaseParkedBacklog();
			}
			else
				_allow.release(_reservedBytes);
			_reservedBytes = 0;
			_cacheIdx = -1;
		}
	}

	public T takeManagedResultForHandover() {
		return _handle.takeManagedResultForHandover();
	}

	public synchronized ManagedPayload<T> extractManagedPayload() {
		if(_closed)
			throw new IllegalStateException("Cannot extract a managed payload from a closed callback.");
		CallbackHandle<T> handle = _handle;
		synchronized(handle) {
			if(handle._failure != null)
				throw handle._failure;
			if(!handle.isExclusiveToRoot())
				throw new IllegalStateException("Cannot extract a managed payload while callback aliases exist.");
			if(handle._cacheIdx >= 0)
				throw new IllegalStateException("Cannot extract a managed payload from a cached-slot callback.");
			T result = handle._result;
			if(result == null)
				throw new IllegalStateException("Cannot extract a managed payload from an empty callback.");
			long bytes = handle._reservedBytes;
			MemoryAllowance owner = handle._allow;
			handle._result = null;
			handle._reservedBytes = 0;
			return new ManagedPayload<>(result, bytes, owner);
		}
	}
}
