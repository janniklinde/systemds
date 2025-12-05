package org.apache.sysds.runtime.instructions.ooc;

import org.apache.sysds.runtime.DMLRuntimeException;

import java.util.concurrent.atomic.AtomicLong;

public class ResourceManager {
	private final Object _waitLock;
	private final long _softLimit;
	private final long _inMemoryLimit;
	private final long _hardLimit;
	private final AtomicLong _residentMemory;
	private final AtomicLong _pinnedMemory;
	private final AtomicLong _reservedMemory;
	private final AtomicLong _unpinnedMemory;
	private final Runnable _onMemoryPressure;

	public ResourceManager(long softLimit, long inMemoryLimit, long hardLimit, Runnable onMemoryPressure) {
		this._waitLock = new Object();
		this._softLimit = softLimit;
		this._inMemoryLimit = inMemoryLimit;
		this._hardLimit = hardLimit;
		this._onMemoryPressure = onMemoryPressure;
		this._residentMemory = new AtomicLong(0);
		this._pinnedMemory = new AtomicLong(0);
		this._reservedMemory = new AtomicLong(0);
		this._unpinnedMemory = new AtomicLong(0);
	}

	public long getSoftLimit() {
		return _softLimit;
	}

	public long getHardLimit() {
		return _hardLimit;
	}

	public long getResidentBytes() {
		return _residentMemory.get();
	}

	public long getReservedBytes() {
		return _reservedMemory.get();
	}

	public long getPinnedBytes() {
		return _pinnedMemory.get();
	}

	public long getUnpinnedBytes() {
		return _unpinnedMemory.get();
	}

	public boolean shouldFreeMemory() {
		return _residentMemory.get() > _inMemoryLimit;
	}

	public void reset() {
		_pinnedMemory.set(0);
		_reservedMemory.set(0);
		_unpinnedMemory.set(0);
		_residentMemory.set(0);
	}

	/**
	 * Tries to reserve the specified chunk of memory
	 * @param memory the number of bytes to be reserved
	 * @return 0 if succeeded; otherwise the number of bytes that need to be freed to successfully reserve
	 */
	public long tryReserve(long memory) {
		for (;;) {
			long cur = _residentMemory.get();
			long next = cur + memory;

			if (next > _hardLimit) {
				// how much we'd need to free to make this possible
				return next - _hardLimit;
			}

			if (_residentMemory.compareAndSet(cur, next)) {
				_reservedMemory.addAndGet(memory);
				probeMemoryPressure(next, _unpinnedMemory.get());
				return 0;
			}
			// CAS failed → retry
		}
	}

	/**
	 * Reserves and directly pins memory. Limits are ignored, allowing us to go above the hard limit
	 * @return the total number of unpinned bytes
	 */
	public long forceReserveAndUnpin(long memory) {
		_residentMemory.addAndGet(memory);
		return _unpinnedMemory.addAndGet(memory);
	}

	/**
	 * Pins already reserved memory
	 * @param memory the number of bytes
	 * @return the total number of pinned bytes
	 */
	public long pinReserved(long memory) {
		_reservedMemory.addAndGet(-memory);
		return _pinnedMemory.addAndGet(memory);
	}

	/**
	 * Unpins previously pinned memory
	 * @param memory the number of bytes to be unpinned
	 * @return the total number of unpinned bytes
	 */
	public long unpinPinned(long memory) {
		_pinnedMemory.addAndGet(-memory);
		long unpinned = _unpinnedMemory.addAndGet(memory);
		probeMemoryPressure(_residentMemory.get(), unpinned);
		return unpinned;
	}

	public void pinUnpinned(long memory) {
		_pinnedMemory.addAndGet(memory);
		_unpinnedMemory.addAndGet(-memory);
	}

	public void unpinReserved(long memory) {
		_unpinnedMemory.addAndGet(memory);
		_reservedMemory.addAndGet(-memory);
	}

	/**
	 * Frees unpinned memory
	 * @param memory the number of bytes to be freed
	 * @return the total number of resident bytes after freeing
	 */
	public long freeUnpinned(long memory) {
		_unpinnedMemory.addAndGet(-memory);
		long total = _residentMemory.addAndGet(-memory);
		synchronized (_waitLock) {
			_waitLock.notifyAll();
		}
		return total;
	}

	/**
	 * Frees reserved memory
	 * @param memory the number of bytes to be freed
	 * @return the total number of resident bytes after freeing
	 */
	public long freeReserved(long memory) {
		_reservedMemory.addAndGet(-memory);
		long total = _residentMemory.addAndGet(-memory);
		synchronized (_waitLock) {
			_waitLock.notifyAll();
		}
		return total;
	}

	/**
	 * Waits for sufficient free memory and reserves
	 */
	public void awaitAndReserve(long memory) {
		synchronized(_waitLock) {
			while (tryReserve(memory) != 0) {
				try {
					_waitLock.wait();
				}
				catch(InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	private void probeMemoryPressure(long residentMemory, long unpinnedMemory) {
		if (residentMemory > _inMemoryLimit &&  unpinnedMemory >= 1000)
			_onMemoryPressure.run();
	}
}
