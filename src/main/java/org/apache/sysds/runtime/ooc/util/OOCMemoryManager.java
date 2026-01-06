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

package org.apache.sysds.runtime.ooc.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class OOCMemoryManager {
	public enum Priority {
		LOW,
		MEDIUM,
		HIGH,
		FORCE
	}

	private final long _totalBytes;
	private final AtomicLong _availableBytes;
	private final Object _waitLock;
	private final Deque<Waiter> _waitersHigh;
	private final Deque<Waiter> _waitersMedium;
	private final Deque<Waiter> _waitersLow;

	public OOCMemoryManager(long totalBytes) {
		if(totalBytes <= 0)
			throw new IllegalArgumentException("Total bytes must be positive");
		_totalBytes = totalBytes;
		_availableBytes = new AtomicLong(totalBytes);
		_waitLock = new Object();
		_waitersHigh = new ArrayDeque<>();
		_waitersMedium = new ArrayDeque<>();
		_waitersLow = new ArrayDeque<>();
	}

	public long getTotalBytes() {
		return _totalBytes;
	}

	public long getAvailableBytes() {
		return _availableBytes.get();
	}

	public Allowance createAllowance(long refillBytes) {
		return new Allowance(this, refillBytes);
	}

	boolean tryReserve(long bytes) {
		if(bytes <= 0)
			return true;
		while(true) {
			long available = _availableBytes.get();
			if(available < bytes)
				return false;
			if(_availableBytes.compareAndSet(available, available - bytes))
				return true;
		}
	}

	private void forceReserve(long bytes) {
		if (bytes <= 0)
			return;
		_availableBytes.addAndGet(-bytes);
	}

	private void reserveBlocking(long bytes, Priority priority) throws InterruptedException {
		try {
			reserveAsync(bytes, priority).get();
		}
		catch (ExecutionException e) {
			throw new IllegalStateException("Unexpected failure while acquiring memory", e.getCause());
		}
	}

	private CompletableFuture<Void> reserveAsync(long bytes, Priority priority) {
		if (priority == Priority.FORCE) {
			if (bytes > 0)
				_availableBytes.addAndGet(-bytes);
			return CompletableFuture.completedFuture(null);
		}
		if(tryReserve(bytes))
			return CompletableFuture.completedFuture(null);
		Waiter waiter = new Waiter(bytes, new CompletableFuture<>());
		synchronized(_waitLock) {
			if(tryReserve(bytes))
				return CompletableFuture.completedFuture(null);
			getWaiterQueue(priority).addLast(waiter);
		}
		return waiter.future;
	}

	private void release(long bytes) {
		if(bytes <= 0)
			return;
		_availableBytes.addAndGet(bytes);
		drainWaiters();
	}

	private void drainWaiters() {
		synchronized(_waitLock) {
			boolean anyCompleted = false;
			while(true) {
				Waiter waiter = _waitersHigh.peekFirst();
				Deque<Waiter> queue = _waitersHigh;
				if (waiter == null) {
					waiter = _waitersMedium.peekFirst();
					queue = _waitersMedium;
				}
				if (waiter == null) {
					waiter = _waitersLow.peekFirst();
					queue = _waitersLow;
				}
				if (waiter == null)
					break;
				if(!tryReserve(waiter.bytes))
					return;
				queue.pollFirst();
				waiter.done = true;
				if(waiter.future != null)
					waiter.future.complete(null);
				anyCompleted = true;
			}
			if(anyCompleted)
				_waitLock.notifyAll();
		}
	}

	private Deque<Waiter> getWaiterQueue(Priority priority) {
		return switch(priority) {
			case HIGH -> _waitersHigh;
			case MEDIUM -> _waitersMedium;
			case LOW -> _waitersLow;
			case FORCE -> _waitersHigh;
		};
	}

	private static final class Waiter {
		private final long bytes;
		private final CompletableFuture<Void> future;
		private boolean done;

		private Waiter(long bytes, CompletableFuture<Void> future) {
			this.bytes = bytes;
			this.future = future;
			this.done = false;
		}
	}

	public static final class Allowance {
		private final OOCMemoryManager _manager;
		private final long _refillBytes;
		private long _localAvailable;
		private boolean _closed;

		private Allowance(OOCMemoryManager manager, long refillBytes) {
			if(refillBytes <= 0)
				throw new IllegalArgumentException("Refill bytes must be positive");
			_manager = Objects.requireNonNull(manager);
			_refillBytes = refillBytes;
			_localAvailable = 0;
			_closed = false;
		}

		public synchronized void acquire(long bytes) throws InterruptedException {
			acquire(bytes, Priority.MEDIUM);
		}

		public synchronized void acquire(long bytes, Priority priority) throws InterruptedException {
			ensureOpen();
			if(bytes <= 0)
				return;
			if(bytes > _localAvailable) {
				long needed = bytes - _localAvailable;
				long request = Math.max(_refillBytes, needed);
				_manager.reserveBlocking(request, priority);
				_localAvailable += request;
			}
			_localAvailable -= bytes;
		}

		public synchronized boolean tryAcquire(long bytes) {
			return tryAcquire(bytes, Priority.MEDIUM);
		}

		public synchronized boolean tryAcquire(long bytes, Priority priority) {
			if(_closed)
				return false;
			if(bytes <= 0)
				return true;
			if(priority == Priority.FORCE) {
				long needed = bytes - _localAvailable;
				long request = Math.max(_refillBytes, needed);
				_localAvailable = 0;
				_manager.forceReserve(request);
				_localAvailable += request - bytes;
				return true;
			}
			if(bytes > _localAvailable) {
				long needed = bytes - _localAvailable;
				long request = Math.max(_refillBytes, needed);
				if(!_manager.tryReserve(request))
					return false;
				_localAvailable += request;
			}
			_localAvailable -= bytes;
			return true;
		}

		public synchronized CompletableFuture<Void> acquireAsync(long bytes) {
			return acquireAsync(bytes, Priority.MEDIUM);
		}

		public synchronized CompletableFuture<Void> acquireAsync(long bytes, Priority priority) {
			if(_closed) {
				CompletableFuture<Void> failed = new CompletableFuture<>();
				failed.completeExceptionally(new IllegalStateException("Allowance is closed"));
				return failed;
			}
			if(bytes <= 0)
				return CompletableFuture.completedFuture(null);
			if(bytes <= _localAvailable) {
				_localAvailable -= bytes;
				return CompletableFuture.completedFuture(null);
			}
			long needed = bytes - _localAvailable;
			long request = Math.max(_refillBytes, needed);
			_localAvailable = 0;
			return _manager.reserveAsync(request, priority).thenRun(() -> {
				synchronized(this) {
					_localAvailable += request - bytes;
				}
			});
		}

		public synchronized void release(long bytes) {
			if(bytes <= 0)
				return;
			if (_closed) {
				_manager.release(bytes);
				return;
			}
			_localAvailable += bytes;
			long excess = _localAvailable - _refillBytes;
			if(excess > 0) {
				_localAvailable -= excess;
				_manager.release(excess);
			}
		}

		public synchronized long getLocalAvailableBytes() {
			return _localAvailable;
		}

		public synchronized void close() {
			if (_closed)
				return;
			_closed = true;
			if (_localAvailable > 0) {
				long toRelease = _localAvailable;
				_localAvailable = 0;
				_manager.release(toRelease);
			}
		}

		private void ensureOpen() {
			if (_closed)
				throw new IllegalStateException("Allowance is closed");
		}
	}
}
