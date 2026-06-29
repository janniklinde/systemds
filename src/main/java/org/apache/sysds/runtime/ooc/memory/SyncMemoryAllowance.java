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
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;

public class SyncMemoryAllowance implements MemoryAllowance {
	private static final long RELEASE_TRIM_BUFFER_BYTES = 20_000_000L;

	protected final MemoryBroker _broker;
	protected final long _consumptionLimit;
	protected volatile long _usedBytes;
	protected volatile long _grantedBytes;
	protected volatile long _targetBytes;
	protected volatile boolean _shutdown;
	protected volatile boolean _destroyed;
	private final ArrayDeque<ReservationWaiter> _reservationWaiters;
	private boolean _drainingReservationWaiters;
	private boolean _reservationDrainRequested;

	public SyncMemoryAllowance(MemoryBroker broker) {
		this(broker, Long.MAX_VALUE);
	}

	public SyncMemoryAllowance(MemoryBroker broker, long consumptionLimit) {
		_broker = broker;
		_consumptionLimit = consumptionLimit;
		_usedBytes = 0;
		_grantedBytes = 0;
		_targetBytes = 0;
		_shutdown = false;
		_destroyed = false;
		_reservationWaiters = new ArrayDeque<>();
		_drainingReservationWaiters = false;
		_reservationDrainRequested = false;
		broker.attachAllowance(this);
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[ALLOW-INIT] allowance=" + dbgId() + " limit=" + _consumptionLimit);
	}

	@Override
	public boolean tryReserve(long bytes) {
		long minRequest;
		long maxRequest;
		long usedBefore;
		long grantedBefore;
		long targetBefore;
		synchronized(this) {
			if(_shutdown || _destroyed)
				return false;
			if(_usedBytes + bytes <= _grantedBytes) {
				usedBefore = _usedBytes;
				grantedBefore = _grantedBytes;
				targetBefore = _targetBytes;
				_usedBytes += bytes;
				if(OOCDebug.TRACE_HOT_PATH)
					System.out.println("[ALLOW-RESERVE-FAST] allowance=" + dbgId() + " bytes=" + bytes
						+ " used=" + usedBefore + "->" + _usedBytes + " granted=" + grantedBefore
						+ " target=" + targetBefore);
				return true;
			}
			if(_usedBytes + bytes > _targetBytes)
				return false;
			minRequest = _usedBytes + bytes - _grantedBytes;
			maxRequest = Math.max(minRequest, Math.max(_grantedBytes, bytes) * 2);
		}

		if(bytes > _consumptionLimit)
			throw new IllegalArgumentException("Cannot reserve more memory than the consumption limit");

		long granted = _broker.requestMemory(this, minRequest, maxRequest);
		long refund = 0;
		boolean success = false;
		boolean drainWaiters = false;
		synchronized(this) {
			if(_shutdown || _destroyed)
				refund = granted;
			else {
				usedBefore = _usedBytes;
				grantedBefore = _grantedBytes;
				targetBefore = _targetBytes;
				_grantedBytes += granted;
				if(_usedBytes + bytes <= _targetBytes && _usedBytes + bytes <= _grantedBytes) {
					_usedBytes += bytes;
					success = true;
				}
				drainWaiters = success && !_reservationWaiters.isEmpty();
				if(OOCDebug.TRACE_HOT_PATH)
					System.out.println("[ALLOW-RESERVE-SLOW] allowance=" + dbgId() + " bytes=" + bytes
						+ " brokerGranted=" + granted + " success=" + success
						+ " used=" + usedBefore + "->" + _usedBytes
						+ " granted=" + grantedBefore + "->" + _grantedBytes
						+ " target=" + targetBefore);
				notifyAll();
			}
		}
		if(refund > 0)
			_broker.freeMemory(this, refund);
		if(drainWaiters)
			requestReservationDrain();
		return success;
	}

	@Override
	public void reserveBlocking(long bytes) {
		if(_shutdown || _destroyed)
			throw new IllegalStateException("Cannot reserve memory on closed allowance.");
		while(true) {
			if(tryReserve(bytes)) {
				synchronized(this) {
					notifyAll();
				}
				return;
			}
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

	@Override
	public OOCFuture<Void> reserveAsync(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot reserve negative bytes: " + bytes);
		if(bytes == 0)
			return OOCFuture.completed(null);
		if(bytes > _consumptionLimit)
			return OOCFuture.failed(new IllegalArgumentException("Cannot reserve more memory than the consumption limit"));
		if(tryReserve(bytes))
			return OOCFuture.completed(null);
		OOCFuture<Void> future = new OOCFuture<>();
		synchronized(this) {
			if(_shutdown || _destroyed) {
				future.completeExceptionally(new IllegalStateException("Cannot reserve memory on closed allowance."));
				return future;
			}
			_reservationWaiters.addLast(new ReservationWaiter(bytes, future));
		}
		requestReservationDrain();
		return future;
	}

	@Override
	public CompletableFuture<Void> reserve(long bytes) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		reserveAsync(bytes).whenComplete((ignored, error) -> {
			if(error != null)
				future.completeExceptionally(error);
			else
				future.complete(null);
		});
		return future;
	}

	@Override
	public void release(long bytes) {
		long freedMemory = 0;
		long destroyFreedMemory = 0;
		boolean destroy = false;
		boolean drainWaiters = false;
		long usedBefore;
		long grantedBefore;
		long targetBefore;
		synchronized(this) {
			if(bytes < 0)
				throw new IllegalArgumentException("Cannot release negative bytes: " + bytes);
			usedBefore = _usedBytes;
			grantedBefore = _grantedBytes;
			targetBefore = _targetBytes;
			if(_usedBytes < bytes) {
				if(OOCDebug.TRACE_HOT_PATH)
					System.out.println("[ALLOW-UNDERFLOW] allowance=" + getClass().getSimpleName() + "@"
						+ System.identityHashCode(this) + " release=" + bytes + " used=" + _usedBytes
						+ " granted=" + _grantedBytes + " target=" + _targetBytes + " shutdown=" + _shutdown
						+ " destroyed=" + _destroyed);
				throw new IllegalArgumentException("Memory allowance underflow in " + getClass().getSimpleName()
					+ ": release=" + bytes + ", used=" + _usedBytes + ", granted=" + _grantedBytes
					+ ", target=" + _targetBytes + ", shutdown=" + _shutdown + ", destroyed=" + _destroyed);
			}
			_usedBytes -= bytes;
			if(_shutdown) {
				long oldGrantedBytes = _grantedBytes;
				_grantedBytes = _usedBytes;
				if(_grantedBytes < 0) {
					throw new IllegalArgumentException("Granted memory underflow in " + getClass().getSimpleName()
						+ ": granted=" + _grantedBytes + ", used=" + _usedBytes + ", released=" + bytes);
				}
				if(_usedBytes == 0) {
					_destroyed = true;
					destroy = true;
					destroyFreedMemory = oldGrantedBytes;
				}
				else {
					freedMemory = oldGrantedBytes - _grantedBytes;
				}
			}
			else if(_grantedBytes > _targetBytes) {
				long oldGrantedBytes = _grantedBytes;
				_grantedBytes = Math.max(_usedBytes, _targetBytes);
				freedMemory = oldGrantedBytes - _grantedBytes;
			}
			else if(_usedBytes * 3 < _grantedBytes * 2) {
				long oldGrantedBytes = _grantedBytes;
				_grantedBytes = Math.max(_usedBytes,
					Math.min(_grantedBytes, saturatingAdd(_usedBytes, RELEASE_TRIM_BUFFER_BYTES)));
				freedMemory = oldGrantedBytes - _grantedBytes;
			}
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[ALLOW-RELEASE] allowance=" + dbgId() + " bytes=" + bytes
					+ " used=" + usedBefore + "->" + _usedBytes
					+ " granted=" + grantedBefore + "->" + _grantedBytes
					+ " target=" + targetBefore + " shutdown=" + _shutdown + " destroyed=" + _destroyed
					+ " freedMemory=" + freedMemory + " destroy=" + destroy);
			drainWaiters = !_reservationWaiters.isEmpty() && !_shutdown && !_destroyed;
			notifyAll();
		}
		if(destroy)
			_broker.destroyAllowance(this, destroyFreedMemory);
		else if(freedMemory > 0)
			_broker.freeMemory(this, freedMemory);
		if(drainWaiters)
			requestReservationDrain();
	}

	@Override
	public long getUsedMemory() {
		return _usedBytes;
	}

	@Override
	public long getGrantedMemory() {
		return _grantedBytes;
	}

	@Override
	public long getTargetMemory() {
		return _targetBytes;
	}

	@Override
	public void setTargetMemory(long targetMemory) {
		long freedMemory = 0;
		long oldTarget;
		long targetAfter;
		long usedBefore;
		long grantedBefore;
		boolean drainWaiters = false;
		synchronized(this) {
			if(_shutdown || _destroyed)
				return;
			oldTarget = _targetBytes;
			usedBefore = _usedBytes;
			grantedBefore = _grantedBytes;
			_targetBytes = Math.min(targetMemory, _consumptionLimit);
			targetAfter = _targetBytes;
			if(_grantedBytes > _targetBytes) {
				long oldGrantedBytes = _grantedBytes;
				_grantedBytes = Math.max(_usedBytes, _targetBytes);
				freedMemory = oldGrantedBytes - _grantedBytes;
			}
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[ALLOW-TARGET] allowance=" + dbgId() + " target=" + oldTarget + "->" + targetAfter
					+ " used=" + usedBefore + " granted=" + grantedBefore + "->" + _grantedBytes
					+ " freedMemory=" + freedMemory);
			drainWaiters = !_reservationWaiters.isEmpty();
			notifyAll();
		}
		if(freedMemory > 0)
			_broker.freeMemory(this, freedMemory);
		if(drainWaiters)
			requestReservationDrain();
	}

	@Override
	public void shutdown() {
		long freedMemory = 0;
		long destroyFreedMemory = 0;
		boolean destroy = false;
		ArrayDeque<ReservationWaiter> waiters;
		synchronized(this) {
			if(_shutdown || _destroyed)
				return;
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[ALLOW-SHUTDOWN-BEGIN] allowance=" + dbgId() + " used=" + _usedBytes
					+ " granted=" + _grantedBytes + " target=" + _targetBytes);
			_shutdown = true;
			long oldGrantedBytes = _grantedBytes;
			_grantedBytes = _usedBytes;
			_targetBytes = 0;
			if(_usedBytes == 0) {
				_destroyed = true;
				destroy = true;
				destroyFreedMemory = oldGrantedBytes;
			}
			else {
				freedMemory = oldGrantedBytes - _grantedBytes;
			}
			waiters = new ArrayDeque<>(_reservationWaiters);
			_reservationWaiters.clear();
			notifyAll();
		}
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[ALLOW-SHUTDOWN-END] allowance=" + dbgId() + " used=" + _usedBytes
				+ " granted=" + _grantedBytes + " target=" + _targetBytes + " destroy=" + destroy
				+ " freedMemory=" + freedMemory + " destroyFreed=" + destroyFreedMemory);
		_broker.shutdownAllowance(this);
		if(destroy)
			_broker.destroyAllowance(this, destroyFreedMemory);
		else if(freedMemory > 0)
			_broker.freeMemory(this, freedMemory);
		failReservationWaiters(waiters, new IllegalStateException("Cannot reserve memory on closed allowance."));
	}

	@Override
	public boolean isShutdown() {
		return _shutdown || _destroyed;
	}

	void onBrokerMemoryAvailable() {
		boolean drainWaiters;
		synchronized(this) {
			if(_shutdown || _destroyed)
				return;
			drainWaiters = !_reservationWaiters.isEmpty();
			notifyAll();
		}
		if(drainWaiters)
			requestReservationDrain();
	}

	private String dbgId() {
		return getClass().getSimpleName() + "@" + System.identityHashCode(this);
	}

	private void requestReservationDrain() {
		synchronized(this) {
			_reservationDrainRequested = true;
			if(_drainingReservationWaiters)
				return;
			_drainingReservationWaiters = true;
		}
		try {
			while(true) {
				synchronized(this) {
					_reservationDrainRequested = false;
				}
				drainReservationWaitersOnce();
				synchronized(this) {
					if(!_reservationDrainRequested) {
						_drainingReservationWaiters = false;
						return;
					}
				}
			}
		}
		catch(RuntimeException | Error t) {
			synchronized(this) {
				_drainingReservationWaiters = false;
			}
			throw t;
		}
	}

	private void drainReservationWaitersOnce() {
		while(true) {
			ReservationWaiter waiter;
			synchronized(this) {
				if(_shutdown || _destroyed)
					return;
				waiter = _reservationWaiters.peekFirst();
				if(waiter == null)
					return;
			}
			boolean admitted;
			try {
				admitted = tryReserve(waiter.bytes);
			}
			catch(Throwable t) {
				removeReservationWaiter(waiter);
				waiter.future.completeExceptionally(t);
				continue;
			}
			if(!admitted)
				return;
			if(removeReservationWaiter(waiter))
				waiter.future.complete(null);
			else
				release(waiter.bytes);
		}
	}

	private synchronized boolean removeReservationWaiter(ReservationWaiter waiter) {
		if(_reservationWaiters.peekFirst() == waiter) {
			_reservationWaiters.removeFirst();
			return true;
		}
		return _reservationWaiters.remove(waiter);
	}

	private static void failReservationWaiters(ArrayDeque<ReservationWaiter> waiters, Throwable error) {
		while(!waiters.isEmpty())
			waiters.removeFirst().future.completeExceptionally(error);
	}

	private static long saturatingAdd(long left, long right) {
		if(Long.MAX_VALUE - left < right)
			return Long.MAX_VALUE;
		return left + right;
	}

	private static final class ReservationWaiter {
		private final long bytes;
		private final OOCFuture<Void> future;

		private ReservationWaiter(long bytes, OOCFuture<Void> future) {
			this.bytes = bytes;
			this.future = future;
		}
	}
}
