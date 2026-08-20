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
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.utils.stats.InfrastructureAnalyzer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;

public class SyncMemoryAllowance implements MemoryAllowance {
	private static final long RELEASE_TRIM_BUFFER_BYTES = 20_000_000L;

	protected final MemoryBroker _broker;
	protected final long _consumptionLimit;
	protected final long _minimumOperatingBytes;
	protected volatile long _usedBytes;
	protected volatile long _grantedBytes;
	protected volatile long _targetBytes;
	protected volatile boolean _shutdown;
	protected volatile boolean _destroyed;
	private final LongAdder _passiveBytes;
	private final Queue<ReservationWaiter> _reservationWaiters;
	private final Queue<ReservationWaiter> _taskWaiters;
	private boolean _drainingReservationWaiters;
	private boolean _reservationDrainRequested;

	public SyncMemoryAllowance(MemoryBroker broker) {
		this(broker, Long.MAX_VALUE);
	}

	public SyncMemoryAllowance(MemoryBroker broker, long consumptionLimit) {
		this(broker, consumptionLimit, 0);
	}

	public SyncMemoryAllowance(MemoryBroker broker, long consumptionLimit, long minimumOperatingBytes) {
		if(consumptionLimit < 0)
			throw new IllegalArgumentException("Consumption limit must not be negative: " + consumptionLimit);
		if(minimumOperatingBytes < 0)
			throw new IllegalArgumentException(
				"Minimum operating memory must not be negative: " + minimumOperatingBytes);
		_broker = broker;
		_consumptionLimit = consumptionLimit;
		_minimumOperatingBytes = Math.min(minimumOperatingBytes, consumptionLimit);
		_usedBytes = 0;
		_grantedBytes = 0;
		_targetBytes = 0;
		_shutdown = false;
		_destroyed = false;
		_passiveBytes = new LongAdder();
		_reservationWaiters = new ConcurrentLinkedQueue<>();
		_taskWaiters = new ConcurrentLinkedQueue<>();
		_drainingReservationWaiters = false;
		_reservationDrainRequested = false;
		broker.attachAllowance(this);
	}

	@Override
	public boolean tryReserve(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot reserve negative bytes: " + bytes);
		if(bytes > _consumptionLimit)
			throw new IllegalArgumentException(
				"Cannot reserve " + bytes + " bytes with a consumption limit of " + _consumptionLimit);
		long minRequest;
		long maxRequest;
		synchronized(this) {
			if(_shutdown || _destroyed)
				return false;
			if(_usedBytes + bytes <= _grantedBytes && withinAdmissionPolicy(bytes)) {
				_usedBytes += bytes;
				return true;
			}
			if(!withinAdmissionPolicy(bytes))
				return false;
			minRequest = _usedBytes + bytes - _grantedBytes;
			maxRequest = Math.max(minRequest, Math.max(_grantedBytes, bytes) * 2);
		}

		long granted = _broker.requestMemory(this, minRequest, maxRequest);
		long refund = 0;
		boolean success = false;
		boolean drainWaiters = false;
		synchronized(this) {
			if(_shutdown || _destroyed)
				refund = granted;
			else {
				_grantedBytes += granted;
				if(withinAdmissionPolicy(bytes) && _usedBytes + bytes <= _grantedBytes) {
					_usedBytes += bytes;
					success = true;
				}
				drainWaiters = success && hasReservationWaiters();
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
	public boolean tryReserveTask(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot reserve negative bytes: " + bytes);
		return bytes == 0 || canAdmitTask(bytes) && tryReserve(bytes);
	}

	@Override
	public void reserveBlocking(long bytes) {
		try {
			reserveAsync(bytes).get();
		}
		catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new DMLRuntimeException(e);
		}
		catch(ExecutionException e) {
			throw DMLRuntimeException.of(e.getCause());
		}
	}

	@Override
	public OOCFuture<Void> reserveAsync(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot reserve negative bytes: " + bytes);
		if(bytes == 0)
			return OOCFuture.completed(null);
		if(bytes > _consumptionLimit)
			return OOCFuture.failed(new IllegalArgumentException(
				"Cannot reserve " + bytes + " bytes with a consumption limit of " + _consumptionLimit));
		if(_shutdown || _destroyed)
			return OOCFuture.failed(new IllegalStateException("Cannot reserve memory on closed allowance."));
		if(_reservationWaiters.isEmpty() && tryReserve(bytes))
			return OOCFuture.completed(null);
		OOCFuture<Void> future = new OOCFuture<>();
		ReservationWaiter waiter = new ReservationWaiter(bytes, future);
		_reservationWaiters.add(waiter);
		if((_shutdown || _destroyed) && _reservationWaiters.remove(waiter)) {
			future.completeExceptionally(new IllegalStateException("Cannot reserve memory on closed allowance."));
			return future;
		}
		requestReservationDrain();
		if(!future.isDone())
			_broker.reservationBlocked(this, bytes);
		return future;
	}

	@Override
	public OOCFuture<Void> reserveTaskAsync(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot reserve negative bytes: " + bytes);
		if(bytes == 0)
			return OOCFuture.completed(null);
		if(bytes > _consumptionLimit)
			return OOCFuture.failed(new IllegalArgumentException(
				"Cannot reserve " + bytes + " bytes with a consumption limit of " + _consumptionLimit));
		if(_shutdown || _destroyed)
			return OOCFuture.failed(new IllegalStateException("Cannot reserve memory on closed allowance."));
		if(_reservationWaiters.isEmpty() && _taskWaiters.isEmpty() && tryReserveTask(bytes))
			return OOCFuture.completed(null);
		OOCFuture<Void> future = new OOCFuture<>();
		ReservationWaiter waiter = new ReservationWaiter(bytes, future);
		_taskWaiters.add(waiter);
		if((_shutdown || _destroyed) && _taskWaiters.remove(waiter)) {
			future.completeExceptionally(new IllegalStateException("Cannot reserve memory on closed allowance."));
			return future;
		}
		requestReservationDrain();
		if(!future.isDone() && canAdmitTask(bytes))
			_broker.reservationBlocked(this, bytes);
		return future;
	}

	@Override
	public void release(long bytes) {
		long freedMemory = 0;
		long destroyFreedMemory = 0;
		boolean destroy = false;
		boolean drainWaiters;
		synchronized(this) {
			if(bytes < 0)
				throw new IllegalArgumentException("Cannot release negative bytes: " + bytes);
			if(_usedBytes < bytes) {
				throw new IllegalArgumentException("Memory allowance underflow in " + getClass().getSimpleName()
					+ ": release=" + bytes + ", used=" + _usedBytes + ", granted=" + _grantedBytes + ", target="
					+ _targetBytes + ", shutdown=" + _shutdown + ", destroyed=" + _destroyed);
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
				_grantedBytes = Math.max(_usedBytes, Math.min(_grantedBytes, _usedBytes + RELEASE_TRIM_BUFFER_BYTES));
				freedMemory = oldGrantedBytes - _grantedBytes;
			}
			drainWaiters = hasReservationWaiters() && !_shutdown && !_destroyed;
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
	public void addPassiveMemory(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot add negative passive memory: " + bytes);
		_passiveBytes.add(bytes);
	}

	@Override
	public void removePassiveMemory(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot remove negative passive memory: " + bytes);
		_passiveBytes.add(-bytes);
	}

	@Override
	public long getPassiveMemory() {
		return Math.max(0, _passiveBytes.sum());
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
		if(targetMemory < 0)
			throw new IllegalArgumentException("Target memory must not be negative: " + targetMemory);
		long freedMemory = 0;
		boolean drainWaiters = false;
		synchronized(this) {
			if(_shutdown || _destroyed)
				return;
			_targetBytes = Math.min(Math.max(targetMemory, _minimumOperatingBytes), _consumptionLimit);
			if(_grantedBytes > _targetBytes) {
				long oldGrantedBytes = _grantedBytes;
				_grantedBytes = Math.max(_usedBytes, _targetBytes);
				freedMemory = oldGrantedBytes - _grantedBytes;
			}
			drainWaiters = hasReservationWaiters();
			notifyAll();
		}
		if(freedMemory > 0)
			_broker.freeMemory(this, freedMemory);
		if(drainWaiters)
			requestReservationDrain();
	}

	private boolean withinAdmissionPolicy(long bytes) {
		if(_broker.isStrictMode())
			return _usedBytes < _broker.getFairShare();
		return _usedBytes + bytes <= _targetBytes;
	}

	@Override
	public synchronized long reclaimUnused() {
		if(_shutdown || _destroyed || _grantedBytes <= _usedBytes)
			return 0;
		long reclaimed = _grantedBytes - _usedBytes;
		_grantedBytes = _usedBytes;
		notifyAll();
		return reclaimed;
	}

	@Override
	public void shutdown() {
		long freedMemory = 0;
		long destroyFreedMemory = 0;
		boolean destroy = false;
		synchronized(this) {
			if(_shutdown || _destroyed)
				return;
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
			notifyAll();
		}
		_broker.shutdownAllowance(this);
		if(destroy)
			_broker.destroyAllowance(this, destroyFreedMemory);
		else if(freedMemory > 0)
			_broker.freeMemory(this, freedMemory);
		IllegalStateException ex = new IllegalStateException("Cannot reserve memory on closed allowance.");
		ReservationWaiter waiter;
		while((waiter = _reservationWaiters.poll()) != null)
			waiter.future.completeExceptionally(ex);
		while((waiter = _taskWaiters.poll()) != null)
			waiter.future.completeExceptionally(ex);
	}

	@Override
	public String debugState() {
		synchronized(this) {
			return "id=" + System.identityHashCode(this) + " used=" + _usedBytes + " passive=" + getPassiveMemory() + " granted=" + _grantedBytes + " target="
				+ _targetBytes + " limit=" + _consumptionLimit + " waiters=" + _reservationWaiters.size()
				+ " taskWaiters=" + _taskWaiters.size() + (_shutdown ? " shutdown" : "");
		}
	}

	@Override
	public boolean isShutdown() {
		return _shutdown || _destroyed;
	}

	void onBrokerMemoryAvailable() {
		boolean drainWaiters;
		synchronized(this) {
			drainWaiters = hasReservationWaiters() && !_shutdown && !_destroyed;
			notifyAll();
		}
		if(drainWaiters)
			requestReservationDrain();
	}

	boolean hasReservationWaiters() {
		return !_reservationWaiters.isEmpty() || !_taskWaiters.isEmpty();
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
				drainTaskWaitersOnce();
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
			if(_shutdown || _destroyed)
				return;
			ReservationWaiter waiter = _reservationWaiters.peek();
			if(waiter == null)
				return;
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

	private void drainTaskWaitersOnce() {
		while(true) {
			if(_shutdown || _destroyed || !_reservationWaiters.isEmpty())
				return;
			ReservationWaiter waiter = _taskWaiters.peek();
			if(waiter == null || !canAdmitTask(waiter.bytes))
				return;
			boolean admitted;
			try {
				admitted = tryReserve(waiter.bytes);
			}
			catch(Throwable t) {
				_taskWaiters.remove(waiter);
				waiter.future.completeExceptionally(t);
				continue;
			}
			if(!admitted)
				return;
			if(_taskWaiters.remove(waiter))
				waiter.future.complete(null);
			else
				release(waiter.bytes);
		}
	}

	private boolean canAdmitTask(long bytes) {
		long parallelism = InfrastructureAnalyzer.getLocalParallelism();
		long active = getActiveMemory();
		long passive = getPassiveMemory();
		long activeLimit = saturatedMultiply(2 * parallelism, bytes);
		long passiveLimit = saturatedMultiply(2, Math.max(active, bytes));
		return active <= activeLimit - bytes && passive < passiveLimit;
	}

	private static long saturatedMultiply(long left, long right) {
		return left == 0 || right <= Long.MAX_VALUE / left ? left * right : Long.MAX_VALUE;
	}

	private boolean removeReservationWaiter(ReservationWaiter waiter) {
		return _reservationWaiters.remove(waiter);
	}

	private record ReservationWaiter(long bytes, OOCFuture<Void> future) {
	}
}
