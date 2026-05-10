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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SyncMemoryAllowance implements MemoryAllowance {
	protected final MemoryBroker _broker;
	protected final long _consumptionLimit;
	protected final ExecutorService _waiter;
	protected volatile long _usedBytes;
	protected volatile long _grantedBytes;
	protected volatile long _targetBytes;
	protected volatile boolean _shutdown;
	protected volatile boolean _destroyed;

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
		_waiter = Executors.newSingleThreadExecutor();
		broker.attachAllowance(this);
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
			if(_usedBytes + bytes > _targetBytes)
				return false;
			if(_usedBytes + bytes <= _grantedBytes) {
				usedBefore = _usedBytes;
				grantedBefore = _grantedBytes;
				targetBefore = _targetBytes;
				_usedBytes += bytes;
				System.out.println("[ALLOW-RESERVE-FAST] allowance=" + dbgId() + " bytes=" + bytes
					+ " used=" + usedBefore + "->" + _usedBytes + " granted=" + grantedBefore
					+ " target=" + targetBefore);
				return true;
			}
			minRequest = _usedBytes + bytes - _grantedBytes;
			maxRequest = Math.max(minRequest, Math.max(_grantedBytes, bytes) * 2);
		}

		if(bytes > _consumptionLimit)
			throw new IllegalArgumentException("Cannot reserve more memory than the consumption limit");

		long granted = _broker.requestMemory(this, minRequest, maxRequest);
		long refund = 0;
		boolean success = false;
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
	public CompletableFuture<Void> reserve(long bytes) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		_waiter.submit(() -> {
			reserveBlocking(bytes);
			future.complete(null);
		});
		return future;
	}

	@Override
	public void release(long bytes) {
		long freedMemory = 0;
		long destroyFreedMemory = 0;
		boolean destroy = false;
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
			System.out.println("[ALLOW-RELEASE] allowance=" + dbgId() + " bytes=" + bytes
				+ " used=" + usedBefore + "->" + _usedBytes
				+ " granted=" + grantedBefore + "->" + _grantedBytes
				+ " target=" + targetBefore + " shutdown=" + _shutdown + " destroyed=" + _destroyed
				+ " freedMemory=" + freedMemory + " destroy=" + destroy);
			notifyAll();
		}
		if(destroy)
			_broker.destroyAllowance(this, destroyFreedMemory);
		else if(freedMemory > 0)
			_broker.freeMemory(this, freedMemory);
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
	public synchronized void setTargetMemory(long targetMemory) {
		if(_shutdown || _destroyed)
			return;
		long oldTarget = _targetBytes;
		_targetBytes = Math.min(targetMemory, _consumptionLimit);
		System.out.println("[ALLOW-TARGET] allowance=" + dbgId() + " target=" + oldTarget + "->" + _targetBytes
			+ " used=" + _usedBytes + " granted=" + _grantedBytes);
		notifyAll();
	}

	@Override
	public void shutdown() {
		long freedMemory = 0;
		long destroyFreedMemory = 0;
		boolean destroy = false;
		synchronized(this) {
			if(_shutdown || _destroyed)
				return;
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
			notifyAll();
		}
		System.out.println("[ALLOW-SHUTDOWN-END] allowance=" + dbgId() + " used=" + _usedBytes
			+ " granted=" + _grantedBytes + " target=" + _targetBytes + " destroy=" + destroy
			+ " freedMemory=" + freedMemory + " destroyFreed=" + destroyFreedMemory);
		_broker.shutdownAllowance(this);
		if(destroy)
			_broker.destroyAllowance(this, destroyFreedMemory);
		else if(freedMemory > 0)
			_broker.freeMemory(this, freedMemory);
		_waiter.shutdownNow();
	}

	@Override
	public boolean isShutdown() {
		return _shutdown || _destroyed;
	}

	private String dbgId() {
		return getClass().getSimpleName() + "@" + System.identityHashCode(this);
	}
}
