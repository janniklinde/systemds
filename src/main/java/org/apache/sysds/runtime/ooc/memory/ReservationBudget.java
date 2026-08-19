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

import org.apache.sysds.runtime.ooc.cache.OOCFuture;

public final class ReservationBudget implements MemoryAllowance, AutoCloseable {
	private final MemoryAllowance _parent;
	private long _outstanding;
	private long _available;
	private boolean _closed;
	private boolean _reusable;

	public ReservationBudget(MemoryAllowance parent, long bytes) {
		if(parent == null)
			throw new NullPointerException("parent");
		if(bytes < 0)
			throw new IllegalArgumentException("Budget must not be negative: " + bytes);
		_parent = parent;
		_outstanding = bytes;
		_available = bytes;
	}

	public synchronized ReservationBudget enableReuse() {
		if(_closed || getUsedMemory() != 0)
			throw new IllegalStateException("Budget reuse must be enabled before reserving memory.");
		_reusable = true;
		return this;
	}

	@Override
	public synchronized boolean tryReserve(long bytes) {
		checkNonNegative(bytes);
		if(bytes == 0)
			return true;
		if(_closed || _available < bytes)
			return false;
		_available -= bytes;
		return true;
	}

	/**
	 * Reserves from the budget, topping it up from the parent allowance when the pre-reservation falls short. The
	 * up-front amount is an estimate from the block geometry, while the charge for the value actually produced is the
	 * truth. Failing here would abort the plan over an underestimate, so the shortfall is granted separately instead.
	 */
	@Override
	public void reserveBlocking(long bytes) {
		checkNonNegative(bytes);
		if(bytes == 0)
			return;
		while(true) {
			long shortfall;
			synchronized(this) {
				if(_closed)
					throw insufficientBudget(bytes);
				if(_available >= bytes) {
					_available -= bytes;
					return;
				}
				shortfall = bytes - _available;
			}
			_parent.reserveBlocking(shortfall);
			boolean revoked;
			synchronized(this) {
				revoked = _closed;
				if(!revoked) {
					_outstanding += shortfall;
					_available += shortfall;
				}
			}
			if(revoked) {
				_parent.release(shortfall);
				throw insufficientBudget(bytes);
			}
		}
	}

	@Override
	public OOCFuture<Void> reserveAsync(long bytes) {
		return tryReserve(bytes) ? OOCFuture.completed(null) : OOCFuture.failed(insufficientBudget(bytes));
	}

	@Override
	public void release(long bytes) {
		checkNonNegative(bytes);
		if(bytes == 0)
			return;
		boolean releaseParent;
		synchronized(this) {
			long used = _outstanding - _available;
			if(bytes > used)
				throw new IllegalStateException("Cannot release " + bytes + " bytes from a budget using " + used);
			releaseParent = _closed || !_reusable;
			if(releaseParent) {
				_outstanding -= bytes;
				if(_closed)
					_parent.removePassiveMemory(bytes);
			}
			else
				_available += bytes;
		}
		if(releaseParent)
			_parent.release(bytes);
	}

	@Override
	public synchronized long getUsedMemory() {
		return _outstanding - _available;
	}

	@Override
	public synchronized long getGrantedMemory() {
		return _outstanding;
	}

	@Override
	public synchronized long getTargetMemory() {
		return _outstanding;
	}

	@Override
	public void setTargetMemory(long targetMemory) {
		throw new UnsupportedOperationException("Reservation budgets have a fixed target");
	}

	@Override
	public void shutdown() {
		close();
	}

	@Override
	public synchronized boolean isShutdown() {
		return _closed || _parent.isShutdown();
	}

	@Override
	public void close() {
		long released;
		synchronized(this) {
			if(_closed)
				return;
			_closed = true;
			released = _available;
			_available = 0;
			_outstanding -= released;
			_parent.addPassiveMemory(_outstanding);
		}
		if(released > 0)
			_parent.release(released);
	}

	private synchronized IllegalStateException insufficientBudget(long bytes) {
		return new IllegalStateException(
			"Cannot reserve " + bytes + " bytes from a budget with " + _available + " bytes available");
	}

	private static void checkNonNegative(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Bytes must not be negative: " + bytes);
	}
}
