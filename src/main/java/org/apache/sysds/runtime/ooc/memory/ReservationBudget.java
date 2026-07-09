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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Finite child allowance backed by one already-admitted parent reservation.
 *
 * Sub-reservations consume local budget only and never wait on the parent. Bytes released through the
 * MemoryAllowance API must have been reserved from this budget first. Unused budget can be detached to
 * transfer ownership back to the parent-level accounting path, or released directly to the parent.
 */
public final class ReservationBudget implements MemoryAllowance, AutoCloseable {
	private final MemoryAllowance _parent;
	private final long _initialBytes;
	private final AtomicReference<State> _state;

	private ReservationBudget(MemoryAllowance parent, long bytes) {
		if(parent == null)
			throw new NullPointerException("parent");
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot create a negative reservation budget: " + bytes);
		_parent = parent;
		_initialBytes = bytes;
		_state = new AtomicReference<>(new State(bytes, bytes, false));
	}

	public static ReservationBudget admitted(MemoryAllowance parent, long bytes) {
		return new ReservationBudget(parent, bytes);
	}

	@Override
	public boolean tryReserve(long bytes) {
		checkNonNegative(bytes);
		if(bytes == 0)
			return true;
		while(true) {
			State current = _state.get();
			if(current.closed || current.available < bytes)
				return false;
			State next = new State(current.outstanding, current.available - bytes, false);
			if(_state.compareAndSet(current, next))
				return true;
		}
	}

	@Override
	public void reserveBlocking(long bytes) {
		if(!tryReserve(bytes))
			throw insufficientBudget(bytes);
	}

	@Override
	public CompletableFuture<Void> reserve(long bytes) {
		return tryReserve(bytes) ? CompletableFuture.completedFuture(null) :
			CompletableFuture.failedFuture(insufficientBudget(bytes));
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
		while(true) {
			State current = _state.get();
			long consumed = current.outstanding - current.available;
			if(consumed < bytes)
				throw new IllegalStateException("Cannot release " + bytes + " bytes from reservation budget with only "
					+ consumed + " consumed bytes.");
			State next = new State(current.outstanding - bytes, current.available, current.closed);
			if(_state.compareAndSet(current, next)) {
				_parent.release(bytes);
				return;
			}
		}
	}

	public long releaseUnused(long bytes) {
		checkNonNegative(bytes);
		if(bytes == 0)
			return 0;
		while(true) {
			State current = _state.get();
			if(current.available < bytes)
				throw new IllegalStateException("Cannot release " + bytes + " unused bytes from reservation budget with only "
					+ current.available + " available bytes.");
			State next = new State(current.outstanding - bytes, current.available - bytes, current.closed);
			if(_state.compareAndSet(current, next)) {
				_parent.release(bytes);
				return bytes;
			}
		}
	}

	public long detachUnused() {
		while(true) {
			State current = _state.get();
			long detached = current.available;
			if(detached == 0)
				return 0;
			State next = new State(current.outstanding - detached, 0, current.closed);
			if(_state.compareAndSet(current, next))
				return detached;
		}
	}

	public long getAvailableMemory() {
		return _state.get().available;
	}

	@Override
	public long getUsedMemory() {
		State current = _state.get();
		return current.outstanding - current.available;
	}

	@Override
	public long getGrantedMemory() {
		return _state.get().outstanding;
	}

	@Override
	public long getTargetMemory() {
		return getGrantedMemory();
	}

	@Override
	public long getMinimumOperatingMemory() {
		return _parent.getMinimumOperatingMemory();
	}

	@Override
	public void setTargetMemory(long targetMemory) {
		throw new UnsupportedOperationException("Reservation budgets have a fixed target.");
	}

	@Override
	public void shutdown() {
		close();
	}

	@Override
	public boolean isShutdown() {
		return _parent.isShutdown() || _state.get().closed;
	}

	@Override
	public void close() {
		long released = 0;
		while(true) {
			State current = _state.get();
			if(current.closed)
				return;
			released = current.available;
			State next = new State(current.outstanding - released, 0, true);
			if(_state.compareAndSet(current, next))
				break;
		}
		if(released > 0)
			_parent.release(released);
	}

	@Override
	public String toString() {
		State current = _state.get();
		return "ReservationBudget{initial=" + _initialBytes + ", outstanding=" + current.outstanding
			+ ", available=" + current.available + ", closed=" + current.closed + '}';
	}

	private IllegalStateException insufficientBudget(long bytes) {
		State current = _state.get();
		return new IllegalStateException("Reservation budget cannot reserve " + bytes + " bytes; available="
			+ current.available + ", outstanding=" + current.outstanding + ", closed=" + current.closed + '.');
	}

	private static void checkNonNegative(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot reserve or release negative bytes: " + bytes);
	}

	private static final class State {
		private final long outstanding;
		private final long available;
		private final boolean closed;

		private State(long outstanding, long available, boolean closed) {
			if(outstanding < 0 || available < 0 || available > outstanding)
				throw new IllegalArgumentException("Invalid reservation budget state: outstanding=" + outstanding
					+ ", available=" + available);
			this.outstanding = outstanding;
			this.available = available;
			this.closed = closed;
		}
	}
}
