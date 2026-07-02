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

package org.apache.sysds.runtime.ooc.stream;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

/**
 * Stream wrapper that forwards source callbacks only after reserving the requested operator memory.
 * The reservation is not attached to the callback; the consuming operator owns the accounting.
 */
public final class AllocatedOOCStream<T> extends SubscribableTaskQueue<T> {
	private final OOCStream<T> _source;
	private final MemoryAllowance _allowance;
	private final ToLongFunction<T> _allocFn;
	private final boolean _reserve;
	private final AtomicInteger _pending;
	private final AtomicBoolean _closed;
	private volatile DMLRuntimeException _failure;

	public AllocatedOOCStream(OOCStream<T> source, MemoryAllowance allowance, ToLongFunction<T> allocFn,
		boolean reserve) {
		_source = source;
		_allowance = allowance;
		_allocFn = allocFn;
		_reserve = reserve;
		_pending = new AtomicInteger(1);
		_closed = new AtomicBoolean(false);
		source.setSubscriber(this::admit);
	}

	private void admit(OOCStream.QueueCallback<T> callback) {
		try {
			if(callback.isFailure()) {
				DMLRuntimeException failure;
				try {
					callback.get();
					failure = new DMLRuntimeException("Source stream failed without cause.");
				}
				catch(DMLRuntimeException ex) {
					failure = ex;
				}
				callback.close();
				fail(failure);
				releasePending();
				return;
			}
			if(callback.isEos()) {
				callback.close();
				releasePending();
				return;
			}
			long bytes = _reserve ? _allocFn.applyAsLong(callback.get()) : 0;
			if(bytes > 0 && OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC ADMIT TRACE] admit source=" + System.identityHashCode(_source)
					+ " stream=" + System.identityHashCode(this)
					+ " bytes=" + bytes
					+ " allowance=" + System.identityHashCode(_allowance)
					+ " cb=" + System.identityHashCode(callback));
			if(bytes < 0)
				throw new IllegalArgumentException("Cannot allocate negative bytes: " + bytes);
			if(!_reserve || bytes == 0 || _allowance.tryReserve(bytes)) {
				forward(callback, bytes);
				return;
			}
			retainUntilAllocated(callback, bytes);
		}
		catch(Throwable t) {
			callback.close();
			fail(DMLRuntimeException.of(t));
		}
	}

	private void retainUntilAllocated(OOCStream.QueueCallback<T> callback, long bytes) {
		OOCStream.QueueCallback<T> retained = callback.keepOpen();
		_pending.incrementAndGet();
		callback.close();
		OOCFuture<Void> reservation;
		try {
			reservation = _allowance.reserveAsync(bytes);
		}
		catch(Throwable t) {
			retained.close();
			releasePending();
			throw t;
		}
		reservation.whenComplete((ignored, error) -> {
			if(error != null) {
				retained.close();
				fail(DMLRuntimeException.of(error));
				releasePending();
				return;
			}
			if(_failure != null) {
				_allowance.release(bytes);
				retained.close();
				releasePending();
				return;
			}
			try {
				forward(retained, bytes);
			}
			catch(Throwable t) {
				fail(DMLRuntimeException.of(t));
			}
			finally {
				releasePending();
			}
		});
	}

	private void forward(OOCStream.QueueCallback<T> callback, long reservedBytes) {
		OOCStream.QueueCallback<T> retained = callback.keepOpen();
		try {
			if(reservedBytes > 0 && OOCDebug.TRACE_HOT_PATH)
				System.out.println("[OOC ADMIT TRACE] forward stream=" + System.identityHashCode(this)
					+ " bytes=" + reservedBytes
					+ " allowance=" + System.identityHashCode(_allowance)
					+ " cb=" + System.identityHashCode(callback)
					+ " retained=" + System.identityHashCode(retained));
			enqueue(retained);
			retained = null;
		}
		catch(Throwable t) {
			if(reservedBytes > 0)
				_allowance.release(reservedBytes);
			throw t;
		}
		finally {
			if(retained != null)
				retained.close();
			callback.close();
		}
	}

	private void fail(DMLRuntimeException failure) {
		if(_failure != null)
			return;
		_failure = failure;
		super.propagateFailure(failure);
	}

	private void releasePending() {
		if(_pending.decrementAndGet() == 0 && _closed.compareAndSet(false, true))
			closeInput();
	}

	@Override
	public void propagateFailure(DMLRuntimeException re) {
		boolean firstFailure = _failure == null;
		_failure = re;
		super.propagateFailure(re);
		if(firstFailure)
			_source.propagateFailure(re);
	}
}
