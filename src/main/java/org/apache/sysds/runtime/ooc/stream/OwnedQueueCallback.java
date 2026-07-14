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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;

import java.util.concurrent.atomic.AtomicInteger;

/** Queue callback whose aliases jointly own an AutoCloseable value. */
public final class OwnedQueueCallback<T extends AutoCloseable> implements OOCStream.QueueCallback<T> {
	private final Shared<T> _shared;
	private boolean _closed;

	public OwnedQueueCallback(T value) {
		_shared = new Shared<>(value);
	}

	private OwnedQueueCallback(Shared<T> shared) {
		_shared = shared;
	}

	@Override
	public T get() {
		if(_shared.failure != null)
			throw _shared.failure;
		return _shared.value;
	}

	@Override
	public synchronized OOCStream.QueueCallback<T> keepOpen() {
		if(_closed)
			throw new IllegalStateException("Cannot keep open a closed callback.");
		_shared.refs.incrementAndGet();
		return new OwnedQueueCallback<>(_shared);
	}

	@Override
	public synchronized void close() {
		if(_closed)
			return;
		_closed = true;
		if(_shared.refs.decrementAndGet() != 0)
			return;
		try {
			_shared.value.close();
		}
		catch(Exception ex) {
			throw DMLRuntimeException.of(ex);
		}
	}

	@Override
	public void fail(DMLRuntimeException failure) {
		_shared.failure = failure;
	}

	@Override
	public boolean isEos() {
		return false;
	}

	@Override
	public boolean isFailure() {
		return _shared.failure != null;
	}

	private static final class Shared<T extends AutoCloseable> {
		private final T value;
		private final AtomicInteger refs = new AtomicInteger(1);
		private volatile DMLRuntimeException failure;

		private Shared(T value) {
			this.value = value;
		}
	}
}
