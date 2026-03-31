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
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;

import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryQueueCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
	private CallbackHandle _handle;
	private boolean _closed;

	public InMemoryQueueCallback(IndexedMatrixValue result, DMLRuntimeException failure, MemoryAllowance allow, long reservedBytes) {
		_handle = new CallbackHandle(result, failure, allow, reservedBytes);
		_closed = false;
	}

	private InMemoryQueueCallback(CallbackHandle handle) {
		_handle = handle;
		_closed = false;
	}

	@Override
	public IndexedMatrixValue get() {
		if (_handle._failure != null)
			throw _handle._failure;
		return _handle._result;
	}

	@Override
	public synchronized OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
		if(_closed)
			throw new IllegalStateException("Cannot keep open a closed callback");
		_handle._refCtr.incrementAndGet();
		return new InMemoryQueueCallback(_handle);
	}

	@Override
	public void fail(DMLRuntimeException failure) {
		_handle._failure = failure;
	}

	@Override
	public synchronized void close() {
		if(_closed)
			return;
		_closed = true;
		if(_handle._refCtr.decrementAndGet() == 0)
			_handle._allow.release(_handle._reservedBytes);
		_handle = null;
	}

	@Override
	public boolean isEos() {
		return _handle._result == null && _handle._failure == null;
	}

	@Override
	public boolean isFailure() {
		return _handle._failure != null;
	}

	private static class CallbackHandle {
		private final IndexedMatrixValue _result;
		private final AtomicInteger _refCtr;
		private final MemoryAllowance _allow;
		private final long _reservedBytes;
		private DMLRuntimeException _failure;

		private CallbackHandle(IndexedMatrixValue result, DMLRuntimeException failure, MemoryAllowance allow, long _reservedBytes) {
			this._result = result;
			this._failure = failure;
			this._refCtr = new AtomicInteger(1);
			this._allow = allow;
			this._reservedBytes = _reservedBytes;
		}
	}
}
