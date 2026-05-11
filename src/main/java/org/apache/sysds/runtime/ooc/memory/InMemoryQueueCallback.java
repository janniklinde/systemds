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
import org.apache.sysds.runtime.ooc.OOCDebug;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryQueueCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
	private static final Map<CallbackHandle, DebugInfo> LIVE_HANDLES = new ConcurrentHashMap<>();

	private CallbackHandle _handle;
	private boolean _closed;

	public InMemoryQueueCallback(IndexedMatrixValue result, DMLRuntimeException failure, MemoryAllowance allow,
		long reservedBytes) {
		String origin = (OOCDebug.TRACK_LIVE_STATE || OOCDebug.TRACE_HOT_PATH) ? creationOrigin() : null;
		_handle = new CallbackHandle(result, failure, allow, reservedBytes);
		if(OOCDebug.TRACK_LIVE_STATE)
			LIVE_HANDLES.put(_handle, new DebugInfo(origin));
		_closed = false;
		OOCDebug.trace(() -> "[CB-CREATE] handle=" + System.identityHashCode(_handle)
			+ " cb=" + System.identityHashCode(this)
			+ " bytes=" + reservedBytes
			+ " allow=" + (allow == null ? "null" : allow.getClass().getSimpleName() + "@"
				+ System.identityHashCode(allow))
			+ " origin=" + origin);
	}

	private InMemoryQueueCallback(CallbackHandle handle) {
		_handle = handle;
		_closed = false;
		OOCDebug.trace(() -> "[CB-ALIAS] handle=" + System.identityHashCode(_handle)
			+ " cb=" + System.identityHashCode(this)
			+ " refs=" + _handle._refCtr.get()
			+ " allow=" + (_handle._allow == null ? "null" : _handle._allow.getClass().getSimpleName() + "@"
				+ System.identityHashCode(_handle._allow)));
	}

	@Override
	public IndexedMatrixValue get() {
		return _handle.get();
	}

	@Override
	public synchronized OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
		if(_closed)
			throw new IllegalStateException("Cannot keep open a closed callback");
		int oldRefs = _handle._refCtr.getAndIncrement();
		OOCDebug.trace(() -> "[CB-KEEP] handle=" + System.identityHashCode(_handle)
			+ " cb=" + System.identityHashCode(this)
			+ " refs=" + oldRefs + "->" + (oldRefs + 1)
			+ " allow=" + (_handle._allow == null ? "null" : _handle._allow.getClass().getSimpleName() + "@"
				+ System.identityHashCode(_handle._allow))
			+ " bytes=" + _handle._reservedBytes);
		return new InMemoryQueueCallback(_handle);
	}

	@Override
	public void fail(DMLRuntimeException failure) {
		_handle._failure = failure;
	}

	@Override
	public long getManagedBytes() {
		synchronized(_handle) {
			return _handle._reservedBytes;
		}
	}

	@Override
	public OOCStream.QueueCallback<IndexedMatrixValue> tryTransferOwnership(MemoryAllowance allowance) {
		synchronized(_handle) {
			long bytes = _handle._reservedBytes;
			if(bytes <= 0 || _handle._allow == allowance)
				return this;
			if(_handle._cacheIdx >= 0)
				return null;
			if(allowance instanceof CachedAllowance)
				return null;
			if(!allowance.tryReserve(bytes))
				return null;
			_handle._allow.release(bytes);
			_handle._allow = allowance;
			return this;
		}
	}

	@Override
	public OOCStream.QueueCallback<IndexedMatrixValue> transferOwnershipBlocking(MemoryAllowance allowance) {
		transferOwnershipBlockingInternal(allowance);
		return this;
	}

	private void transferOwnershipBlockingInternal(MemoryAllowance allowance) {
		synchronized(_handle) {
			long bytes = _handle._reservedBytes;
			if(bytes <= 0 || _handle._allow == allowance)
				return;
			OOCDebug.trace(() -> "[CB-TRANSFER] handle=" + System.identityHashCode(_handle)
				+ " cb=" + System.identityHashCode(this)
				+ " bytes=" + bytes
				+ " from=" + _handle._allow.getClass().getSimpleName() + "@"
				+ System.identityHashCode(_handle._allow)
				+ " to=" + allowance.getClass().getSimpleName() + "@"
				+ System.identityHashCode(allowance)
				+ " refs=" + _handle._refCtr.get()
				+ " cacheIdx=" + _handle._cacheIdx);
			if(_handle._cacheIdx >= 0)
				throw new IllegalStateException("Cannot transfer ownership of a cached allowance callback.");
			if(allowance instanceof CachedAllowance cached)
				cached.admitBlocking(bytes);
			else
				allowance.reserveBlocking(bytes);
			_handle._allow.release(bytes);
			_handle._allow = allowance;
			OOCDebug.trace(() -> "[CB-TRANSFER-DONE] handle=" + System.identityHashCode(_handle)
				+ " newAllow=" + _handle._allow.getClass().getSimpleName() + "@"
				+ System.identityHashCode(_handle._allow)
				+ " bytes=" + _handle._reservedBytes);
		}
	}

	public long releaseManagedMemory() {
		synchronized(_handle) {
			long bytes = _handle._reservedBytes;
			if(bytes <= 0)
				return 0;
			OOCDebug.trace(() -> "[CB-RELEASE-MANAGED] handle=" + System.identityHashCode(_handle)
				+ " cb=" + System.identityHashCode(this)
				+ " bytes=" + bytes
				+ " allow=" + _handle._allow.getClass().getSimpleName() + "@"
				+ System.identityHashCode(_handle._allow)
				+ " refs=" + _handle._refCtr.get()
				+ " cacheIdx=" + _handle._cacheIdx);
			_handle._reservedBytes = 0;
			_handle._allow.release(bytes);
			return bytes;
		}
	}

	public long detachManagedMemoryForHandover(MemoryAllowance expectedAllowance) {
		synchronized(_handle) {
			if(_handle._allow != expectedAllowance)
				throw new IllegalStateException("Unexpected memory owner for handover.");
			long bytes = _handle._reservedBytes;
			OOCDebug.trace(() -> "[CB-DETACH-HANDOVER] handle=" + System.identityHashCode(_handle)
				+ " cb=" + System.identityHashCode(this)
				+ " bytes=" + bytes
				+ " allow=" + _handle._allow.getClass().getSimpleName() + "@"
				+ System.identityHashCode(_handle._allow)
				+ " refs=" + _handle._refCtr.get()
				+ " cacheIdx=" + _handle._cacheIdx);
			_handle._reservedBytes = 0;
			return bytes;
		}
	}

	@Override
	public synchronized void close() {
		if(_closed)
			return;
		_closed = true;
		if(_handle._refCtr.decrementAndGet() == 0)
			_handle.closeFinal();
		_handle = null;
	}

	@Override
	public boolean isEos() {
		return _handle.isEos();
	}

	@Override
	public boolean isFailure() {
		return _handle._failure != null;
	}

	CallbackHandle getHandle() {
		return _handle;
	}

	static final class CallbackHandle {
		private volatile IndexedMatrixValue _result;
		private final AtomicInteger _refCtr;
		private MemoryAllowance _allow;
		private long _reservedBytes;
		private volatile DMLRuntimeException _failure;
		private int _cacheIdx;

		private CallbackHandle(IndexedMatrixValue result, DMLRuntimeException failure, MemoryAllowance allow,
			long reservedBytes) {
			_result = result;
			_failure = failure;
			_refCtr = new AtomicInteger(1);
			_allow = allow;
			_reservedBytes = reservedBytes;
			_cacheIdx = -1;
		}

		private IndexedMatrixValue get() {
			if(_failure != null)
				throw _failure;
			return _result;
		}

		private boolean isEos() {
			return _failure == null && _result == null;
		}

		synchronized void attachCachedAllowance(CachedAllowance allowance, int index) {
			if(_allow != allowance)
				throw new IllegalStateException("Callback ownership must already belong to the cached allowance.");
			if(_cacheIdx >= 0 && _cacheIdx != index)
				throw new IllegalStateException("Callback is already attached to a different cached slot.");
			_cacheIdx = index;
		}

		synchronized void detachCachedAllowance() {
			_cacheIdx = -1;
		}

		boolean isExclusiveToRoot() {
			return _refCtr.get() == 1;
		}

		private synchronized IndexedMatrixValue takeManagedResultForHandover() {
			IndexedMatrixValue result = _result;
			_result = null;
			return result;
		}

		private void closeFinal() {
			if(_reservedBytes < 0) {
				throw new IllegalArgumentException("Callback reserved-bytes underflow before final close: reserved="
					+ _reservedBytes + ", allow=" + (_allow == null ? "null" : _allow.getClass().getSimpleName())
					+ ", cacheIdx=" + _cacheIdx + ", resultNull=" + (_result == null));
			}
			OOCDebug.trace(() -> "[CB-CLOSE-FINAL] handle=" + System.identityHashCode(this)
				+ " bytes=" + _reservedBytes
				+ " allow=" + (_allow == null ? "null" : _allow.getClass().getSimpleName() + "@"
					+ System.identityHashCode(_allow))
				+ " refs=" + _refCtr.get()
				+ " cacheIdx=" + _cacheIdx
				+ " resultNull=" + (_result == null));
			_result = null;
			_allow.release(_reservedBytes);
			_reservedBytes = 0;
			_cacheIdx = -1;
			if(OOCDebug.TRACK_LIVE_STATE)
				LIVE_HANDLES.remove(this);
		}
	}

	public IndexedMatrixValue takeManagedResultForHandover() {
		return _handle.takeManagedResultForHandover();
	}

	private static String creationOrigin() {
		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		for(int i = 0; i < trace.length; i++) {
			StackTraceElement e = trace[i];
			if(!e.getClassName().equals(InMemoryQueueCallback.class.getName())
				&& !e.getClassName().equals(Thread.class.getName()))
				return e.getClassName() + ":" + e.getLineNumber();
		}
		return "unknown";
	}

	public static String dumpLiveHandles() {
		if(!OOCDebug.TRACK_LIVE_STATE)
			return "Live InMemoryQueueCallback handles tracking disabled\n";
		StringBuilder sb = new StringBuilder();
		ArrayList<Map.Entry<CallbackHandle, DebugInfo>> entries = new ArrayList<>(LIVE_HANDLES.entrySet());
		entries.sort(Comparator.comparingInt(e -> System.identityHashCode(e.getKey())));
		sb.append("Live InMemoryQueueCallback handles: ").append(entries.size()).append('\n');
		for(Map.Entry<CallbackHandle, DebugInfo> entry : entries) {
			CallbackHandle handle = entry.getKey();
			synchronized(handle) {
				sb.append("  handle=").append(System.identityHashCode(handle))
					.append(" origin=").append(entry.getValue()._origin)
					.append(" lastEscape=").append(entry.getValue()._lastEscape)
					.append(" allow=").append(handle._allow == null ? "null" :
						handle._allow.getClass().getSimpleName() + "@" + System.identityHashCode(handle._allow))
					.append(" reserved=").append(handle._reservedBytes)
					.append(" refs=").append(handle._refCtr.get())
					.append(" cacheIdx=").append(handle._cacheIdx)
					.append(" resultNull=").append(handle._result == null)
					.append(" failure=").append(handle._failure != null)
					.append('\n');
			}
		}
		return sb.toString();
	}

	public static boolean hasLiveHandles() {
		return OOCDebug.TRACK_LIVE_STATE && !LIVE_HANDLES.isEmpty();
	}

	public static void noteEscape(InMemoryQueueCallback cb, String escape) {
		if(!OOCDebug.TRACK_LIVE_STATE)
			return;
		if(cb == null || cb._handle == null)
			return;
		DebugInfo info = LIVE_HANDLES.get(cb._handle);
		if(info != null)
			info._lastEscape = escape;
	}

	private static final class DebugInfo {
		private final String _origin;
		private volatile String _lastEscape;

		private DebugInfo(String origin) {
			_origin = origin;
			_lastEscape = "unrecorded";
		}
	}
}
