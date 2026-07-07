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

package org.apache.sysds.runtime.ooc.store;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.util.ArrayDeque;
import java.util.concurrent.ExecutionException;
import java.util.function.IntConsumer;

final class OrderedMaterializedStoreReader<T extends SpillableObject>
	implements MaterializedStore.Reader<T>, StoreRegisteredReader, StoreLeaseReleaser {
	private final OOCCache _cache;
	private final long _streamId;
	private final MaterializedStore.AccessPattern _pattern;
	private final MemoryAllowance _allowance;
	private final int _maxPrefetch;
	private final Runnable _afterClose;
	private final IntConsumer _afterRelease;
	private final ArrayDeque<Request> _requests;
	private volatile boolean _closed;

	OrderedMaterializedStoreReader(OOCCache cache, long streamId, MaterializedStore.AccessPattern pattern,
		MemoryAllowance allowance, int maxPrefetch, Runnable afterClose, IntConsumer afterRelease) {
		this._cache = cache;
		this._streamId = streamId;
		this._pattern = pattern;
		this._allowance = allowance;
		this._maxPrefetch = maxPrefetch;
		this._afterClose = afterClose;
		this._afterRelease = afterRelease;
		_requests = new ArrayDeque<>(maxPrefetch);
	}

	@Override
	public MaterializedStore.Liveness liveness() {
		return _pattern;
	}

	@Override
	public boolean isClosed() {
		return _closed;
	}

	@Override
	public boolean hasNext() {
		checkReady();
		fill();
		return !_requests.isEmpty();
	}

	@Override
	public MaterializedStore.Lease<T> next() throws InterruptedException {
		checkReady();
		fill();
		if(_requests.isEmpty())
			throw new IllegalStateException("No remaining item");
		Request request = _requests.peekFirst();
		BlockEntry entry = awaitEntry(request);
		_requests.removeFirst();
		fill();
		return new StoreLease<>(this, request.index, entry);
	}

	@Override
	public void close() {
		if(_closed)
			return;
		_closed = true;
		Request request;
		while((request = _requests.pollFirst()) != null) {
			request.future.whenComplete((entry, error) -> {
				if(entry != null)
					_cache.unpin(entry, _allowance);
			});
		}
		_afterClose.run();
	}

	@Override
	public void release(int index, BlockEntry entry) {
		_cache.unpin(entry, _allowance);
		_pattern.consumed(index);
		_afterRelease.accept(index);
	}

	private void checkReady() {
		if(_closed)
			throw new IllegalStateException("Reader is closed");
	}

	private void fill() {
		while(_requests.size() < _maxPrefetch && _pattern.hasNext()) {
			int index = _pattern.next();
			OOCFuture<BlockEntry> future = _cache.pin(_streamId, index, _allowance);
			_requests.addLast(new Request(index, future));
		}
	}

	private BlockEntry awaitEntry(Request request) throws InterruptedException {
		try {
			BlockEntry entry = request.future.get();
			if(entry == null) {
				OOCFuture<BlockEntry> retried = new OOCFuture<>();
				StorePinRetry.pinWithRetry(_cache, _streamId, request.index, _allowance, () -> _closed, retried);
				request.future = retried;
				entry = retried.get();
				if(entry == null)
					throw new IllegalStateException("Reader is closed");
			}
			return entry;
		}
		catch(ExecutionException e) {
			throw DMLRuntimeException.of(e.getCause());
		}
	}

	private static final class Request {
		private final int index;
		private OOCFuture<BlockEntry> future;

		private Request(int index, OOCFuture<BlockEntry> future) {
			this.index = index;
			this.future = future;
		}
	}
}
