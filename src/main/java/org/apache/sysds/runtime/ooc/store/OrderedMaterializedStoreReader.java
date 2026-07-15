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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

public final class OrderedMaterializedStoreReader<T extends SpillableObject> implements MaterializedStore.StoreReader<T> {
	private static final Request CLOSED = new Request(-1, OOCFuture.completed(null));

	private final OOCCache _cache;
	private final long _streamId;
	private final MaterializedStore.AccessPattern _pattern;
	private final MemoryAllowance _allowance;
	private final int _maxPrefetch;
	private final boolean _softOrdering;
	private final Runnable _afterClose;
	private final IntConsumer _afterRelease;
	private final ArrayDeque<Request> _requests;
	private final BlockingQueue<Request> _readyRequests;
	private final AtomicInteger _inFlightRequests;
	private volatile boolean _closed;

	OrderedMaterializedStoreReader(OOCCache cache, long streamId, MaterializedStore.AccessPattern pattern,
		MemoryAllowance allowance, int maxPrefetch, boolean softOrdering, Runnable afterClose,
		IntConsumer afterRelease) {
		_cache = cache;
		_streamId = streamId;
		_pattern = pattern;
		_allowance = allowance;
		_maxPrefetch = maxPrefetch;
		_softOrdering = softOrdering;
		_afterClose = afterClose;
		_afterRelease = afterRelease;
		_requests = new ArrayDeque<>(maxPrefetch);
		_readyRequests = softOrdering ? new LinkedBlockingQueue<>() : null;
		_inFlightRequests = new AtomicInteger();
	}

	@Override
	public MaterializedStore.Liveness liveness() {
		return _pattern;
	}

	@Override
	public boolean isClosed() {
		return _closed;
	}

	public boolean hasNext() {
		if(_softOrdering) {
			checkReady();
			fillSoft();
			return _inFlightRequests.get() > 0;
		}
		synchronized(this) {
			checkReady();
			fillStrict();
			return !_requests.isEmpty();
		}
	}

	public MaterializedStore.Lease<T> next() throws InterruptedException {
		if(_softOrdering) {
			checkReady();
			return nextSoft();
		}
		Request request;
		synchronized(this) {
			checkReady();
			fillStrict();
			if(_requests.isEmpty())
				throw new IllegalStateException("No remaining item");
			request = _requests.removeFirst();
		}
		BlockEntry entry;
		try {
			entry = awaitEntry(request);
		}
		catch(InterruptedException | RuntimeException ex) {
			releaseWhenReady(request);
			throw ex;
		}
		synchronized(this) {
			if(_closed) {
				_cache.unpin(entry, _allowance);
				throw new IllegalStateException("Reader is closed");
			}
			try {
				fillStrict();
			}
			catch(RuntimeException ex) {
				_cache.unpin(entry, _allowance);
				throw ex;
			}
		}
		return new StoreLease<>(lease -> release(lease.index(), lease.entryUnsafe()), request.index, entry);
	}

	@Override
	public void close() {
		ArrayDeque<Request> pending = new ArrayDeque<>();
		synchronized(this) {
			if(_closed)
				return;
			_closed = true;
			if(!_softOrdering) {
				Request request;
				while((request = _requests.pollFirst()) != null)
					pending.addLast(request);
			}
		}
		if(_softOrdering) {
			Request request;
			while((request = _readyRequests.poll()) != null) {
				if(request.entry != null)
					_cache.unpin(request.entry, _allowance);
				_inFlightRequests.decrementAndGet();
			}
			_readyRequests.offer(CLOSED);
		}
		else
			for(Request request : pending)
				releaseWhenReady(request);
		_afterClose.run();
	}

	public void release(int index, BlockEntry entry) {
		_cache.unpin(entry, _allowance);
		_pattern.consumed(index);
		_afterRelease.accept(index);
	}

	private void checkReady() {
		if(_closed)
			throw new IllegalStateException("Reader is closed");
	}

	private MaterializedStore.Lease<T> nextSoft() throws InterruptedException {
		fillSoft();
		if(_inFlightRequests.get() <= 0)
			throw new IllegalStateException("No remaining item");
		Request request = _readyRequests.take();
		if(request == CLOSED)
			throw new IllegalStateException("Reader is closed");
		_inFlightRequests.decrementAndGet();
		if(request.error != null)
			throw DMLRuntimeException.of(request.error);
		if(request.entry == null)
			throw new IllegalStateException("Reader is closed");
		fillSoft();
		return new StoreLease<>(lease -> release(lease.index(), lease.entryUnsafe()), request.index, request.entry);
	}

	private void fillStrict() {
		while(_requests.size() < _maxPrefetch && _pattern.hasNext()) {
			int index = _pattern.next();
			OOCFuture<BlockEntry> future = _cache.pin(_streamId, index, _allowance);
			_requests.addLast(new Request(index, future));
		}
	}

	private void fillSoft() {
		while(_inFlightRequests.get() < _maxPrefetch && _pattern.hasNext()) {
			int index = _pattern.next();
			OOCFuture<BlockEntry> future = _cache.pin(_streamId, index, _allowance);
			_inFlightRequests.incrementAndGet();
			registerSoftRequest(new Request(index, future));
		}
	}

	private void registerSoftRequest(Request request) {
		request.future.whenComplete((entry, error) -> {
			if(error != null || entry != null) {
				completeSoft(request, entry, error);
				return;
			}
			request.future = StorePinAdmission.pinAdmitted(_cache, _streamId, request.index, _allowance,
				() -> _closed);
			request.future.whenComplete((admittedEntry, admittedError) ->
				completeSoft(request, admittedEntry, admittedError));
		});
	}

	private void completeSoft(Request request, BlockEntry entry, Throwable error) {
		if(_closed) {
			if(entry != null)
				_cache.unpin(entry, _allowance);
			_inFlightRequests.decrementAndGet();
			return;
		}
		request.entry = entry;
		request.error = error;
		_readyRequests.offer(request);
		if(_closed && _readyRequests.remove(request)) {
			if(entry != null)
				_cache.unpin(entry, _allowance);
			_inFlightRequests.decrementAndGet();
		}
	}

	private BlockEntry awaitEntry(Request request) throws InterruptedException {
		try {
			BlockEntry entry = request.future.get();
			if(entry == null) {
				request.future = StorePinAdmission.pinAdmitted(_cache, _streamId, request.index, _allowance,
					() -> _closed);
				entry = request.future.get();
				if(entry == null)
					throw new IllegalStateException("Reader is closed");
			}
			return entry;
		}
		catch(ExecutionException e) {
			throw DMLRuntimeException.of(e.getCause());
		}
	}

	private void releaseWhenReady(Request request) {
		request.future.whenComplete((entry, error) -> {
			if(entry != null)
				_cache.unpin(entry, _allowance);
		});
	}

	private static final class Request {
		private final int index;
		private OOCFuture<BlockEntry> future;
		private BlockEntry entry;
		private Throwable error;

		private Request(int index, OOCFuture<BlockEntry> future) {
			this.index = index;
			this.future = future;
		}
	}
}
