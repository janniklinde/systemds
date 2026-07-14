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

final class OrderedMaterializedStoreReader<T extends SpillableObject>
	implements MaterializedStore.Reader<T>, StoreRegisteredReader {
	private final OOCCache cache;
	private final long streamId;
	private final MaterializedStore.AccessPattern pattern;
	private final MemoryAllowance allowance;
	private final int maxPrefetch;
	private final boolean softOrdering;
	private final Runnable afterClose;
	private final IntConsumer afterRelease;
	private final ArrayDeque<Request> requests;
	private final BlockingQueue<Request> readyRequests;
	private final AtomicInteger inFlightRequests;
	private volatile boolean closed;

	OrderedMaterializedStoreReader(OOCCache cache, long streamId, MaterializedStore.AccessPattern pattern,
		MemoryAllowance allowance, int maxPrefetch, boolean softOrdering, Runnable afterClose,
		IntConsumer afterRelease) {
		this.cache = cache;
		this.streamId = streamId;
		this.pattern = pattern;
		this.allowance = allowance;
		this.maxPrefetch = maxPrefetch;
		this.softOrdering = softOrdering;
		this.afterClose = afterClose;
		this.afterRelease = afterRelease;
		requests = new ArrayDeque<>(maxPrefetch);
		readyRequests = softOrdering ? new LinkedBlockingQueue<>() : null;
		inFlightRequests = new AtomicInteger();
	}

	@Override
	public MaterializedStore.Liveness liveness() {
		return pattern;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public boolean hasNext() {
		checkReady();
		if(softOrdering) {
			fillSoft();
			return inFlightRequests.get() > 0;
		}
		fillStrict();
		return !requests.isEmpty();
	}

	@Override
	public MaterializedStore.Lease<T> next() throws InterruptedException {
		checkReady();
		if(softOrdering)
			return nextSoft();
		fillStrict();
		if(requests.isEmpty())
			throw new IllegalStateException("No remaining item");
		Request request = requests.peekFirst();
		BlockEntry entry = awaitEntry(request);
		requests.removeFirst();
		fillStrict();
		return new StoreLease<>(lease -> release(lease.index(), lease.entry()), request.index, entry);
	}

	@Override
	public void close() {
		if(closed)
			return;
		closed = true;
		if(softOrdering) {
			Request request;
			while((request = readyRequests.poll()) != null) {
				if(request.entry != null)
					cache.unpin(request.entry, allowance);
				inFlightRequests.decrementAndGet();
			}
		}
		else {
			Request request;
			while((request = requests.pollFirst()) != null) {
				request.future.whenComplete((entry, error) -> {
					if(entry != null)
						cache.unpin(entry, allowance);
				});
			}
		}
		afterClose.run();
	}

	public void release(int index, BlockEntry entry) {
		cache.unpin(entry, allowance);
		pattern.consumed(index);
		afterRelease.accept(index);
	}

	private void checkReady() {
		if(closed)
			throw new IllegalStateException("Reader is closed");
	}

	private MaterializedStore.Lease<T> nextSoft() throws InterruptedException {
		fillSoft();
		if(inFlightRequests.get() <= 0)
			throw new IllegalStateException("No remaining item");
		Request request = readyRequests.take();
		inFlightRequests.decrementAndGet();
		if(request.error != null)
			throw DMLRuntimeException.of(request.error);
		if(request.entry == null)
			throw new IllegalStateException("Reader is closed");
		fillSoft();
		return new StoreLease<>(lease -> release(lease.index(), lease.entryUnsafe()), request.index, request.entry);
	}

	private void fillStrict() {
		while(requests.size() < maxPrefetch && pattern.hasNext()) {
			int index = pattern.next();
			OOCFuture<BlockEntry> future = cache.pin(streamId, index, allowance);
			requests.addLast(new Request(index, future));
		}
	}

	private void fillSoft() {
		while(inFlightRequests.get() < maxPrefetch && pattern.hasNext()) {
			int index = pattern.next();
			OOCFuture<BlockEntry> future = cache.pin(streamId, index, allowance);
			inFlightRequests.incrementAndGet();
			registerSoftRequest(new Request(index, future));
		}
	}

	private void registerSoftRequest(Request request) {
		request.future.whenComplete((entry, error) -> {
			if(error != null || entry != null) {
				completeSoft(request, entry, error);
				return;
			}
			request.future = StorePinAdmission.pinAdmitted(cache, streamId, request.index, allowance,
				() -> closed);
			request.future.whenComplete((admittedEntry, admittedError) ->
				completeSoft(request, admittedEntry, admittedError));
		});
	}

	private void completeSoft(Request request, BlockEntry entry, Throwable error) {
		if(closed) {
			if(entry != null)
				cache.unpin(entry, allowance);
			inFlightRequests.decrementAndGet();
			return;
		}
		request.entry = entry;
		request.error = error;
		readyRequests.offer(request);
		if(closed && readyRequests.remove(request)) {
			if(entry != null)
				cache.unpin(entry, allowance);
			inFlightRequests.decrementAndGet();
		}
	}

	private BlockEntry awaitEntry(Request request) throws InterruptedException {
		try {
			BlockEntry entry = request.future.get();
			if(entry == null) {
				request.future = StorePinAdmission.pinAdmitted(cache, streamId, request.index, allowance,
					() -> closed);
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
