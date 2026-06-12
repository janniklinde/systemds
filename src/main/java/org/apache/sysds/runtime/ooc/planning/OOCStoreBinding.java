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

package org.apache.sysds.runtime.ooc.planning;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.store.MaterializationSink;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreImpl;

/**
 * Planner-owned coordination handle for one materialized boundary: the store, its publication sink,
 * counted reader registration, and the consumer refcount. The planner knows the consumer set of a
 * boundary, so it declares up front how many reader registrations to expect; registration goes
 * through this binding and {@code sealReaders()} fires exactly when the declared count has
 * registered — consumers never need a global barrier or knowledge of each other. The last
 * {@link #release()} closes the store (dropping whatever forgetting has not reclaimed yet).
 *
 * Consumers must await {@link #completion()} before opening readers (readers require a completed
 * store) and must not bypass the binding via {@link #store()} for registration.
 */
public final class OOCStoreBinding {
	private final MaterializedStore<IndexedMatrixValue> _store;
	private final MaterializationSink _sink;
	private final AtomicInteger _pendingReaders;
	private final AtomicInteger _refCtr;

	public OOCStoreBinding(OOCCache cache, long streamId, ToIntFunction<MatrixIndexes> linearize,
		MemoryAllowance sinkAllowance, int expectedReaders, int consumers) {
		this(cache, streamId, linearize, sinkAllowance, expectedReaders, consumers, List.of());
	}

	public OOCStoreBinding(OOCCache cache, long streamId, ToIntFunction<MatrixIndexes> linearize,
		MemoryAllowance sinkAllowance, int expectedReaders, int consumers,
		List<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> liveConsumers) {
		if(expectedReaders < 0 || consumers <= 0)
			throw new IllegalArgumentException("Invalid binding counts: readers=" + expectedReaders
				+ ", consumers=" + consumers);
		_store = new MaterializedStoreImpl<>(cache, streamId);
		_sink = new MaterializationSink(_store, linearize, sinkAllowance, liveConsumers);
		_pendingReaders = new AtomicInteger(expectedReaders);
		_refCtr = new AtomicInteger(consumers);
	}

	public void attach(OOCStream<IndexedMatrixValue> source) {
		_sink.attach(source);
	}

	public MaterializationSink sink() {
		return _sink;
	}

	/**
	 * Completes when the sink completed the store (readers may be opened), or exceptionally on a
	 * source failure (the store stays incomplete and consumers must release without reading).
	 */
	public OOCFuture<Void> completion() {
		return _sink.completion();
	}

	public MaterializedStore<IndexedMatrixValue> store() {
		return _store;
	}

	public MaterializedStore.Reader<IndexedMatrixValue> openReader(MaterializedStore.AccessPattern pattern,
		MemoryAllowance allowance, int maxPrefetch) {
		MaterializedStore.Reader<IndexedMatrixValue> reader = _store.openReader(pattern, allowance, maxPrefetch);
		sealIfLastRegistration();
		return reader;
	}

	public MaterializedStore.PackReader<IndexedMatrixValue> openOpportunisticReader(
		MaterializedStore.AccessPattern pattern, MemoryAllowance allowance, int maxPrefetch) {
		MaterializedStore.PackReader<IndexedMatrixValue> reader =
			_store.openOpportunisticReader(pattern, allowance, maxPrefetch);
		sealIfLastRegistration();
		return reader;
	}

	public MaterializedStore.IndexedReader<IndexedMatrixValue> openIndexedReader(
		MaterializedStore.Liveness liveness, MemoryAllowance allowance) {
		MaterializedStore.IndexedReader<IndexedMatrixValue> reader = _store.openIndexedReader(liveness, allowance);
		sealIfLastRegistration();
		return reader;
	}

	/**
	 * Releases one consumer; the last release closes the store.
	 */
	public void release() {
		int remaining = _refCtr.decrementAndGet();
		if(remaining < 0)
			throw new IllegalStateException("Store binding released more often than declared consumers.");
		if(remaining == 0)
			_store.close();
	}

	private void sealIfLastRegistration() {
		int remaining = _pendingReaders.decrementAndGet();
		if(remaining < 0)
			throw new IllegalStateException("More reader registrations than declared for this binding.");
		if(remaining == 0)
			_store.sealReaders();
	}
}
