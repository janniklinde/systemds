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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.store.OOCStreamMaterializer;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.OOCMaterializedView;

/**
 * Planner-owned coordination handle for one materialized input: the source, store, publication
 * sink, counted reader registration, and the consumer refcount. The planner knows the consumer set
 * of a boundary, so it declares up front how many reader registrations to expect; registration goes
 * through this binding and {@code sealReaders()} fires exactly when the declared count has
 * registered — consumers never need a global barrier or knowledge of each other. The last
 * {@link #close()} closes the store (dropping whatever forgetting has not reclaimed yet).
 *
 * One binding is shared by ALL consumers of a materialized input. Attachment is planner-owned and
 * first-wins so the source is materialized exactly once. Late-planned consumers can still join via
 * {@link #tryRegister} as long as the reader set has not sealed (a consumer joining after sealing is
 * a planning error — the store may already have started forgetting). Consumers must await
 * {@link #completion()} before opening readers (readers require a completed store).
 */
public final class OOCStoreBinding implements OOCMaterializedView {
	private final OOCStreamable<IndexedMatrixValue> _source;
	private final OOCCache _cache;
	private final long _streamId;
	private final OOCStoreLayout<MatrixIndexes> _layout;
	private final MaterializedStore<IndexedMatrixValue> _store;
	private final OOCStreamMaterializer _sink;
	private final AtomicBoolean _attached;
	private final OOCFuture<Void> _readersSealed;
	private int _pendingReaders;
	private int _refCtr;
	private boolean _sealed;

	public OOCStoreBinding(OOCStreamable<IndexedMatrixValue> source, OOCCache cache, long streamId,
		ToIntFunction<MatrixIndexes> linearize, MemoryAllowance sinkAllowance, int expectedReaders,
		int consumers) {
		this(source, cache, streamId, OOCStoreLayout.of(linearize, index -> new MatrixIndexes(index + 1L, 1)),
			sinkAllowance, expectedReaders, consumers);
	}

	public OOCStoreBinding(OOCStreamable<IndexedMatrixValue> source, OOCCache cache, long streamId,
		OOCStoreLayout<MatrixIndexes> layout, MemoryAllowance sinkAllowance, int expectedReaders,
		int consumers) {
		this(source, cache, streamId, layout, sinkAllowance, expectedReaders, consumers, List.of(), List.of());
	}

	public OOCStoreBinding(OOCStreamable<IndexedMatrixValue> source, OOCCache cache, long streamId,
		OOCStoreLayout<MatrixIndexes> layout, MemoryAllowance sinkAllowance, int expectedReaders, int consumers,
		List<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> liveConsumers) {
		this(source, cache, streamId, layout, sinkAllowance, expectedReaders, consumers, liveConsumers, List.of());
	}

	public OOCStoreBinding(OOCStreamable<IndexedMatrixValue> source, OOCCache cache, long streamId,
		OOCStoreLayout<MatrixIndexes> layout, MemoryAllowance sinkAllowance, int expectedReaders, int consumers,
		List<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> liveConsumers,
		List<ToLongFunction<MatrixIndexes>> evictionPolicies) {
		if(expectedReaders < 0 || consumers <= 0)
			throw new IllegalArgumentException("Invalid binding counts: readers=" + expectedReaders
				+ ", consumers=" + consumers);
		if(layout == null)
			throw new IllegalArgumentException("Store binding requires a layout.");
		_source = source;
		_cache = cache;
		_streamId = streamId;
		_layout = layout;
		_store = new MaterializedStore<>(cache, streamId);
		_sink = new OOCStreamMaterializer(_store, layout, sinkAllowance, liveConsumers);
		_attached = new AtomicBoolean(false);
		_readersSealed = new OOCFuture<>();
		_pendingReaders = expectedReaders;
		_refCtr = consumers;
		for(ToLongFunction<MatrixIndexes> evictionPolicy : evictionPolicies)
			addEvictionPolicy(evictionPolicy);
	}

	/**
	 * Planner-owned source attachment. First-wins to tolerate shared bindings appearing in multiple
	 * regions, but primitives never call this directly.
	 */
	void attachMaterializedInput() {
		if(_source != null && _attached.compareAndSet(false, true))
			_sink.attach(_source.getReadStream());
	}

	/**
	 * Joins a late-planned consumer onto this boundary: grows the declared reader and consumer
	 * counts. Succeeds only while the reader set has not sealed and the binding has not been fully
	 * released; afterwards the boundary's data may already be partially reclaimed, so joining is a
	 * planning error and the caller must fail.
	 */
	public synchronized boolean tryRegister(int expectedReaders, int consumers) {
		if(expectedReaders < 0 || consumers <= 0)
			throw new IllegalArgumentException("Invalid registration counts: readers=" + expectedReaders
				+ ", consumers=" + consumers);
		if(_sealed || _refCtr <= 0)
			return false;
		_pendingReaders += expectedReaders;
		_refCtr += consumers;
		return true;
	}

	@Override
	public void addEvictionPolicy(ToLongFunction<MatrixIndexes> policy) {
		if(policy == null)
			return;
		_cache.addEvictionPolicy(_streamId,
			index -> policy.applyAsLong(_layout.delinearize(Math.toIntExact(index))));
	}

	/**
	 * Completes when the sink completed the store (readers may be opened), or exceptionally on a
	 * source failure (the store stays incomplete and consumers must release without reading).
	 */
	public OOCFuture<Void> completion() {
		return _sink.completion();
	}

	/**
	 * Completes when the declared reader set has fully registered and the store sealed. Demand-driven
	 * consumers must await this before issuing reads; the store rejects reads while unsealed because
	 * forgetting decisions need the complete reader population.
	 */
	public OOCFuture<Void> readersSealed() {
		return _readersSealed;
	}

	public MaterializedStore<IndexedMatrixValue> store() {
		return _store;
	}

	public MaterializedStore.Reader<IndexedMatrixValue> openReader(MaterializedStore.AccessPattern pattern,
		MemoryAllowance allowance, int maxPrefetch) {
		return openReader(pattern, allowance, maxPrefetch, true);
	}

	public MaterializedStore.Reader<IndexedMatrixValue> openReader(MaterializedStore.AccessPattern pattern,
		MemoryAllowance allowance, int maxPrefetch, boolean softOrdering) {
		MaterializedStore.Reader<IndexedMatrixValue> reader =
			_store.openReader(pattern, allowance, maxPrefetch, softOrdering);
		sealIfLastRegistration();
		return reader;
	}

	public MaterializedStore.IndexedReader<IndexedMatrixValue> openIndexedReader(MaterializedStore.Liveness liveness) {
		MaterializedStore.IndexedReader<IndexedMatrixValue> reader = _store.openIndexedReader(liveness);
		sealIfLastRegistration();
		return reader;
	}

	/**
	 * Releases one consumer; the last release closes the store.
	 */
	@Override
	public void close() {
		boolean close;
		synchronized(this) {
			if(_refCtr <= 0)
				throw new IllegalStateException("Store binding released more often than declared consumers.");
			close = --_refCtr == 0;
		}
		if(close)
			_store.close();
	}

	/**
	 * Whether every declared consumer has released (the store is closed). A released binding cannot
	 * serve new consumers; the planner replaces it with a fresh materialization if the source can be
	 * consumed again.
	 */
	public synchronized boolean isReleased() {
		return _refCtr <= 0;
	}

	private void sealIfLastRegistration() {
		boolean seal;
		synchronized(this) {
			if(_pendingReaders <= 0)
				throw new IllegalStateException("More reader registrations than declared for this binding.");
			seal = --_pendingReaders == 0;
			if(seal)
				_sealed = true;
		}
		if(seal) {
			_store.sealReaders();
			_readersSealed.complete(null);
		}
	}
}
