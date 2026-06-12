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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

/**
 * Consumes a live {@code OOCStream} into a {@link MaterializedStore}. This is the publication half of
 * the {@code CachingStream} replacement: a planner-supplied linearization maps each tile's
 * {@code MatrixIndexes} to the store's dense int index (removing the runtime hash index), managed
 * callbacks transfer their detached reservation exclusively through
 * {@code InMemoryQueueCallback.extractManagedPayload()}, non-managed callbacks are measured and
 * reserved on the sink allowance, and EOS completes the store (validating holes/duplicates).
 *
 * With registered live consumers the sink is the tee replacement: each tile is published pinned ONCE
 * and consumers receive aliases of one lease-backed callback ({@link PinnedLeaseCallback}); the last
 * alias close unpins the canonical entry (ownership to cache, possibly deferred). While an alias is
 * open, {@link PinnedLeaseCallback#pinnedEntry()} exposes the pinned canonical entry so a consumer can
 * park a logical reference to it (e.g. {@code OperatorStateTable.installReference}). Retained live
 * aliases are the live-side backpressure token: the producer allowance stays charged until the last
 * alias closes.
 */
public final class MaterializationSink implements Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> {
	private final MaterializedStore<IndexedMatrixValue> _store;
	private final ToIntFunction<MatrixIndexes> _linearize;
	private final MemoryAllowance _allowance;
	private final List<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> _liveConsumers;
	private final OOCFuture<Void> _completion;
	private final AtomicBoolean _done;

	public MaterializationSink(MaterializedStore<IndexedMatrixValue> store,
		ToIntFunction<MatrixIndexes> linearize, MemoryAllowance allowance) {
		this(store, linearize, allowance, List.of());
	}

	public MaterializationSink(MaterializedStore<IndexedMatrixValue> store,
		ToIntFunction<MatrixIndexes> linearize, MemoryAllowance allowance,
		List<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> liveConsumers) {
		_store = store;
		_linearize = linearize;
		_allowance = allowance;
		_liveConsumers = List.copyOf(liveConsumers);
		_completion = new OOCFuture<>();
		_done = new AtomicBoolean(false);
	}

	public void attach(OOCStream<IndexedMatrixValue> source) {
		source.setSubscriber(this);
	}

	/**
	 * Completes with null after EOS completed the store, or exceptionally on a source failure or
	 * publication error (the store is then left incomplete).
	 */
	public OOCFuture<Void> completion() {
		return _completion;
	}

	@Override
	public void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(_done.get()) {
			callback.close();
			return;
		}
		try {
			if(callback.isFailure()) {
				DMLRuntimeException failure;
				try {
					callback.get();
					failure = new DMLRuntimeException("Source stream failed without cause");
				}
				catch(DMLRuntimeException ex) {
					failure = ex;
				}
				callback.close();
				fail(failure);
				return;
			}
			if(callback.isEos()) {
				callback.close();
				finish();
				return;
			}
			publish(callback);
		}
		catch(RuntimeException ex) {
			fail(DMLRuntimeException.of(ex));
		}
	}

	private void publish(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		IndexedMatrixValue value = callback.get();
		int index = _linearize.applyAsInt(value.getIndexes());
		if(_liveConsumers.isEmpty()) {
			if(callback instanceof InMemoryQueueCallback managed) {
				_store.publishPinned(index, managed.extractManagedPayload());
				managed.close();
			}
			else {
				long bytes = measure(value);
				_allowance.reserveBlocking(bytes);
				try {
					_store.publishPinned(index, value, bytes, _allowance);
				}
				catch(RuntimeException ex) {
					_allowance.release(bytes);
					throw ex;
				}
				callback.close();
			}
			return;
		}

		MaterializedStore.LiveLease<IndexedMatrixValue> lease;
		if(callback instanceof InMemoryQueueCallback managed) {
			lease = _store.publishPinnedLive(index, managed.extractManagedPayload());
			managed.close();
		}
		else {
			long bytes = measure(value);
			_allowance.reserveBlocking(bytes);
			try {
				lease = _store.publishPinnedLive(index, value, bytes, _allowance);
			}
			catch(RuntimeException ex) {
				_allowance.release(bytes);
				throw ex;
			}
			callback.close();
		}
		try {
			for(int i = 0; i < _liveConsumers.size(); i++) {
				try(OOCStream.QueueCallback<IndexedMatrixValue> alias = new PinnedLeaseCallback(lease.retain())) {
					_liveConsumers.get(i).accept(alias);
				}
			}
		}
		finally {
			lease.close();
		}
	}

	private void finish() {
		if(!_done.compareAndSet(false, true))
			return;
		try {
			_store.complete();
		}
		catch(RuntimeException ex) {
			deliverEos(DMLRuntimeException.of(ex));
			_completion.completeExceptionally(ex);
			return;
		}
		deliverEos(null);
		_completion.complete(null);
	}

	private void fail(DMLRuntimeException failure) {
		if(!_done.compareAndSet(false, true))
			return;
		deliverEos(failure);
		_completion.completeExceptionally(failure);
	}

	private void deliverEos(DMLRuntimeException failure) {
		for(int i = 0; i < _liveConsumers.size(); i++) {
			try {
				_liveConsumers.get(i).accept(OOCStream.eos(failure));
			}
			catch(RuntimeException ignored) {
				//EOS delivery must reach every consumer
			}
		}
	}

	private static long measure(IndexedMatrixValue value) {
		return ((MatrixBlock)value.getValue()).getExactSerializedSize();
	}

	/**
	 * Lease-backed callback for production-time fan-out. {@code get()} reads the published value,
	 * {@code keepOpen()} retains the shared live lease, and the last close across all aliases unpins
	 * the canonical entry. The deliverer closes the delivered alias after the consumer returns, so
	 * consumers retain tiles by calling {@code keepOpen()} — the standard subscriber contract.
	 */
	public static final class PinnedLeaseCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final MaterializedStore.LiveLease<IndexedMatrixValue> _lease;
		private DMLRuntimeException _failure;
		private boolean _closed;

		private PinnedLeaseCallback(MaterializedStore.LiveLease<IndexedMatrixValue> lease) {
			_lease = lease;
		}

		/**
		 * The still-pinned canonical cache entry, for parking logical references while live.
		 */
		public BlockEntry pinnedEntry() {
			return _lease.entry();
		}

		@Override
		public IndexedMatrixValue get() {
			if(_failure != null)
				throw _failure;
			return _lease.value();
		}

		@Override
		public synchronized OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
			if(_closed)
				throw new IllegalStateException("Cannot keep open a closed callback");
			return new PinnedLeaseCallback(_lease.retain());
		}

		@Override
		public synchronized void close() {
			if(_closed)
				return;
			_closed = true;
			_lease.close();
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_failure = failure;
		}

		@Override
		public boolean isEos() {
			return false;
		}

		@Override
		public boolean isFailure() {
			return _failure != null;
		}

		@Override
		public synchronized BlockKey getBlockKey() {
			return _closed ? null : _lease.entry().getKey();
		}
	}
}
