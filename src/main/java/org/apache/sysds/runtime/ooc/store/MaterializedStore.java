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

import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongBinaryOperator;

public final class MaterializedStore<T extends SpillableObject> {
	private static final byte FORGOTTEN = 0;
	private static final byte ALREADY_FORGOTTEN = 1;
	private static final byte RETRY = 2;

	private final OOCCache _cache;
	private final long _streamId;
	private final ArrayList<StoreReader> _registeredReaders;
	private final BitSet _forgotten;
	private final AtomicInteger _published;
	private final AtomicInteger _publishedCount;
	private final OOCFuture<Void> _completion;
	private final OOCFuture<DataCharacteristics> _dimensions;
	private final OOCFuture<Void> _readersSealedFuture;
	private final boolean _autoSealReaders;
	private final OOCStoreLayout _layout;
	private final DataCharacteristics _characteristics;

	private volatile List<StoreReader> _readers;
	private volatile int _completedSize;
	private volatile boolean _complete;
	private volatile boolean _readersSealed;
	private volatile boolean _closed;
	private volatile int _readerVersion;
	private int _pendingReaders;
	private int _consumers;

	public MaterializedStore(OOCCache cache, long streamId) {
		this(cache, streamId, -1, 1, null, null);
	}

	public MaterializedStore(OOCCache cache, long streamId, int expectedReaders, int consumers) {
		this(cache, streamId, expectedReaders, consumers, null, null);
	}

	public MaterializedStore(OOCCache cache, long streamId, int expectedReaders, int consumers, OOCStoreLayout layout,
		DataCharacteristics characteristics) {
		if(expectedReaders == 0 || expectedReaders < -1)
			throw new IllegalArgumentException("Expected reader count must be positive or disabled.");
		if(consumers <= 0)
			throw new IllegalArgumentException("Materialized store requires at least one consumer.");
		_cache = cache;
		_streamId = streamId;
		_registeredReaders = new ArrayList<>();
		_forgotten = new BitSet();
		_published = new AtomicInteger();
		_publishedCount = new AtomicInteger();
		_completion = new OOCFuture<>();
		_dimensions = new OOCFuture<>();
		if(characteristics != null && characteristics.dimsKnown() && characteristics.getBlocksize() > 0)
			_dimensions.complete(characteristics);
		_readersSealedFuture = new OOCFuture<>();
		_autoSealReaders = expectedReaders > 0;
		_layout = layout;
		_characteristics = characteristics;
		_pendingReaders = expectedReaders;
		_consumers = consumers;
		_readers = Collections.emptyList();
	}

	StoreLease<T> publishPinnedLive(int index, T value, long bytes, MemoryAllowance allowance) {
		BlockEntry entry;
		try {
			if(_complete || _closed)
				throw new IllegalStateException("Store no longer accepts published items");
			if(index < 0 || index == Integer.MAX_VALUE)
				throw new IndexOutOfBoundsException("Invalid index: " + index);
			entry = _cache.putPinned(_streamId, index, value, bytes, allowance);
		}
		catch(RuntimeException ex) {
			if(bytes > 0)
				allowance.release(bytes);
			throw ex;
		}
		return publishPinnedEntryLive(index, entry, allowance);
	}

	public StoreLease<T> publishPinnedEntryLive(int index, BlockEntry entry, MemoryAllowance allowance) {
		if(entry == null || !entry.isPinned())
			throw new IllegalArgumentException("Materialized store requires a pinned cache entry.");
		if(_complete || _closed)
			throw new IllegalStateException("Store no longer accepts published items");
		if(index < 0 || index == Integer.MAX_VALUE)
			throw new IndexOutOfBoundsException("Invalid index: " + index);
		_publishedCount.incrementAndGet();
		updatePublished(index + 1);
		return StoreLease.createAsync(entry, () -> {
			OOCCache.UnpinHandle unpin = _cache.unpin(entry, allowance);
			tryForget(index);
			return unpin.getCompletionFuture();
		});
	}

	StoreLease<T> publishPinnedLive(int index, ManagedPayload<T> payload) {
		payload.transfer();
		return publishPinnedLive(index, payload.value(), payload.bytes(), payload.owner());
	}

	public void complete() {
		complete(null);
	}

	/**
	 * Seals the store. Block arrival is unordered, so dimensions observed during materialization only become final
	 * here; they are resolved before the completion subscribers run so that a subscriber never sees a sealed store
	 * with unresolved dimensions.
	 *
	 * @param observed dimensions measured while publishing, or null if they were not tracked
	 */
	public void complete(DataCharacteristics observed) {
		boolean seal;
		synchronized(this) {
			if(_complete)
				return;
			_completedSize = _published.get();
			if(_publishedCount.get() != _completedSize)
				throw new IllegalStateException("Incomplete publication: " + _publishedCount.get()
					+ " published items for logical range [0, " + _completedSize + ")");
			_complete = true;
			seal = _autoSealReaders && _pendingReaders == 0;
		}
		resolveDimensions(observed);
		// Completion subscribers run inline and may register consumers or open readers on this store,
		// either directly or through an owning streamable. They must never observe the store monitor.
		_completion.complete(null);
		if(seal)
			sealReaders();
	}

	public OOCFuture<DataCharacteristics> dimensions() {
		return _dimensions;
	}

	private void resolveDimensions(DataCharacteristics observed) {
		if(_dimensions.isDone())
			return;
		if(observed != null)
			_dimensions.complete(observed);
		else
			_dimensions.completeExceptionally(new DMLRuntimeException(
				"Materialized store " + _streamId + " completed without observing dimensions."));
	}

	public synchronized void registerConsumer(int expectedReaders) {
		//sealing no longer bars new readers (see registerReader), so it must not bar new consumers either
		if(_closed)
			throw new IllegalStateException("Store no longer accepts consumers");
		_pendingReaders += expectedReaders;
		_consumers++;
	}

	void failMaterialization(Throwable error) {
		if(!_dimensions.isDone())
			_dimensions.completeExceptionally(error);
		_completion.completeExceptionally(error);
	}

	OOCCache cache() {
		return _cache;
	}

	long streamId() {
		return _streamId;
	}

	public OOCFuture<Void> completion() {
		return _completion;
	}

	public OOCFuture<Void> readersSealed() {
		return _readersSealedFuture;
	}

	public void addEvictionPolicy(LongBinaryOperator policy) {
		if(_layout == null)
			throw new IllegalStateException("Materialized store has no logical matrix-index layout.");
		_cache.addEvictionPolicy(_streamId, slot -> {
			MatrixIndexes indexes = _layout.delinearize((int) slot, _characteristics);
			return policy.applyAsLong(indexes.getRowIndex(), indexes.getColumnIndex());
		});
	}

	public OrderedMaterializedStoreReader<T> openReader(AccessPattern pattern, MemoryAllowance allowance,
		int maxPrefetch) {
		return openReader(pattern, allowance, maxPrefetch, true);
	}

	public OrderedMaterializedStoreReader<T> openReader(AccessPattern pattern, MemoryAllowance allowance,
		int maxPrefetch, boolean softOrdering) {
		OrderedMaterializedStoreReader<T> reader;
		boolean seal;
		synchronized(this) {
			if(!_complete || _closed)
				throw new IllegalStateException("Readers require a completed store");
			reader = new OrderedMaterializedStoreReader<>(this, _cache, _streamId, pattern, allowance,
				Math.max(1, maxPrefetch), softOrdering, this::forgetAfterReaderClose, this::tryForget);
			seal = registerReader(reader, pattern);
		}
		if(seal)
			sealReaders();
		return reader;
	}

	public IndexedMaterializedStoreReader<T> openIndexedReader(Liveness liveness) {
		IndexedMaterializedStoreReader<T> reader;
		boolean seal;
		synchronized(this) {
			if(!_complete || _closed)
				throw new IllegalStateException("Readers require a completed store");
			reader = new IndexedMaterializedStoreReader<>(_cache, _streamId, () -> _completedSize, liveness, _layout,
				_characteristics, this::forgetAfterReaderClose, this::tryForget);
			seal = registerReader(reader, liveness);
		}
		if(seal)
			sealReaders();
		return reader;
	}

	public IndexedMaterializedStoreReader<T> openLiveIndexedReader(Liveness liveness) {
		IndexedMaterializedStoreReader<T> reader;
		boolean seal;
		synchronized(this) {
			if(_closed)
				throw new IllegalStateException("Store is closed");
			reader = new IndexedMaterializedStoreReader<>(_cache, _streamId, this::size, liveness, _layout,
				_characteristics, this::forgetAfterReaderClose, this::tryForget);
			seal = registerReader(reader, liveness);
		}
		if(seal)
			sealReaders();
		return reader;
	}

	public OOCFuture<StoreLease<T>> requestPublished(MatrixIndexes indexes, MemoryAllowance allowance) {
		return requestPublished(indexes.getRowIndex(), indexes.getColumnIndex(), allowance);
	}

	public OOCFuture<StoreLease<T>> requestPublished(long row, long col, MemoryAllowance allowance) {
		if(_layout == null)
			throw new IllegalStateException("Materialized store has no logical matrix-index layout.");
		return requestPublished(_layout.linearize(row, col, _characteristics), allowance);
	}

	public StoreLease<T> requestPublishedIfResident(int index, MemoryAllowance allowance) {
		if(_closed)
			return null;
		if(index < 0 || index >= _published.get())
			throw new IndexOutOfBoundsException("Invalid requested index: " + index);
		BlockEntry entry = _cache.pinIfLive(_streamId, index, allowance);
		if(entry == null)
			return null;
		return StoreLease.createAsync(entry, () -> _cache.unpin(entry, allowance).getCompletionFuture());
	}

	public OOCFuture<StoreLease<T>> requestPublished(int index, MemoryAllowance allowance) {
		if(_closed)
			throw new IllegalStateException("Store is closed");
		if(index < 0 || index >= _published.get())
			throw new IndexOutOfBoundsException("Invalid requested index: " + index);
		OOCFuture<BlockEntry> pinned = OOCUtils.pinAdmitted(_cache, _streamId, index, allowance, () -> _closed);
		OOCFuture<StoreLease<T>> result = new OOCFuture<>();
		pinned.whenComplete((entry, error) -> {
			if(error != null)
				result.completeExceptionally(error);
			else if(entry == null)
				result.complete(null);
			else
				result.complete(
					StoreLease.createAsync(entry, () -> _cache.unpin(entry, allowance).getCompletionFuture()));
		});
		return result;
	}

	public void sealReaders() {
		int publishedSize;
		synchronized(this) {
			if(_closed)
				throw new IllegalStateException("Cannot seal readers for a closed store");
			if(_readersSealed)
				return;
			_readers = new ArrayList<>(_registeredReaders);
			_readersSealed = true;
			publishedSize = _complete ? _completedSize : _published.get();
		}
		for(int i = 0; i < publishedSize; i++)
			tryForget(i);
		_readersSealedFuture.complete(null);
	}

	private synchronized boolean registerReader(StoreReader reader, Liveness liveness) {
		if(_readersSealed) {
			int published = size();
			for(int i = 0; i < published; i++)
				if(_forgotten.get(i) && liveness.needs(i))
					throw new IllegalStateException("Store cannot serve a late reader: block " + i +
						" of stream " + _streamId + " was already reclaimed.");
		}
		_registeredReaders.add(reader);
		if(_readersSealed) {
			_readers = new ArrayList<>(_registeredReaders);
			_readerVersion++;
		}
		return readerRegistered();
	}

	private boolean readerRegistered() {
		if(!_autoSealReaders || _readersSealed || _pendingReaders <= 0)
			return false;
		return --_pendingReaders == 0 && _complete;
	}

	public OOCStoreLayout layout() {
		return _layout;
	}

	public DataCharacteristics characteristics() {
		return _characteristics;
	}

	public int linearize(long row, long col) {
		if(_layout == null)
			throw new IllegalStateException("Materialized store has no logical matrix-index layout.");
		return _layout.linearize(row, col, _characteristics);
	}

	public int size() {
		return _complete ? _completedSize : _published.get();
	}

	public void close() {
		List<StoreReader> localReaders;
		synchronized(this) {
			if(_closed)
				return;
			if(--_consumers > 0)
				return;
			_closed = true;
			localReaders = _readersSealed ? _readers : new ArrayList<>(_registeredReaders);
		}
		for(StoreReader localReader : localReaders)
			localReader.close();
		for(int i = 0; i < size(); i++)
			if(markForgotten(i))
				_cache.dereference(new BlockKey(_streamId, i));
	}

	private void tryForget(int index) {
		while(_readersSealed) {
			int version = _readerVersion;
			if(isNeeded(index))
				return;
			byte result = markForgotten(index, version);
			if(result == RETRY)
				continue;
			if(result == FORGOTTEN)
				_cache.dereference(new BlockKey(_streamId, index));
			return;
		}
	}

	private boolean isNeeded(int index) {
		List<StoreReader> localReaders = _readers;
		for(StoreReader reader : localReaders)
			if(!reader.isClosed() && reader.liveness().needs(index))
				return true;
		return false;
	}

	private synchronized byte markForgotten(int index, int expectedVersion) {
		if(_readerVersion != expectedVersion)
			return RETRY;
		return markForgotten(index) ? FORGOTTEN : ALREADY_FORGOTTEN;
	}

	private synchronized boolean markForgotten(int index) {
		if(_forgotten.get(index))
			return false;
		_forgotten.set(index);
		return true;
	}

	private void forgetAfterReaderClose() {
		if(_closed || !_readersSealed)
			return;
		for(int i = 0; i < _completedSize; i++)
			tryForget(i);
	}

	private void updatePublished(int size) {
		int current = _published.get();
		while(current < size && !_published.compareAndSet(current, size))
			current = _published.get();
	}

	public interface Liveness {
		boolean needs(int index);

		void consumed(int index);

		default boolean reserve(int index) {
			return needs(index);
		}

		default void unreserve(int index) {
		}
	}

	public interface AccessPattern extends Liveness {
		boolean hasNext();

		int next();
	}

	public interface StoreReader extends AutoCloseable {
		Liveness liveness();

		boolean isClosed();

		@Override
		void close();
	}
}
