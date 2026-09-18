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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.primitives.MaterializeOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;

import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;

public final class MaterializedStoreStreamable implements OOCStreamable<IndexedMatrixValue> {
	private final int _replayPrefetch;
	private final long _replayMemory;
	private final OOCStoreLayout _layout;
	private final MaterializeOOCPrimitive _primitive;
	private final OOCFuture<DataCharacteristics> _dimensions;
	private final IntArrayList _publications = new IntArrayList();
	private final List<DeferredReader> _liveReaders = new ArrayList<>();
	private final List<InputView> _views = new CopyOnWriteArrayList<>();
	private boolean _viewPolicyInstalled;
	private MaterializedStore<IndexedMatrixValue> _store;
	private DeferredReader _replayDriver;
	private CacheableData<?> _data;
	private boolean _publicationDone;
	private DMLRuntimeException _publicationFailure;
	private boolean _deleteScheduled;
	private boolean _materializationDone;
	private boolean _sealingReaders;
	private boolean _closed;
	private int _reservedReaders;
	private int _pendingReaders;
	private int _activeReaders;

	public MaterializedStoreStreamable(OOCStream<IndexedMatrixValue> source, CacheableData<?> data) {
		this(source, data, OOCStoreLayout.ROW_MAJOR);
	}

	public MaterializedStoreStreamable(OOCStream<IndexedMatrixValue> source, CacheableData<?> data,
		OOCStoreLayout layout) {
		if(source == null)
			throw new IllegalArgumentException("Materialized stream requires a source.");
		DMLConfig conf = ConfigurationManager.getDMLConfig();
		_replayPrefetch = Math.max(1, conf.getIntValue(DMLConfig.OOC_REPLAY_PREFETCH));
		_replayMemory = Math.min(conf.getLongValue(DMLConfig.OOC_REPLAY_MEMORY),
			GlobalMemoryBroker.getSource().getAllowedMemory() / 2);
		_layout = layout;
		_data = data;
		_dimensions = new OOCFuture<>();
		_primitive = MaterializeOOCPrimitive.reusable(source, layout);
		_primitive.setPublicationListener(this::acceptPublication);
		_primitive.store().whenComplete((store, error) -> {
			if(error != null) {
				_dimensions.completeExceptionally(error);
				markMaterializationDone();
				return;
			}
			synchronized(this) {
				_store = store;
			}
			store.dimensions().whenComplete(this::resolveDimensions);
			store.completion().whenComplete((ignored, completionError) -> markMaterializationDone());
			tryFinalize();
		});
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReadStream() {
		return createReader(false, OOCAccessPattern.ANY, false);
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReservedReadStream() {
		return createReader(true, OOCAccessPattern.ANY, false);
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReservedReadStream(OOCAccessPattern pattern, boolean streaming) {
		return createReader(true, pattern, streaming);
	}

	private synchronized OOCStream<IndexedMatrixValue> createReader(boolean reserved, OOCAccessPattern pattern,
		boolean streaming) {
		if(reserved && _reservedReaders > 0)
			_reservedReaders--;
		else if(_deleteScheduled)
			throw new DMLRuntimeException("Cannot open a reader on a materialized stream scheduled for deletion.");
		_pendingReaders++;
		_primitive.registerRequest(1, null);
		DeferredReader stream = new DeferredReader(pattern, streaming);
		stream.setData(_data);
		stream.assignPrimitive(_primitive);
		return stream;
	}

	private void acceptPublication(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback.isEos() || callback.isFailure()) {
			DMLRuntimeException failure = null;
			if(callback.isFailure()) {
				try {
					callback.get();
				}
				catch(Throwable error) {
					failure = DMLRuntimeException.of(error);
				}
			}
			List<DeferredReader> readers;
			synchronized(this) {
				_publicationDone = true;
				_publicationFailure = failure;
				readers = List.copyOf(_liveReaders);
				_liveReaders.clear();
			}
			for(DeferredReader reader : readers)
				reader.finishLive(failure);
			for(InputView view : _views)
				view.finishPublications(failure);
			return;
		}
		int index = ((MaterializedCallback<?>) callback).publishedIndex();
		List<DeferredReader> readers;
		List<InputView> views;
		synchronized(this) {
			_publications.add(index);
			readers = List.copyOf(_liveReaders);
			readers.forEach(reader -> reader._pendingLive.incrementAndGet());
			views = List.copyOf(_views);
			views.forEach(view -> view._pending.incrementAndGet());
		}
		for(DeferredReader reader : readers)
			reader.acceptLive(callback);
		for(InputView view : views)
			view.publish(index);
	}

	public synchronized InputView getReservedInputView() {
		if(_reservedReaders > 0)
			_reservedReaders--;
		else if(_deleteScheduled)
			throw new DMLRuntimeException("Cannot open a view on a deleted materialized stream.");
		_pendingReaders++;
		_primitive.registerRequest(1, null);
		return new InputView();
	}

	public final class InputView implements AutoCloseable, MaterializedStore.Liveness {
		private final BitSet _consumed = new BitSet();
		private final AtomicInteger _pending = new AtomicInteger(1);
		private final AtomicBoolean _finished = new AtomicBoolean();
		private IndexedMaterializedStoreReader<IndexedMatrixValue> _reader;
		private IntConsumer _publication;
		private Consumer<Throwable> _completion;
		private volatile boolean _liveDone;
		private volatile boolean _closed;
		private boolean _active;
		private Throwable _error;

		public void start(IntConsumer publication, Consumer<Throwable> completion) {
			_publication = publication;
			_completion = completion;
			_primitive.store().whenComplete((store, error) -> {
				if(error != null) {
					finishPublications(error);
					releasePublication();
					return;
				}
				try {
					int[] prefix;
					boolean installPolicy;
					synchronized(MaterializedStoreStreamable.this) {
						if(_closed)
							return;
						_reader = store.openLiveIndexedReader(this);
						_pendingReaders--;
						_activeReaders++;
						_active = true;
						_views.add(this);
						prefix = _publications.toIntArray();
						_liveDone = _publicationDone;
						_error = _publicationFailure;
						installPolicy = !_viewPolicyInstalled;
						_viewPolicyInstalled = true;
					}
					if(installPolicy) {
						DataCharacteristics dc = getDataCharacteristics();
						OOCStoreLayout layout = _layout;
						List<InputView> views = _views;
						long tiles = dc.getNumRowBlocks() * dc.getNumColBlocks();
						store.addEvictionPolicy((row, col) -> {
							int slot = layout.linearize(row, col, dc);
							for(InputView view : views)
								if(view.needs(slot))
									return slot - tiles;
							return slot;
						});
					}
					for(int index : prefix)
						_publication.accept(index);
				}
				catch(Throwable failure) {
					finishPublications(failure);
				}
				finally {
					releasePublication();
					tryFinalize();
				}
			});
			_primitive.startOnDemand();
		}

		public MatrixIndexes indexes(int index) {
			return _layout.delinearize(index, getDataCharacteristics());
		}

		public OOCFuture<StoreLease<IndexedMatrixValue>> acquire(long row, long col, MemoryAllowance allowance) {
			return _store.requestPublished(row, col, allowance);
		}

		public StoreLease<IndexedMatrixValue> tryAcquireResident(long row, long col, MemoryAllowance allowance) {
			return _store.requestPublishedIfResident(_layout.linearize(row, col, getDataCharacteristics()), allowance);
		}

		public void clear(long row, long col) {
			_reader.consumed(_layout.linearize(row, col, getDataCharacteristics()));
		}

		@Override
		public synchronized boolean needs(int index) {
			return !_closed && !_consumed.get(index);
		}

		@Override
		public synchronized void consumed(int index) {
			_consumed.set(index);
		}

		private void publish(int index) {
			try {
				if(!_closed)
					_publication.accept(index);
			}
			catch(Throwable error) {
				finishPublications(error);
			}
			finally {
				releasePublication();
			}
		}

		private void finishPublications(Throwable error) {
			_error = error;
			_liveDone = true;
			if(_pending.get() == 0 && _finished.compareAndSet(false, true))
				_completion.accept(_error);
		}

		private void releasePublication() {
			if(_pending.decrementAndGet() == 0 && _liveDone && _finished.compareAndSet(false, true))
				_completion.accept(_error);
		}

		@Override
		public void close() {
			synchronized(MaterializedStoreStreamable.this) {
				if(_closed)
					return;
				_closed = true;
				_views.remove(this);
				if(_active)
					_activeReaders--;
				else
					_pendingReaders--;
			}
			if(_reader != null)
				_reader.close();
			_primitive.store().whenComplete((store, error) -> {
				if(store != null)
					store.close();
			});
			tryFinalize();
		}
	}

	private void openStreamingReader(DeferredReader reader) {
		_primitive.store().whenComplete((store, error) -> {
			if(error != null) {
				reader.fail(error);
				return;
			}
			DMLRuntimeException failure;
			synchronized(this) {
				reader._livenessReader = store.openLiveIndexedReader(reader);
				_pendingReaders--;
				_activeReaders++;
				reader._active = true;
				failure = _publicationFailure;
				if(_publicationDone && _replayDriver != null && !_replayDriver._finished.get()
					&& reader._pattern.fused(_replayDriver._pattern).isPlannable()) {
					reader._replayLog = _replayDriver._delivered;
					reader._replayEnd = reader._replayLog.size();
					_replayDriver._followers.add(reader);
				}
				else {
					reader._replayLog = _publications;
					reader._replayEnd = _publications.size();
					reader._liveDone = _publicationDone;
					if(!_publicationDone)
						_liveReaders.add(reader);
					else {
						reader._driver = true;
						if(_replayDriver == null)
							_replayDriver = reader;
					}
				}
			}
			tryFinalize();
			if(failure != null)
				reader.fail(failure);
			else
				reader.pumpReplay();
		});
	}

	private void forwardReplay(DeferredReader driver, OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		List<DeferredReader> followers;
		synchronized(this) {
			driver._delivered.add(((MaterializedCallback<?>) callback).publishedIndex());
			followers = List.copyOf(driver._followers);
			followers.forEach(reader -> reader._pendingLive.incrementAndGet());
		}
		try {
			driver.enqueueShared(callback);
		}
		finally {
			for(DeferredReader reader : followers)
				reader.acceptLive(callback);
		}
	}

	private void openReader(DeferredReader output) {
		_primitive.store().whenComplete((store, storeError) -> {
			if(storeError != null) {
				output.fail(storeError);
				return;
			}
			store.completion().whenComplete((ignored, completionError) -> {
				if(completionError != null) {
					output.fail(completionError);
					return;
				}
				try {
					output._allowance = new SyncMemoryAllowance(GlobalMemoryBroker.getSource(), _replayMemory);
					OrderedMaterializedStoreReader<IndexedMatrixValue> reader = store.openReader(
						new SequentialAccessPattern(store.size()), output._allowance, _replayPrefetch);
					synchronized(this) {
						_pendingReaders--;
						_activeReaders++;
						output._active = true;
					}
					tryFinalize();
					StoreBackedStream<IndexedMatrixValue> replay = new StoreBackedStream<>(reader);
					replay.setData(_data);
					replay.setSubscriber(callback -> {
						try(callback) {
							if(callback.isFailure())
								callback.get();
							if(callback.isEos())
								output.complete();
							else
								output.enqueueShared(callback);
						}
						catch(Throwable failure) {
							reader.close();
							output.fail(failure);
						}
					});
				}
				catch(Throwable failure) {
					output.fail(failure);
				}
			});
		});
	}

	private void finishReader(DeferredReader reader, DMLRuntimeException failure) {
		List<DeferredReader> followers;
		synchronized(this) {
			if(reader._active)
				_activeReaders--;
			else
				_pendingReaders--;
			_liveReaders.remove(reader);
			if(_replayDriver == reader)
				_replayDriver = null;
			followers = List.copyOf(reader._followers);
			reader._followers.clear();
		}
		for(DeferredReader follower : followers)
			follower.finishLive(failure);
		if(reader._livenessReader != null)
			reader._livenessReader.close();
		if(reader._allowance != null)
			reader._allowance.shutdown();
		_primitive.store().whenComplete((store, error) -> {
			if(store != null)
				store.close();
		});
		tryFinalize();
	}

	private void markMaterializationDone() {
		synchronized(this) {
			_materializationDone = true;
		}
		tryFinalize();
	}

	@Override
	public synchronized void reserveLazyHandle() {
		if(_closed || (_deleteScheduled && _reservedReaders == 0))
			throw new DMLRuntimeException("Cannot reserve a reader on a closed materialized stream.");
		_reservedReaders++;
	}

	@Override
	public void discardHandle() {
		synchronized(this) {
			if(_reservedReaders <= 0)
				return;
			_reservedReaders--;
		}
		tryFinalize();
	}

	@Override
	public void scheduleMaterializedStoreDeletion() {
		synchronized(this) {
			_deleteScheduled = true;
		}
		tryFinalize();
	}

	private void tryFinalize() {
		MaterializedStore<IndexedMatrixValue> store;
		boolean seal = false;
		boolean close = false;
		synchronized(this) {
			store = _store;
			if(!_deleteScheduled || _reservedReaders != 0 || _pendingReaders != 0 || _closed || store == null)
				return;
			if(!store.readersSealed().isDone()) {
				if(_sealingReaders)
					return;
				_sealingReaders = true;
				seal = true;
			}
			else if(_materializationDone && _activeReaders == 0) {
				_closed = true;
				close = true;
			}
		}
		if(seal) {
			store.readersSealed().whenComplete((ignored, error) -> tryFinalize());
			store.sealReaders();
		}
		else if(close)
			store.close();
	}

	@Override
	public boolean hasMaterializedStore() {
		return true;
	}

	@Override
	public OOCStream<IndexedMatrixValue> getWriteStream() {
		throw new UnsupportedOperationException("Materialized streams are read-only.");
	}

	@Override
	public boolean hasStreamCache() {
		return false;
	}

	@Override
	public CachingStream getStreamCache() {
		return null;
	}

	@Override
	public boolean isProcessed() {
		return false;
	}

	@Override
	public synchronized DataCharacteristics getDataCharacteristics() {
		return _data == null ? null : _data.getDataCharacteristics();
	}

	@Override
	public OOCFuture<DataCharacteristics> dimensions() {
		DataCharacteristics known = getDataCharacteristics();
		if(known != null && known.dimsKnown() && known.getBlocksize() > 0)
			return OOCFuture.completed(known);
		return _dimensions;
	}

	private void resolveDimensions(DataCharacteristics observed, Throwable error) {
		if(error != null) {
			_dimensions.completeExceptionally(error);
			return;
		}
		synchronized(this) {
			DataCharacteristics current = _data == null ? null : _data.getDataCharacteristics();
			if(current != null && !current.dimsKnown())
				current.set(observed.getRows(), observed.getCols(), observed.getBlocksize(), observed.getNonZeros());
		}
		_dimensions.complete(observed);
	}

	@Override
	public synchronized CacheableData<?> getData() {
		return _data;
	}

	@Override
	public synchronized void setData(CacheableData<?> data) {
		_data = data;
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return _primitive;
	}

	private final class DeferredReader extends SubscribableTaskQueue<IndexedMatrixValue>
		implements MaterializedStore.Liveness {
		private final OOCAccessPattern _pattern;
		private final boolean _streaming;
		private final AtomicBoolean _activated = new AtomicBoolean();
		private final AtomicBoolean _finished = new AtomicBoolean();
		private final AtomicBoolean _pumping = new AtomicBoolean();
		private final AtomicInteger _pendingReplay = new AtomicInteger();
		private final AtomicInteger _pendingLive = new AtomicInteger();
		private final IntArrayList _delivered = new IntArrayList();
		private final List<DeferredReader> _followers = new ArrayList<>();
		private final BitSet _consumed = new BitSet();
		private IndexedMaterializedStoreReader<IndexedMatrixValue> _livenessReader;
		private IntArrayList _replayLog;
		private int _replayPosition;
		private int _replayEnd;
		private boolean _active;
		private boolean _driver;
		private volatile boolean _liveDone;
		private SyncMemoryAllowance _allowance;

		private DeferredReader(OOCAccessPattern pattern, boolean streaming) {
			CacheableData<?> data = MaterializedStoreStreamable.this._data;
			DataCharacteristics dc = data == null ? null : data.getDataCharacteristics();
			boolean singleBand = dc != null && dc.dimsKnown()
				&& (dc.getNumRowBlocks() == 1 || dc.getNumColBlocks() == 1);
			_pattern = singleBand && (pattern == OOCAccessPattern.ROW_MAJOR || pattern == OOCAccessPattern.COL_MAJOR)
				? OOCAccessPattern.ANY : pattern;
			_streaming = streaming && (_pattern == OOCAccessPattern.ANY
				|| _pattern == (_layout == OOCStoreLayout.ROW_MAJOR ? OOCAccessPattern.ROW_MAJOR
					: OOCAccessPattern.COL_MAJOR));
		}

		@Override
		public synchronized boolean needs(int index) {
			return index >= 0 && !_consumed.get(index);
		}

		@Override
		public synchronized void consumed(int index) {
			_consumed.set(index);
		}

		@Override
		public void setSubscriber(Consumer<QueueCallback<IndexedMatrixValue>> subscriber) {
			super.setSubscriber(subscriber);
			activate();
		}

		@Override
		public IndexedMatrixValue dequeue() {
			activate();
			return super.dequeue();
		}

		@Override
		public QueueCallback<IndexedMatrixValue> dequeueCB() {
			activate();
			return super.dequeueCB();
		}

		private void activate() {
			if(!_activated.compareAndSet(false, true))
				return;
			if(_streaming)
				openStreamingReader(this);
			else
				openReader(this);
			_primitive.startOnDemand();
		}

		private void acceptLive(QueueCallback<IndexedMatrixValue> callback) {
			try {
				if(!_finished.get())
					enqueueShared(callback);
			}
			catch(Throwable error) {
				fail(error);
			}
			finally {
				_pendingLive.decrementAndGet();
				pumpReplay();
			}
		}

		private void enqueueShared(QueueCallback<IndexedMatrixValue> callback) {
			QueueCallback<IndexedMatrixValue> retained = callback.keepOpen();
			try {
				enqueue(retained);
			}
			catch(Throwable error) {
				retained.close();
				throw error;
			}
			if(_livenessReader != null)
				_livenessReader.consumed(((MaterializedCallback<?>) callback).publishedIndex());
		}

		private void finishLive(DMLRuntimeException failure) {
			if(failure != null)
				fail(failure);
			else {
				_liveDone = true;
				pumpReplay();
			}
		}

		private void pumpReplay() {
			do {
				if(!_pumping.compareAndSet(false, true))
					return;
				try {
					if(_allowance == null && _replayPosition < _replayEnd)
						_allowance = new SyncMemoryAllowance(GlobalMemoryBroker.getSource(), _replayMemory);
					while(!_finished.get() && _pendingReplay.get() < _replayPrefetch && _replayPosition < _replayEnd) {
						int index;
						synchronized(MaterializedStoreStreamable.this) {
							index = _replayLog.getInt(_replayPosition++);
						}
						_pendingReplay.incrementAndGet();
						try {
							_store.requestPublished(index, _allowance).whenComplete((lease, error) -> {
								try {
									if(error != null)
										throw DMLRuntimeException.of(error);
									try(QueueCallback<IndexedMatrixValue> callback = new MaterializedCallback<>(lease, index, _store)) {
										if(!_finished.get()) {
											if(_driver)
												forwardReplay(this, callback);
											else
												enqueueShared(callback);
										}
									}
								}
								catch(Throwable failure) {
									fail(failure);
								}
								finally {
									_pendingReplay.decrementAndGet();
									pumpReplay();
								}
							});
						}
						catch(Throwable failure) {
							_pendingReplay.decrementAndGet();
							fail(failure);
						}
					}
					if(!_finished.get() && _liveDone && _replayPosition == _replayEnd
						&& _pendingReplay.get() == 0 && _pendingLive.get() == 0)
						complete();
				}
				finally {
					_pumping.set(false);
				}
			}
			while(!_finished.get() && (_pendingReplay.get() < _replayPrefetch && _replayPosition < _replayEnd
				|| _liveDone && _pendingReplay.get() == 0 && _pendingLive.get() == 0));
		}

		private void complete() {
			if(_finished.compareAndSet(false, true)) {
				try {
					closeInput();
				}
				finally {
					finishReader(this, null);
				}
			}
		}

		private void fail(Throwable error) {
			if(_finished.compareAndSet(false, true)) {
				DMLRuntimeException failure = DMLRuntimeException.of(error);
				try {
					propagateFailure(failure);
				}
				finally {
					finishReader(this, failure);
				}
			}
		}
	}
}
