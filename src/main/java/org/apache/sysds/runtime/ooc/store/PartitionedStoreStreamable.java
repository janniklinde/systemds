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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.packed.PackedBlock;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCPlanner;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.SourceReadOOCPrimitive;

public final class PartitionedStoreStreamable implements OOCStreamable<IndexedMatrixValue> {
	private final Materializer _primitive;
	private final OOCFuture<MaterializedStore<PackedBlock>> _partitionStore = new OOCFuture<>();
	private final OOCFuture<MaterializedStore<IndexedMatrixValue>> _tileStore = new OOCFuture<>();
	private final SourceReadOOCPrimitive _source;
	private final long _partitionBytes;
	private CacheableData<?> _data;
	private int _reserved;
	private int _active;
	private boolean _delete;
	private boolean _closed;
	private boolean _materializationDone;
	private boolean _partitionsRequested;
	private boolean _partitioned;
	private MaterializedStoreStreamable _unpartitioned;
	private int _openPartitionReaders;
	private boolean _partitionReadersSealed;
	private volatile Consumer<StoreLease<PackedBlock>> _liveConsumer;

	public PartitionedStoreStreamable(OOCStream<IndexedMatrixValue> source, CacheableData<?> data,
		long partitionBytes) {
		if(!(source.getPrimitive() instanceof SourceReadOOCPrimitive sourcePrimitive))
			throw new IllegalArgumentException("Partition-capable stores require a source-read primitive");
		_data = data;
		_source = sourcePrimitive;
		_partitionBytes = partitionBytes;
		_primitive = new Materializer(source);
		_partitionStore.whenComplete((store, error) -> tryFinalize());
		_tileStore.whenComplete((store, error) -> tryFinalize());
	}

	/**
	 * Claims native partition delivery before source execution. Configuration makes a source capable of grouping;
	 * this consumer claim is what actually enables it.
	 */
	public synchronized boolean requestPartitions() {
		if(_primitive.hasStartedExecution())
			return _partitioned;
		if(!_source.requestSourceGroups(_partitionBytes))
			return false;
		_partitionsRequested = true;
		return true;
	}

	public synchronized boolean isPartitioned() {
		return _primitive.hasStartedExecution() && _partitioned;
	}

	public synchronized boolean partitionsSelected() {
		return _partitionsRequested || isPartitioned();
	}

	@Override
	public long maxPhysicalReadBytes(long logicalBytes) {
		return Math.max(_partitionBytes, OOCStreamable.super.maxPhysicalReadBytes(logicalBytes));
	}

	public synchronized MaterializedStoreStreamable unpartitionedView() {
		if(_unpartitioned == null) {
			_unpartitioned = MaterializedStoreStreamable.unpartitioned(getReadStream(), _data,
				OOCStoreLayout.ROW_MAJOR);
			//The conversion owns the read handle above. Once existing partition consumers release their handles, the
			//source store can forget each partition as the conversion advances.
			scheduleMaterializedStoreDeletion();
		}
		return _unpartitioned;
	}

	public synchronized MaterializedStoreStreamable existingUnpartitionedView() {
		return _unpartitioned;
	}

	public synchronized OOCFuture<MaterializedStore<PackedBlock>> acquirePartitions() {
		if(!requestPartitions())
			throw new DMLRuntimeException("Source can no longer select partitioned storage");
		if(_closed)
			throw new DMLRuntimeException("Partition store is closed");
		if(_reserved > 0)
			_reserved--;
		else if(_delete)
			throw new DMLRuntimeException("Partition store is scheduled for deletion");
		_active++;
		OOCPlanner.compileAndStart(_primitive);
		return _partitionStore;
	}

	public synchronized boolean registerLiveConsumer(Consumer<StoreLease<PackedBlock>> consumer) {
		if(!requestPartitions() || _liveConsumer != null)
			return false;
		_liveConsumer = consumer;
		return true;
	}

	public void releasePartitions() {
		synchronized(this) {
			_active--;
		}
		trySealPartitionReaders();
		tryFinalize();
	}

	public void partitionReaderOpened() {
		synchronized(this) {
			_openPartitionReaders++;
		}
		trySealPartitionReaders();
	}

	public synchronized void partitionReaderClosed() {
		_openPartitionReaders--;
	}

	@Override
	public synchronized void reserveLazyHandle() {
		//A late primitive is constructed against the MatrixObject's original handle before planner negotiation can
		//redirect it to the memoized tile view. Keep this transient reservation legal; replaceInput immediately moves
		//it to the unpartitioned store.
		if(_closed && _unpartitioned == null)
			throw new DMLRuntimeException("Partition store is closed");
		_reserved++;
	}

	@Override
	public void discardHandle() {
		synchronized(this) {
			if(_reserved > 0)
				_reserved--;
		}
		tryFinalize();
	}

	@Override
	public void scheduleMaterializedStoreDeletion() {
		synchronized(this) {
			_delete = true;
		}
		trySealPartitionReaders();
		tryFinalize();
	}

	private void trySealPartitionReaders() {
		MaterializedStore<PackedBlock> store;
		synchronized(this) {
			if(!_delete || !_partitioned || _partitionReadersSealed || _reserved != 0 ||
				_openPartitionReaders != _active)
				return;
			store = _partitionStore.getNow(null);
			if(store == null)
				return;
			_partitionReadersSealed = true;
		}
		store.completion().whenComplete((ignored, error) -> {
			if(error == null)
				store.sealReaders();
		});
	}

	private void tryFinalize() {
		synchronized(this) {
			if(!_delete || _reserved != 0 || _active != 0 || _closed || !_materializationDone)
				return;
			_closed = true;
		}
		if(_partitioned)
			_partitionStore.whenComplete((store, error) -> {
				if(store != null)
					store.close();
			});
		else
			_tileStore.whenComplete((store, error) -> {
				if(store != null)
					store.close();
			});
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReadStream() {
		reserveLazyHandle();
		return getReservedReadStream();
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReservedReadStream() {
		Replay replay = new Replay();
		replay.setData(_data);
		replay.assignPrimitive(_primitive);
		return replay;
	}

	@Override
	public OOCStream<IndexedMatrixValue> getWriteStream() {
		throw new UnsupportedOperationException("Partitioned source is read-only");
	}

	@Override
	public boolean hasMaterializedStore() {
		return true;
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
	public DataCharacteristics getDataCharacteristics() {
		return _data.getDataCharacteristics();
	}

	@Override
	public CacheableData<?> getData() {
		return _data;
	}

	@Override
	public void setData(CacheableData<?> data) {
		_data = data;
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return _primitive;
	}

	private final class Materializer extends OOCPrimitive {
		private Materializer(OOCStream<IndexedMatrixValue> source) {
			super(null, source);
		}

		@Override
		protected void inferPatternsInternal() {
			_pattern = OOCAccessPattern.UNKNOWN;
			inferParentPatterns();
		}

		@Override
		protected void requestPatternInternal(OOCAccessPattern pattern) {
			_pattern = OOCAccessPattern.UNKNOWN;
		}

		@Override
		protected void startExecution() {
			synchronized(PartitionedStoreStreamable.this) {
				_partitioned = _partitionsRequested;
			}
			if(!_partitioned) {
				startTileMaterialization();
				return;
			}
			MaterializedStore<PackedBlock> store = new MaterializedStore<>(OOCCacheManager.getGlobalCache(),
				CachingStream._streamSeq.getNextID(), -1, 1, null, getDataCharacteristics());
			try {
				PartitionedOOCStreamMaterializer materializer = new PartitionedOOCStreamMaterializer(store,
					getDataCharacteristics(), _liveConsumer);
				materializer.completion().whenComplete((ignored, error) -> {
					synchronized(PartitionedStoreStreamable.this) {
						_materializationDone = true;
					}
					if(error != null) {
						_partitionStore.completeExceptionally(error);
						store.close();
						fail(error);
					}
					onComplete();
					tryFinalize();
				});
				_partitionStore.complete(store);
				materializer.attach(getInputReadStream(0));
			}
			catch(Throwable error) {
				store.failMaterialization(error);
				store.close();
				_partitionStore.completeExceptionally(error);
				fail(error);
				onComplete();
			}
		}

		private void startTileMaterialization() {
			DataCharacteristics characteristics = getDataCharacteristics();
			MaterializedStore<IndexedMatrixValue> store = new MaterializedStore<>(OOCCacheManager.getGlobalCache(),
				CachingStream._streamSeq.getNextID(), -1, 1, OOCStoreLayout.ROW_MAJOR, characteristics);
			try {
				OOCStreamMaterializer materializer = new OOCStreamMaterializer(store,
					indexes -> OOCStoreLayout.ROW_MAJOR.linearize(indexes, characteristics), _allowance,
					java.util.List.of(), characteristics.getBlocksize());
				materializer.completion().whenComplete((ignored, error) -> {
					synchronized(PartitionedStoreStreamable.this) {
						_materializationDone = true;
					}
					if(error != null) {
						_tileStore.completeExceptionally(error);
						store.close();
						fail(error);
					}
					onComplete();
					tryFinalize();
				});
				_tileStore.complete(store);
				materializer.attach(getInputReadStream(0));
			}
			catch(Throwable error) {
				store.failMaterialization(error);
				store.close();
				_tileStore.completeExceptionally(error);
				fail(error);
				onComplete();
			}
		}
	}

	private final class Replay extends SubscribableTaskQueue<IndexedMatrixValue> {
		private final AtomicBoolean _started = new AtomicBoolean();

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
			if(!_started.compareAndSet(false, true))
				return;
			acquireRead();
			if(isPartitioned())
				_partitionStore.whenComplete((store, error) -> {
					if(error != null) {
						propagateFailure(DMLRuntimeException.of(error));
						releasePartitions();
						return;
					}
					store.completion().whenComplete((ignored, completionError) -> replay(store, completionError));
				});
			else
				_tileStore.whenComplete((store, error) -> {
				if(error != null) {
					propagateFailure(DMLRuntimeException.of(error));
					releasePartitions();
					return;
				}
				store.completion().whenComplete((ignored, completionError) -> replayTiles(store, completionError));
				});
		}

		private void acquireRead() {
			synchronized(PartitionedStoreStreamable.this) {
				if(_closed)
					throw new DMLRuntimeException("Source store is closed");
				if(_reserved > 0)
					_reserved--;
				else if(_delete)
					throw new DMLRuntimeException("Source store is scheduled for deletion");
				_active++;
			}
			OOCPlanner.compileAndStart(_primitive);
		}

		private void replayTiles(MaterializedStore<IndexedMatrixValue> store, Throwable error) {
			if(error != null) {
				propagateFailure(DMLRuntimeException.of(error));
				releasePartitions();
				return;
			}
			SyncMemoryAllowance allowance = new SyncMemoryAllowance(GlobalMemoryBroker.getSource(),
				GlobalMemoryBroker.getSource().getAllowedMemory() / 2);
			StoreBackedStream<IndexedMatrixValue> stream = new StoreBackedStream<>(
				store.openReader(new SequentialAccessPattern(store.size()), allowance, 1));
			stream.setSubscriber(callback -> {
				if(callback.isFailure() || callback.isEos()) {
					try {
						if(callback.isFailure())
							callback.get();
						closeInput();
					}
					catch(Throwable failure) {
						propagateFailure(DMLRuntimeException.of(failure));
					}
					finally {
						allowance.shutdown();
						releasePartitions();
					}
				}
				else
					enqueue(callback.keepOpen());
			});
		}

		private void replay(MaterializedStore<PackedBlock> store, Throwable error) {
			if(error != null) {
				propagateFailure(DMLRuntimeException.of(error));
				releasePartitions();
				return;
			}
			SyncMemoryAllowance allowance = new SyncMemoryAllowance(GlobalMemoryBroker.getSource(),
				GlobalMemoryBroker.getSource().getAllowedMemory() / 2);
			StoreBackedStream<PackedBlock> stream = new StoreBackedStream<>(
				store.openReader(new SequentialAccessPattern(store.size()), allowance, 1));
			partitionReaderOpened();
			stream.setSubscriber(callback -> {
				if(callback.isFailure() || callback.isEos()) {
					try {
						if(callback.isFailure())
							callback.get();
						closeInput();
					}
					catch(Throwable failure) {
						propagateFailure(DMLRuntimeException.of(failure));
					}
					finally {
						allowance.shutdown();
						partitionReaderClosed();
						releasePartitions();
					}
				}
				else {
					PackedBlock pack = callback.get();
					for(int i = 0; i < pack.count(); i++)
						enqueue(new TileCallback((IndexedMatrixValue) pack.value(i), callback.keepOpen()));
				}
			});
		}
	}

	private static final class TileCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final IndexedMatrixValue _value;
		private final OOCStream.QueueCallback<PackedBlock> _owner;

		private TileCallback(IndexedMatrixValue value, OOCStream.QueueCallback<PackedBlock> owner) {
			_value = value;
			_owner = owner;
		}

		@Override
		public IndexedMatrixValue get() {
			return _value;
		}

		@Override
		public OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
			return new TileCallback(_value, _owner.keepOpen());
		}

		@Override
		public void close() {
			_owner.close();
		}

		@Override
		public void fail(DMLRuntimeException error) {
			_owner.fail(error);
		}

		@Override
		public boolean isEos() {
			return false;
		}

		@Override
		public boolean isFailure() {
			return false;
		}
	}
}
