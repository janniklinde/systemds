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
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;

public final class PartitionedStoreStreamable implements OOCStreamable<IndexedMatrixValue> {
	private final Materializer _primitive;
	private final OOCFuture<MaterializedStore<PackedBlock>> _store = new OOCFuture<>();
	private CacheableData<?> _data;
	private int _reserved;
	private int _active;
	private boolean _delete;
	private boolean _closed;

	public PartitionedStoreStreamable(OOCStream<IndexedMatrixValue> source, CacheableData<?> data, long bytes) {
		_data = data;
		_primitive = new Materializer(source, bytes);
		_store.whenComplete((store, error) -> tryFinalize());
	}

	public synchronized OOCFuture<MaterializedStore<PackedBlock>> acquirePartitions() {
		if(_closed)
			throw new DMLRuntimeException("Partition store is closed");
		if(_reserved > 0)
			_reserved--;
		else if(_delete)
			throw new DMLRuntimeException("Partition store is scheduled for deletion");
		_active++;
		OOCPlanner.compileAndStart(_primitive);
		return _store;
	}

	public void releasePartitions() {
		synchronized(this) {
			_active--;
		}
		tryFinalize();
	}

	@Override
	public synchronized void reserveLazyHandle() {
		if(_closed)
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
		tryFinalize();
	}

	private void tryFinalize() {
		synchronized(this) {
			if(!_delete || _reserved != 0 || _active != 0 || _closed || !_store.isDone())
				return;
			_closed = true;
		}
		_store.whenComplete((store, error) -> {
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
		private final long _bytes;

		private Materializer(OOCStream<IndexedMatrixValue> source, long bytes) {
			super(null, source);
			_bytes = bytes;
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
			MaterializedStore<PackedBlock> store = new MaterializedStore<>(OOCCacheManager.getGlobalCache(),
				CachingStream._streamSeq.getNextID(), -1, 1, null, getDataCharacteristics());
			try {
				PartitionedOOCStreamMaterializer materializer = new PartitionedOOCStreamMaterializer(store, _allowance,
					getDataCharacteristics(), _bytes);
				materializer.completion().whenComplete((ignored, error) -> {
					if(error != null) {
						_store.completeExceptionally(error);
						store.close();
						fail(error);
					}
					else
						_store.complete(store);
					onComplete();
				});
				materializer.attach(getInputReadStream(0));
			}
			catch(Throwable error) {
				store.close();
				_store.completeExceptionally(error);
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
			acquirePartitions().whenComplete((store, error) -> {
				if(error != null) {
					propagateFailure(DMLRuntimeException.of(error));
					releasePartitions();
					return;
				}
				SyncMemoryAllowance allowance = new SyncMemoryAllowance(GlobalMemoryBroker.getSource(),
					GlobalMemoryBroker.getSource().getAllowedMemory() / 2);
				StoreBackedStream<PackedBlock> stream = new StoreBackedStream<>(
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
					else {
						PackedBlock pack = callback.get();
						for(int i = 0; i < pack.count(); i++)
							enqueue(new TileCallback((IndexedMatrixValue) pack.value(i), callback.keepOpen()));
					}
				});
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
