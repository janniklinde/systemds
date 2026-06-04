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

package org.apache.sysds.runtime.instructions.ooc;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.parfor.util.IDSequence;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.GroupedBlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.runtime.ooc.primitives.CachingOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.SourceOOCStream;
import org.apache.sysds.runtime.ooc.stream.message.OOCGetStreamTypeMessage;
import org.apache.sysds.runtime.ooc.stream.message.OOCStreamMessage;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.util.IndexRange;
import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

/**
 * A wrapper around LocalTaskQueue to consume the source stream and reset to
 * consume again for other operators.
 *
 */
	public class CachingStream implements OOCStreamable<IndexedMatrixValue> {

	public static final IDSequence _streamSeq = new IDSequence();
	private static final ConcurrentHashMap<Long, CachingStream> LIVE_STREAMS = new ConcurrentHashMap<>();
	private static final int DEFAULT_ALLOWANCE_REPLAY_INFLIGHT = 8;

	// original live stream
	private final OOCStream<IndexedMatrixValue> _source;
	private final CachingOOCPrimitive _primitive;
	private final IntArrayList _consumptionCounts = new IntArrayList();
	private final IntArrayList _consumerConsumptionCounts = new IntArrayList();
	private final IntArrayList _groupIndices = new IntArrayList();
	private final IntArrayList _groupSizes = new IntArrayList();
	private final List<MatrixIndexes> _blockIndexes = new ArrayList<>();
	private final List<BlockKey> _cacheKeys = new ArrayList<>();
	private final List<String> _consumerRegistrations = new ArrayList<>();
	private final BitSet _ownedCacheKeys = new BitSet();
	private int _ownedCacheKeysSize = 0;

	// stream identifier
	private final long _streamId;

	// block counter
	private int _numBlocks = 0;

	private Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>[] _subscribers;
	private CopyOnWriteArrayList<Consumer<OOCStreamMessage>> _downstreamRelays;

	// state flags
	private boolean _cacheInProgress = true; // caching in progress, in the first pass.
	private volatile Map<MatrixIndexes, Integer> _index;

	private DMLRuntimeException _failure;

	private boolean _deletable = false;
	private int _maxConsumptionCount = 0;
	private int _lazyHandleReservations = 0;
	private String _watchdogId = null;

	public CachingStream(OOCStream<IndexedMatrixValue> source) {
		this(source, _streamSeq.getNextID());
	}

	public CachingStream(OOCStream<IndexedMatrixValue> source, long streamId) {
		_source = source;
		_primitive = new CachingOOCPrimitive(source, this);
		_source.setDownstreamMessageRelay(this::messageDownstream);
		_streamId = streamId;
		if(OOCDebug.TRACK_LIVE_STATE || OOCDebug.DUMP_CACHE_STATE)
			LIVE_STREAMS.put(streamId, this);
		if(OOCWatchdog.WATCH) {
			_watchdogId = "CS-" + hashCode();
			// Capture a short context to help identify origin
			OOCWatchdog.registerOpen(_watchdogId, toString(), getCtxMsg(), this);
		}
		_downstreamRelays = null;
		source.setSubscriber(tmp -> {
			try(tmp) {
				int blk;
				int groupSize = 1;
				Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>[] mSubscribers;
				OOCStream.QueueCallback<IndexedMatrixValue> mCallback = null;

				synchronized(this) {
					mSubscribers = _subscribers;
					if(!tmp.isEos()) {
						if(!_cacheInProgress)
							throw new DMLRuntimeException("Stream is closed");

						if(tmp instanceof OOCStream.GroupQueueCallback<?>) {
							OOCStream.GroupQueueCallback<IndexedMatrixValue> group =
								(OOCStream.GroupQueueCallback<IndexedMatrixValue>) tmp;
							groupSize = group.size();
							List<MatrixIndexes> groupBlockIndexes = new ArrayList<>(groupSize);
							for(int gi = 0; gi < groupSize; gi++) {
								OOCStream.QueueCallback<IndexedMatrixValue> sub = group.getCallback(gi);
								try(sub) {
									IndexedMatrixValue imv = sub.get();
									groupBlockIndexes.add(new MatrixIndexes(imv.getIndexes()));
									if(_index != null)
										_index.put(imv.getIndexes(), _numBlocks + gi);
								}
							}

							BlockKey baseKey;
							boolean ownsEntry = true;
							if(tmp instanceof OOCCacheManager.CachedGroupCallback<?> cachedGroup) {
								baseKey = cachedGroup.getBlockKey();
								ensureReferencedOrRematerialize(baseKey, cachedGroup);
								ownsEntry = false;
								if(mSubscribers != null && mSubscribers.length > 0)
									mCallback = tmp.keepOpen();
							}
							else {
								List<IndexedMatrixValue> values = new ArrayList<>(groupSize);
								long totalSize = 0;
								for(int gi = 0; gi < groupSize; gi++) {
									OOCStream.QueueCallback<IndexedMatrixValue> sub = group.getCallback(gi);
									try(sub) {
										IndexedMatrixValue imv = sub.get();
										values.add(imv);
										totalSize += ((MatrixBlock) imv.getValue()).getExactSerializedSize();
									}
								}

								baseKey = new BlockKey(_streamId, _numBlocks);
								if (_source instanceof SourceOOCStream && tmp instanceof SourceOOCStream.SourceGroupCallback sg) {
									OOCIOHandler.GroupSourceBlockDescriptor gdesc = sg.getDescriptor();
									if (mSubscribers == null || mSubscribers.length == 0)
										OOCCacheManager.putRawSourceBacked(baseKey, values, totalSize, gdesc);
									else
										mCallback = OOCCacheManager.putAndPinRawSourceBacked(baseKey, values, totalSize, gdesc);
								}
								else {
									if(mSubscribers == null || mSubscribers.length == 0)
										OOCCacheManager.putRaw(baseKey, values, totalSize);
									else
										mCallback = OOCCacheManager.putAndPinRaw(baseKey, values, totalSize);
								}
							}

							blk = _numBlocks;
							_numBlocks += groupSize;
							for(int gi = 0; gi < groupSize; gi++) {
								registerCacheKey(blk + gi,
									new GroupedBlockKey(baseKey.getStreamId(), (int) baseKey.getSequenceNumber(), gi),
									ownsEntry);
								_blockIndexes.add(groupBlockIndexes.get(gi));
								_consumptionCounts.add(0);
								_groupIndices.add(gi);
								_groupSizes.add(groupSize);
								if(_deletable)
									tryDeleteBlock(blk + gi);
							}
						}
						else {
							final IndexedMatrixValue task = tmp.get();
							OOCIOHandler.SourceBlockDescriptor descriptor = null;
							BlockKey blockKey = null;
							boolean ownsEntry = true;

							if(tmp instanceof OOCCacheManager.CachedQueueCallback<?> cachedQueue) {
								blockKey = cachedQueue.getBlockKey();
								ensureReferencedOrRematerialize(blockKey, task);
								ownsEntry = false;
								if(mSubscribers != null && mSubscribers.length > 0)
									mCallback = tmp.keepOpen();
							}
							else if(tmp instanceof OOCCacheManager.CachedSubCallback<?> cachedSub) {
								BlockKey parent = cachedSub.getParent().getBlockKey();
								ensureReferencedOrRematerialize(parent, cachedSub.getParent());
								blockKey = new GroupedBlockKey(parent.getStreamId(), (int) parent.getSequenceNumber(),
									cachedSub.getGroupIndex());
								ownsEntry = false;
								if(mSubscribers != null && mSubscribers.length > 0)
									mCallback = tmp.keepOpen();
							}

							if(_source instanceof SourceOOCStream src) {
								descriptor = src.getDescriptor(task.getIndexes());
							}

							if(blockKey == null) {
								ownsEntry = true;
								blockKey = new BlockKey(_streamId, _numBlocks);
								if(descriptor == null && tmp instanceof InMemoryQueueCallback inMemory) {
									InMemoryQueueCallback handover = (InMemoryQueueCallback) inMemory.keepOpen();
									if(mSubscribers == null || mSubscribers.length == 0)
										OOCCacheManager.handover(blockKey, handover);
									else
										mCallback = OOCCacheManager.handoverAndPin(blockKey, handover);
								}
								else if(descriptor == null) {
									if(mSubscribers == null || mSubscribers.length == 0)
										OOCCacheManager.put(_streamId, _numBlocks, task);
									else
										mCallback = OOCCacheManager.putAndPin(_streamId, _numBlocks, task);
								}
								else {
									if(mSubscribers == null || mSubscribers.length == 0)
										OOCCacheManager.putSourceBacked(_streamId, _numBlocks, task, descriptor);
									else
										mCallback = OOCCacheManager.putAndPinSourceBacked(_streamId, _numBlocks, task,
											descriptor);
								}
							}

							if(_index != null)
								_index.put(task.getIndexes(), _numBlocks);

							blk = _numBlocks;
							_numBlocks++;
							registerCacheKey(blk, blockKey, ownsEntry);
							_blockIndexes.add(new MatrixIndexes(task.getIndexes()));
							_consumptionCounts.add(0);
							_groupIndices.add(-1);
							_groupSizes.add(1);

							if(_deletable)
								tryDeleteBlock(blk);
						}

						notifyAll();
					}
					else {
						_cacheInProgress = false; // caching is complete
						try {
							validateBlockCountOnClose();
						}
						catch(Exception e) {
							_failure = e instanceof DMLRuntimeException ? (DMLRuntimeException) e : new DMLRuntimeException(e);
						}
						if (OOCWatchdog.WATCH)
							OOCWatchdog.registerClose(_watchdogId);
						notifyAll();
						blk = -1;
					}
				}

				if(mSubscribers != null && mSubscribers.length > 0) {
					final OOCStream.QueueCallback<IndexedMatrixValue> finalCallback = mCallback;
					try(finalCallback) {
						if(blk != -1) {
							for(int i = 0; i < mSubscribers.length; i++) {
								OOCStream.QueueCallback<IndexedMatrixValue> localCallback = finalCallback.keepOpen();
								try(localCallback) {
									mSubscribers[i].accept(localCallback);
								}
								boolean done = false;
								for(int gi = 0; gi < groupSize; gi++) {
									if(onConsumed(blk + gi, i))
										done = true;
								}
								if(done)
									mSubscribers[i].accept(OOCStream.eos(_failure));
							}
						}
						else {
							OOCStream.QueueCallback<IndexedMatrixValue> cb = OOCStream.eos(_failure);
							for(int i = 0; i < mSubscribers.length; i++) {
								if(onNoMoreTasks(i))
									mSubscribers[i].accept(cb);
							}
						}
					}
				}
			} catch (DMLRuntimeException e) {
				// Propagate failure to subscribers
				_failure = e;
				synchronized (this) {
					notifyAll();
				}

				Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>[] mSubscribers = _subscribers;
				OOCStream.QueueCallback<IndexedMatrixValue> err = OOCStream.eos(_failure);
				if(mSubscribers != null) {
					for(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> mSubscriber : mSubscribers) {
						try {
							mSubscriber.accept(err);
						} catch (Exception ignored) {
						}
					}
				}
			}
		});
	}

	public static String dumpStreams(Iterable<Long> streamIds) {
		if(!OOCDebug.DUMP_CACHE_STATE)
			return "";
		StringBuilder sb = new StringBuilder();
		for(Long streamId : streamIds) {
			CachingStream stream = LIVE_STREAMS.get(streamId);
			sb.append("[WARN] CachingStream dump streamId=").append(streamId);
			if(stream == null) {
				sb.append(" missing").append('\n');
				continue;
			}
			synchronized(stream) {
				sb.append(" cacheInProgress=").append(stream._cacheInProgress)
					.append(" deletable=").append(stream._deletable)
					.append(" maxConsumptionCount=").append(stream._maxConsumptionCount)
					.append(" numBlocks=").append(stream._numBlocks)
					.append(" ownedCacheKeysSize=").append(stream._ownedCacheKeysSize)
					.append(" subscribers=").append(stream._subscribers == null ? 0 : stream._subscribers.length)
					.append(" failure=").append(stream._failure != null)
					.append('\n');
				sb.append("  consumptionCounts=").append(stream._consumptionCounts).append('\n');
				sb.append("  consumerCounts=").append(stream._consumerConsumptionCounts).append('\n');
				sb.append("  groupIndices=").append(stream._groupIndices).append('\n');
				sb.append("  groupSizes=").append(stream._groupSizes).append('\n');
				sb.append("  cacheKeys=").append(stream._cacheKeys).append('\n');
				sb.append("  ownedCacheKeys=").append(stream._ownedCacheKeys).append('\n');
				sb.append("  consumerRegistrations=").append(stream._consumerRegistrations).append('\n');
				sb.append(PlaybackStream.dumpLivePlaybackStreams(stream.toString()));
			}
		}
		return sb.toString();
	}

	private static String debugOrigin(String label) {
		StackTraceElement[] st = new Exception().getStackTrace();
		for(int i = 2; i < st.length; i++) {
			String cls = st[i].getClassName();
			if(!cls.equals(CachingStream.class.getName()))
				return label + "@" + cls + ":" + st[i].getLineNumber();
		}
		return label + "@unknown";
	}


	private void ensureReferencedOrRematerialize(BlockKey key, IndexedMatrixValue value) {
		try {
			OOCCacheManager.getCache().addReference(key);
		}
		catch(IllegalArgumentException ex) {
			try {
				OOCCacheManager.putRaw(key, value, ((MatrixBlock) value.getValue()).getExactSerializedSize());
			}
			catch(IllegalStateException putEx) {
				// Another downstream stream may have re-materialized the same entry first.
				OOCCacheManager.getCache().addReference(key);
			}
		}
	}

	private void ensureReferencedOrRematerialize(BlockKey key, OOCCacheManager.CachedGroupCallback<?> group) {
		try {
			OOCCacheManager.getCache().addReference(key);
		}
		catch(IllegalArgumentException ex) {
			try {
				List<IndexedMatrixValue> values = new ArrayList<>(group.size());
				long totalSize = 0;
				for(int gi = 0; gi < group.size(); gi++) {
					@SuppressWarnings("unchecked")
					OOCStream.QueueCallback<IndexedMatrixValue> sub =
						(OOCStream.QueueCallback<IndexedMatrixValue>) group.getCallback(gi);
					try(sub) {
						IndexedMatrixValue imv = sub.get();
						values.add(imv);
						totalSize += ((MatrixBlock) imv.getValue()).getExactSerializedSize();
					}
				}
				OOCCacheManager.putRaw(key, values, totalSize);
			}
			catch(IllegalStateException putEx) {
				// Another downstream stream may have re-materialized the same entry first.
				OOCCacheManager.getCache().addReference(key);
			}
		}
	}

	private String getCtxMsg() {
		StackTraceElement[] st = new Exception().getStackTrace();
		// Skip the first few frames (constructor, createWritableStream, etc.)
		StringBuilder sb = new StringBuilder();
		int limit = Math.min(st.length, 7);
		for(int i = 2; i < limit; i++) {
			sb.append(st[i].getClassName()).append(".").append(st[i].getMethodName()).append(":")
				.append(st[i].getLineNumber());
			if(i < limit - 1)
				sb.append(" <- ");
		}
		return sb.toString();
	}

	public synchronized void scheduleDeletion() {
		if (_deletable)
			return; // Deletion already scheduled

		if (_cacheInProgress && _maxConsumptionCount == 0 && _lazyHandleReservations == 0)
			System.out.println("[WARN] Scheduling deletion for caching stream with no listeners: " + this);

		_deletable = true;
		for (int i = 0; i < _consumptionCounts.size(); i++) {
			tryDeleteBlock(i);
		}
	}

	public String toString() {
		return "CachingStream@" + _streamId;
	}

	private synchronized void tryDeleteBlock(int i) {
		if(_lazyHandleReservations > 0)
			return;
		int cnt = _consumptionCounts.getInt(i);
		if (cnt > _maxConsumptionCount)
			throw new DMLRuntimeException("Cannot have more than " + _maxConsumptionCount + " consumptions.");
		if(!_ownedCacheKeys.get(i))
			return;

		int groupIdx = _groupIndices.getInt(i);
		int groupSize = _groupSizes.getInt(i);
		if (groupIdx > 0 && groupSize > 1) {
			// Grouped entries are physically represented by a single base cache entry.
			// Re-check the base when a member reaches its terminal count.
			if (cnt == _maxConsumptionCount) {
				int baseId = i - groupIdx;
				tryDeleteBlock(baseId);
			}
			return;
		}

		if (cnt == _maxConsumptionCount) {
			if (groupIdx >= 0 && groupSize > 1) {
				int baseId = i - groupIdx;
				for (int j = 0; j < groupSize; j++) {
					if (_consumptionCounts.getInt(baseId + j) < _maxConsumptionCount)
						return;
				}
				OOCCacheManager.getCache().forget(getEntryBlockKey(baseId));
			}
			else {
				OOCCacheManager.getCache().forget(getEntryBlockKey(i));
			}
		}
	}

	private synchronized boolean onConsumed(int blockIdx, int consumerIdx) {
		int newCount = _consumptionCounts.getInt(blockIdx)+1;
		if (newCount > _maxConsumptionCount)
			throw new DMLRuntimeException("Cannot have more than " + _maxConsumptionCount + " consumptions.");
		_consumptionCounts.set(blockIdx, newCount);
		int newConsumerCount = _consumerConsumptionCounts.getInt(consumerIdx)+1;
		_consumerConsumptionCounts.set(consumerIdx, newConsumerCount);

		if (_deletable)
			tryDeleteBlock(blockIdx);

		return !_cacheInProgress && newConsumerCount == _numBlocks + 1;
	}

	private synchronized boolean onNoMoreTasks(int consumerIdx) {
		int newConsumerCount = _consumerConsumptionCounts.getInt(consumerIdx)+1;
		_consumerConsumptionCounts.set(consumerIdx, newConsumerCount);
		return !_cacheInProgress && newConsumerCount == _numBlocks + 1;
	}

	public synchronized CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> get(int idx) throws InterruptedException,
		ExecutionException {
		while (true) {
			if(_failure != null)
				throw _failure;
			else if(idx < _numBlocks) {
				return OOCCacheManager.requestBlock(getBlockKey(idx))
					.thenApply(cb -> {
						synchronized(this) {
							if(_index != null) // Ensure index is up to date
								_index.putIfAbsent(cb.get().getIndexes(), idx);

							int newCount = _consumptionCounts.getInt(idx) + 1;
							if(newCount > _maxConsumptionCount)
								throw new DMLRuntimeException("Consumer overflow! Expected: " + _maxConsumptionCount);
							_consumptionCounts.set(idx, newCount);

							if(_deletable)
								tryDeleteBlock(idx);
						}
						return cb;
					});
			}
			else if(!_cacheInProgress) {
				return CompletableFuture.completedFuture(new OOCStream.SimpleQueueCallback<>(null, _failure));
			}

			wait();
		}
	}

	public int findCachedIndex(MatrixIndexes idx) {
		return _index.get(idx);
	}

	public synchronized BlockKey peekCachedBlockKey(MatrixIndexes idx) {
		return getBlockKey(_index.get(idx));
	}

	public synchronized OOCStream.QueueCallback<IndexedMatrixValue> findCached(MatrixIndexes idx) {
		int mIdx = _index.get(idx);
		int newCount = _consumptionCounts.getInt(mIdx)+1;
		if (newCount > _maxConsumptionCount)
			throw new DMLRuntimeException("Consumer overflow in " + _streamId + "_" + mIdx + ". Expected: " +
				_maxConsumptionCount);

		_consumptionCounts.set(mIdx, newCount);

		try {
			return OOCCacheManager.requestBlock(getBlockKey(mIdx)).get();
		} catch (InterruptedException | ExecutionException e) {
			return new OOCStream.SimpleQueueCallback<>(null, new DMLRuntimeException(e));
		} finally {
			if (_deletable)
				tryDeleteBlock(mIdx);
		}
	}

	public void findCachedAsync(MatrixIndexes idx, Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> callback) {
		int mIdx;
		synchronized(this) {
			mIdx = _index.get(idx);
			int newCount = _consumptionCounts.getInt(mIdx)+1;
			if (newCount > _maxConsumptionCount)
				throw new DMLRuntimeException("Consumer overflow in " + _streamId + "_" + mIdx + ". Expected: " +
					_maxConsumptionCount);
		}
		OOCCacheManager.requestBlock(getBlockKey(mIdx)).whenComplete((cb, r) -> {
			try (cb) {
				synchronized(CachingStream.this) {
					int newCount = _consumptionCounts.getInt(mIdx) + 1;
					if(newCount > _maxConsumptionCount) {
						_failure = new DMLRuntimeException(
							"Consumer overflow in " + _streamId + "_" + mIdx + ". Expected: " + _maxConsumptionCount);
						cb.fail(_failure);
					}
					else
						_consumptionCounts.set(mIdx, newCount);
				}

				callback.accept(cb);
			}
		});
	}

	private void validateBlockCountOnClose() {
		DataCharacteristics dc = _source.getDataCharacteristics();
		if (dc != null && dc.dimsKnown() && dc.getBlocksize() > 0) {
			long expected = OOCUtils.getNumBlocks(dc);
			if (expected >= 0 && _numBlocks != expected) {
				throw new DMLRuntimeException("CachingStream block count mismatch: expected "
					+ expected + " but saw " + _numBlocks + " (" + dc.getRows() + "x" + dc.getCols() + ")");
			}
		}
	}

	/**
	 * Finds a cached item asynchronously without counting it as a consumption.
	 */
	public void peekCachedAsync(MatrixIndexes idx, Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> callback) {
		int mIdx = _index.get(idx);
		OOCCacheManager.requestBlock(getBlockKey(mIdx)).whenComplete((cb, r) -> callback.accept(cb));
	}

	/**
	 * Finds a cached item without counting it as a consumption.
	 */
	public OOCStream.QueueCallback<IndexedMatrixValue> peekCached(MatrixIndexes idx) {
		int mIdx = _index.get(idx);
		try {
			return OOCCacheManager.requestBlock(getBlockKey(mIdx)).get();
		} catch (InterruptedException | ExecutionException e) {
			return new OOCStream.SimpleQueueCallback<>(null, new DMLRuntimeException(e));
		}
	}

	private BlockKey getBlockKey(int blockIdx) {
		if(_cacheKeys.size() > blockIdx)
			return _cacheKeys.get(blockIdx);
		int groupIdx = _groupIndices.size() > blockIdx ? _groupIndices.getInt(blockIdx) : -1;
		if (groupIdx >= 0) {
			int baseId = blockIdx - groupIdx;
			return new GroupedBlockKey(_streamId, baseId, groupIdx);
		}
		return new BlockKey(_streamId, blockIdx);
	}

	private BlockKey getEntryBlockKey(int blockIdx) {
		BlockKey key = getBlockKey(blockIdx);
		if(key instanceof GroupedBlockKey)
			return new BlockKey(key.getStreamId(), key.getSequenceNumber());
		return key;
	}

	private void registerCacheKey(int blockIdx, BlockKey key, boolean ownsEntry) {
		_cacheKeys.add(key);
		if(ownsEntry)
			_ownedCacheKeys.set(blockIdx);
		else
			_ownedCacheKeys.clear(blockIdx);
		_ownedCacheKeysSize++;
		if(_cacheKeys.size() != blockIdx + 1 || _ownedCacheKeysSize != blockIdx + 1)
			throw new IllegalStateException("Invalid cache key registration order");
	}

	public void activateIndexing() {
		if(_index != null)
			return;
		synchronized(this) {
			if(_index == null)
				_index = new ConcurrentHashMap<>();
		}
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReadStream() {
		return new PlaybackStream(this, consumeLazyHandleReservation());
	}

	@Override
	public OOCStream<IndexedMatrixValue> getWriteStream() {
		return _source.getWriteStream();
	}

	@Override
	public boolean hasStreamCache() {
		return true;
	}

	@Override
	public CachingStream getStreamCache() {
		return this;
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return _primitive;
	}

	@Override
	public boolean isProcessed() {
		return false;
	}

	@Override
	public DataCharacteristics getDataCharacteristics() {
		return _source.getDataCharacteristics();
	}

	@Override
	public CacheableData<?> getData() {
		return _source.getData();
	}

	@Override
	public void setData(CacheableData<?> data) {
		_source.setData(data);
	}

	@Override
	public void messageUpstream(OOCStreamMessage msg) {
		if (msg.isCancelled())
			return;
		if(msg instanceof OOCGetStreamTypeMessage) {
			((OOCGetStreamTypeMessage) msg).setCachedType();
			activateIndexing();
			return;
		}

		_source.messageUpstream(msg);
	}

	@Override
	public void messageDownstream(OOCStreamMessage msg) {
		CopyOnWriteArrayList<Consumer<OOCStreamMessage>> relays = _downstreamRelays;
		if (relays != null) {
			for (Consumer<OOCStreamMessage> relay : relays) {
				if (msg.isCancelled())
					break;
				relay.accept(msg);
			}
		}
	}

	@Override
	public void setUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		addDownstreamMessageRelay(relay);
	}

	@Override
	public void addUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		if (relay == null)
			throw new IllegalArgumentException("Cannot set downstream relay to null");
		CopyOnWriteArrayList<Consumer<OOCStreamMessage>> relays = _downstreamRelays;
		if (relays == null) {
			synchronized(this) {
				if (_downstreamRelays == null)
					_downstreamRelays = new CopyOnWriteArrayList<>();
				relays = _downstreamRelays;
			}
		}
		relays.add(0, relay);
	}

	@Override
	public void clearUpstreamMessageRelays() {
		// No upstream relays supported
	}

	@Override
	public void clearDownstreamMessageRelays() {
		_downstreamRelays = null;
	}

	@Override
	public void setIXTransform(BiFunction<Boolean, IndexRange, IndexRange> transform) {
		throw new UnsupportedOperationException();
	}

	public void setSubscriber(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber, boolean incrConsumers) {
		setSubscriber(subscriber, incrConsumers, false);
	}

	public void setSubscriber(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber, boolean incrConsumers,
		MemoryAllowance readAllowance, ToLongFunction<MatrixIndexes> allocFn) {
		setSubscriber(subscriber, incrConsumers, false, readAllowance, allocFn);
	}

	@SuppressWarnings("unchecked")
	public void setSubscriber(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber, boolean incrConsumers,
		boolean bypassDeletable) {
		setSubscriber(subscriber, incrConsumers, bypassDeletable, null, null);
	}

	@SuppressWarnings("unchecked")
	public void setSubscriber(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber, boolean incrConsumers,
		boolean bypassDeletable, MemoryAllowance readAllowance, ToLongFunction<MatrixIndexes> allocFn) {
		boolean allowanceBound = readAllowance != null;
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> sourceSubscriber = subscriber;
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> replaySubscriber = subscriber;
		if(readAllowance != null) {
			if(allocFn == null)
				throw new IllegalArgumentException("Allocation function required for allowance-bound subscriber.");
			replaySubscriber = new AllowanceLifecycleSubscriber(subscriber, readAllowance);
		}
		if(_deletable && !bypassDeletable)
			throw new DMLRuntimeException("Cannot register a new subscriber on " + this + " because has been flagged for deletion");
		if(_failure != null)
			throw _failure;

		int mNumBlocks;
		boolean cacheInProgress;
		int consumerIdx;
		synchronized(this) {
			mNumBlocks = _numBlocks;
			cacheInProgress = _cacheInProgress;
			consumerIdx = _consumerConsumptionCounts.size();
			_consumerConsumptionCounts.add(0);
			if(incrConsumers) {
				_maxConsumptionCount++;
				if(OOCDebug.DUMP_CACHE_STATE)
					_consumerRegistrations.add(debugOrigin("setSubscriber+1"));
			}
			else {
				if(OOCDebug.DUMP_CACHE_STATE)
					_consumerRegistrations.add(debugOrigin("setSubscriber+0"));
			}
			if(cacheInProgress) {
				int newLen = _subscribers == null ? 1 : _subscribers.length + 1;
				Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>[] newSubscribers = new Consumer[newLen];

				if(newLen > 1)
					System.arraycopy(_subscribers, 0, newSubscribers, 0, newLen - 1);

				newSubscribers[newLen - 1] = sourceSubscriber;
				_subscribers = newSubscribers;
			}
		}

		if(allowanceBound) {
			Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> finalSubscriber = replaySubscriber;
			new Thread(() -> {
				replayExistingBlocks(mNumBlocks, consumerIdx, finalSubscriber, true, readAllowance, allocFn);
				if(!cacheInProgress && onNoMoreTasks(consumerIdx))
					finalSubscriber.accept(OOCStream.eos(_failure)); // NO_MORE_TASKS
			}).start();
		}
		else {
			replayExistingBlocks(mNumBlocks, consumerIdx, sourceSubscriber, false, null, null);
			if(!cacheInProgress && onNoMoreTasks(consumerIdx))
				sourceSubscriber.accept(OOCStream.eos(_failure)); // NO_MORE_TASKS
		}
	}

	private void replayExistingBlocks(int mNumBlocks, int consumerIdx,
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber, boolean sequential,
		MemoryAllowance readAllowance, ToLongFunction<MatrixIndexes> allocFn) {
		if(sequential) {
			replayExistingBlocksBounded(mNumBlocks, consumerIdx, subscriber, DEFAULT_ALLOWANCE_REPLAY_INFLIGHT,
				readAllowance, allocFn);
			return;
		}
		replayExistingBlocksBounded(mNumBlocks, consumerIdx, subscriber, 1, null, null);
	}

	private void replayExistingBlocksBounded(int mNumBlocks, int consumerIdx,
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber, int maxInflight,
		MemoryAllowance readAllowance, ToLongFunction<MatrixIndexes> allocFn) {
		Deque<ReplayRequest> inflight = new ArrayDeque<>(Math.max(1, maxInflight));
		int nextIdx = 0;
		while(true) {
			while(inflight.size() < maxInflight && nextIdx < mNumBlocks) {
				final int idx = nextIdx;
				int gIdx;
				int gSize;
				synchronized(this) {
					gIdx = _groupIndices.getInt(idx);
					gSize = _groupSizes.getInt(idx);
				}
				final int groupIdx = gIdx;
				final int groupSize = gSize;
				if(groupIdx > 0) {
					nextIdx++;
					continue; // only replay grouped blocks once at the base index
				}

				BlockKey replayKey = (groupSize > 1 && groupIdx == 0) ? getEntryBlockKey(idx) : getBlockKey(idx);
				if(readAllowance != null && allocFn != null) {
					long logicalBytes = getReplayLogicalBytes(idx, groupSize, allocFn);
					if(logicalBytes > 0 && !readAllowance.tryReserve(logicalBytes)) {
						if(inflight.isEmpty()) {
							readAllowance.reserveBlocking(logicalBytes);
						}
						else {
							if(!drainReplayRequest(inflight.removeFirst(), consumerIdx, subscriber))
								return;
							continue;
						}
					}
					inflight.addLast(new ReplayRequest(idx, groupSize,
						OOCCacheManager.requestBlockBacked(replayKey, readAllowance, logicalBytes)));
				}
				else {
					inflight.addLast(new ReplayRequest(idx, groupSize, OOCCacheManager.requestBlock(replayKey)));
				}
				nextIdx++;
			}

			if(inflight.isEmpty())
				return;

			if(!drainReplayRequest(inflight.removeFirst(), consumerIdx, subscriber))
				return;
		}
	}

	private CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> requestReplayBlock(BlockKey replayKey,
		int idx, int groupSize, MemoryAllowance readAllowance, ToLongFunction<MatrixIndexes> allocFn) {
		if(readAllowance == null || allocFn == null)
			return OOCCacheManager.requestBlock(replayKey);
		long logicalBytes = getReplayLogicalBytes(idx, groupSize, allocFn);
		readAllowance.reserveBlocking(logicalBytes);
		return OOCCacheManager.requestBlockBacked(replayKey, readAllowance, logicalBytes);
	}

	private long getReplayLogicalBytes(int idx, int groupSize, ToLongFunction<MatrixIndexes> allocFn) {
		long logicalBytes = 0;
		synchronized(this) {
			for(int gi = 0; gi < groupSize; gi++) {
				int blockIdx = idx + gi;
				if(blockIdx >= _blockIndexes.size())
					throw new DMLRuntimeException("Missing replay index metadata for block " + blockIdx
						+ " in " + this);
				logicalBytes += Math.max(0, allocFn.applyAsLong(_blockIndexes.get(blockIdx)));
			}
		}
		return logicalBytes;
	}

	private boolean drainReplayRequest(ReplayRequest req, int consumerIdx,
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber) {
		try {
			deliverReplayBlock(req.future.get(), req.idx, req.groupSize, consumerIdx, subscriber);
			return true;
		}
		catch(Throwable t) {
			subscriber.accept(OOCStream.eos(DMLRuntimeException.of(t)));
			return false;
		}
	}

	private static class ReplayRequest {
		private final int idx;
		private final int groupSize;
		private final CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future;

		private ReplayRequest(int idx, int groupSize, CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> future) {
			this.idx = idx;
			this.groupSize = groupSize;
			this.future = future;
		}
	}

	@SuppressWarnings("unchecked")
	private void deliverReplayBlock(OOCStream.QueueCallback<IndexedMatrixValue> cb, int idx, int groupSize,
		int consumerIdx, Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber) {
		try(cb) {
			synchronized(CachingStream.this) {
				if(_index != null) {
					if(cb instanceof OOCStream.GroupQueueCallback<?> && groupSize > 1) {
						OOCStream.GroupQueueCallback<IndexedMatrixValue> group =
							(OOCStream.GroupQueueCallback<IndexedMatrixValue>) cb;
						for(int gi = 0; gi < groupSize; gi++) {
							OOCStream.QueueCallback<IndexedMatrixValue> sub = group.getCallback(gi);
							try(sub) {
								_index.put(sub.get().getIndexes(), idx + gi);
							}
						}
					}
					else {
						_index.put(cb.get().getIndexes(), idx);
					}
				}
			}
			subscriber.accept(cb);

			boolean done = false;
			if(groupSize > 1) {
				for(int gi = 0; gi < groupSize; gi++) {
					if(onConsumed(idx + gi, consumerIdx))
						done = true;
				}
			}
			else if(onConsumed(idx, consumerIdx)) {
				done = true;
			}
			if(done)
				subscriber.accept(OOCStream.eos(_failure)); // NO_MORE_TASKS
		}
	}

	private static class AllowanceLifecycleSubscriber implements Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> {
		private final Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> _subscriber;
		private final MemoryAllowance _allowance;

		private AllowanceLifecycleSubscriber(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> subscriber,
			MemoryAllowance allowance) {
			_subscriber = subscriber;
			_allowance = allowance;
		}

		@Override
		public void accept(OOCStream.QueueCallback<IndexedMatrixValue> cb) {
			if(cb.isEos() || cb.isFailure()) {
				if(_allowance instanceof AutoCloseable ownedAllowance) {
					try {
						ownedAllowance.close();
					}
					catch(Exception e) {
						throw DMLRuntimeException.of(e);
					}
				}
				_subscriber.accept(cb);
				return;
			}
			_subscriber.accept(cb);
		}
	}

	/**
	 * Artificially increase subscriber count.
	 * Only use if certain blocks are accessed more than once.
	 */
	public synchronized void incrSubscriberCount(int count) {
		if (_deletable)
			throw new IllegalStateException("Cannot increment the subscriber count if flagged for deletion");

		_maxConsumptionCount += count;
		if(OOCDebug.DUMP_CACHE_STATE)
			_consumerRegistrations.add(debugOrigin("incrSubscriberCount+" + count));
	}

	@Override
	public synchronized void reserveLazyHandle() {
		_lazyHandleReservations++;
	}

	@Override
	public synchronized void discardHandle() {
		if(_lazyHandleReservations <= 0)
			return;
		_lazyHandleReservations--;
		if(_deletable) {
			for(int i = 0; i < _consumptionCounts.size(); i++)
				tryDeleteBlock(i);
		}
	}

	private synchronized boolean consumeLazyHandleReservation() {
		if(_lazyHandleReservations <= 0)
			return false;
		_lazyHandleReservations--;
		_maxConsumptionCount++;
		if(OOCDebug.DUMP_CACHE_STATE)
			_consumerRegistrations.add(debugOrigin("consumeLazyHandle+1"));
		return true;
	}

	/**
	 * Artificially increase the processing count of a block.
	 */
	public synchronized void incrProcessingCount(int i, int count) {
		int cnt = _consumptionCounts.getInt(i)+count;
		_consumptionCounts.set(i, cnt);

		if (_deletable)
			tryDeleteBlock(i);
	}

	public void incrProcessingCount(MatrixIndexes idx, int count) {
		incrProcessingCount(_index.get(idx), count);
	}
}
