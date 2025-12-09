package org.apache.sysds.runtime.ooc.cache;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.utils.Statistics;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class OOCCacheManager {
	private static final double OOC_BUFFER_PERCENTAGE = 0.1;
	private static final double OOC_BUFFER_PERCENTAGE_HARD = 0.15;
	private static final long _evictionLimit;
	private static final long _hardLimit;

	private static OOCIOHandler _ioHandler;
	private static OOCCacheScheduler _scheduler;

	static {
		_evictionLimit = (long)(Runtime.getRuntime().maxMemory() * OOC_BUFFER_PERCENTAGE);
		_hardLimit = (long)(Runtime.getRuntime().maxMemory() * OOC_BUFFER_PERCENTAGE_HARD);
		_ioHandler = new OOCMatrixIOHandler();
		_scheduler = new OOCLRUCacheScheduler(_ioHandler, _evictionLimit, _hardLimit);
	}

	public static void reset() {
		_ioHandler.shutdown();
		_scheduler.shutdown();
		_ioHandler = new OOCMatrixIOHandler();
		_scheduler = new OOCLRUCacheScheduler(_ioHandler, _evictionLimit, _hardLimit);
		if (DMLScript.STATISTICS) {
			System.out.println(Statistics.displayOOCEvictionStats());
			Statistics.resetOOCEvictionStats();
		}
	}

	public static OOCCacheScheduler getCache() {
		return _scheduler;
	}

	/**
	 * Removes a block from the cache without setting its data to null.
	 */
	public static void forget(long streamId, int blockId) {
		BlockKey key = new BlockKey(streamId, blockId);
		_scheduler.forget(key);
	}

	/**
	 * Store a block in the OOC cache (serialize once)
	 */
	public static void put(long streamId, int blockId, IndexedMatrixValue value) {
		BlockKey key = new BlockKey(streamId, blockId);
		_scheduler.put(key, value, ((MatrixBlock)value.getValue()).getExactSerializedSize());
	}

	public static CompletableFuture<OOCStream.QueueCallback<IndexedMatrixValue>> requestBlock(long streamId, long blockId) {
		BlockKey key = new BlockKey(streamId, blockId);
		return _scheduler.request(key).thenApply(e -> new CachedQueueCallback(e, null));
	}

	public static CompletableFuture<List<OOCStream.QueueCallback<IndexedMatrixValue>>> requestManyBlocks(List<BlockKey> keys) {
		return _scheduler.request(keys).thenApply(
			l -> l.stream().map(e -> (OOCStream.QueueCallback<IndexedMatrixValue>)new CachedQueueCallback(e, null)).toList());
	}

	private static void pin(BlockEntry entry) {
		_scheduler.pin(entry);
	}

	private static void unpin(BlockEntry entry) {
		_scheduler.unpin(entry);
	}




	static class CachedQueueCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final BlockEntry _result;
		private DMLRuntimeException _failure;
		private final AtomicBoolean _pinned;

		private CachedQueueCallback(BlockEntry result, DMLRuntimeException failure) {
			this._result = result;
			this._failure = failure;
			this._pinned = new AtomicBoolean(true);
		}

		@Override
		public IndexedMatrixValue get() {
			if (_failure != null)
				throw _failure;
			if (!_pinned.get())
				throw new IllegalStateException("Cannot get cached item of a closed callback");
			IndexedMatrixValue ret = (IndexedMatrixValue)_result.getData();
			if (ret == null)
				throw new IllegalStateException("Cannot get a cached item if it is not pinned in memory: " + _result.getState());
			return ret;
		}

		@Override
		public OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
			pin(_result);
			return new CachedQueueCallback(_result, _failure);
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			this._failure = failure;
		}

		@Override
		public boolean isEos() {
			return get() == null;
		}

		@Override
		public void close() {
			if (_pinned.compareAndSet(true, false)) {
				unpin(_result);
			}
		}
	}
}
