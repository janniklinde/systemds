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

package org.apache.sysds.runtime.ooc.cache.io;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.utils.Statistics;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class OOCIOHandlerImpl implements OOCIOHandler {
	private static final long MIN_READ_BYTES = 512_000;

	private final ThreadPoolExecutor _readExec;
	private final SpillStore _spill;
	private final SourceStore _source;
	private final int _readCallerId = OOCEventLog.registerCaller("read");

	private volatile OOCCache _cache;

	public OOCIOHandlerImpl() {
		DMLConfig conf = ConfigurationManager.getDMLConfig();
		int readers = conf.getIntValue(DMLConfig.OOC_IO_READER_THREADS);
		MatrixBlock.setUseCOO(conf.getBooleanValue(DMLConfig.OOC_SPARSE_COO));
		_readExec = new ThreadPoolExecutor(readers, readers, 0L, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(100000));
		_spill = new SpillStore();
		_source = new SourceStore();
	}

	@Override
	public void setCache(OOCCache cache) {
		_cache = cache;
		_spill.setCache(cache);
	}

	@Override
	public void shutdown() {
		_readExec.getQueue().clear();
		_readExec.shutdownNow();
		_spill.shutdown();
		_source.shutdown();
	}

	@Override
	public OOCFuture<Void> scheduleEviction(BlockEntry block) {
		return _spill.write(block);
	}

	@Override
	public OOCFuture<BlockEntry> scheduleRead(final BlockEntry block) {
		final OOCFuture<BlockEntry> future = new OOCFuture<>();
		int pinnedPartitionId = _spill.pinPartitionForRead(block);
		try {
			_readExec.execute(new ReadTask(block, future, pinnedPartitionId));
		}
		catch(RejectedExecutionException e) {
			_spill.unpinPartitionForRead(pinnedPartitionId);
			future.completeExceptionally(e);
		}
		return future;
	}


	@Override
	public CompletableFuture<Boolean> scheduleDeletion(BlockEntry block) {
		_spill.delete(block);
		_source.delete(block.getKey());
		return CompletableFuture.completedFuture(true);
	}

	@Override
	public void registerSourceLocation(BlockKey key, SourceBlockDescriptor descriptor) {
		_source.register(key, descriptor);
	}

	@Override
	public CompletableFuture<SourceReadResult> scheduleSourceRead(SourceReadRequest request) {
		return _source.scan(request, request.maxBytesInFlight);
	}

	@Override
	public CompletableFuture<SourceReadResult> continueSourceRead(SourceReadContinuation continuation,
		long maxBytesInFlight) {
		return _source.continueScan(continuation, maxBytesInFlight);
	}

	private long readAheadBudget(BlockEntry block) {
		OOCCache cache = _cache;
		if(cache == null || block.getSize() >= MIN_READ_BYTES)
			return 0;
		return cache.readAheadBudget();
	}

	private final class ReadTask implements Runnable {
		private final BlockEntry _block;
		private final OOCFuture<BlockEntry> _future;
		private final int _pinnedPartitionId;
		private final long _submittedAt;

		private ReadTask(BlockEntry block, OOCFuture<BlockEntry> future, int pinnedPartitionId) {
			_block = block;
			_future = future;
			_pinnedPartitionId = pinnedPartitionId;
			_submittedAt = DMLScript.OOC_STATISTICS ? System.nanoTime() : 0;
		}

		@Override
		public void run() {
			long startedAt = DMLScript.OOC_STATISTICS ? System.nanoTime() : 0;
			if(DMLScript.OOC_STATISTICS)
				Statistics.recordOOCReaderTaskStart(startedAt - _submittedAt, _readExec.getMaximumPoolSize(),
					_readExec.getQueue().size());
			try {
				long ioStart = DMLScript.OOC_LOG_EVENTS ? System.nanoTime() : 0;
				boolean sourceBacked = _source.contains(_block.getKey());
				long budget = sourceBacked ? readAheadBudget(_block) : 0;
				Object data = sourceBacked ? _source.read(_block, budget, _cache) : _spill.read(_block, budget, _cache);
				if(data != null)
					_block.setDataUnsafe(data);
				if(DMLScript.OOC_LOG_EVENTS)
					OOCEventLog.onDiskReadEvent(_readCallerId, ioStart, System.nanoTime(), _block.getSize());
				_future.complete(_block);
			}
			catch(Throwable e) {
				_future.completeExceptionally(e);
			}
			finally {
				if(DMLScript.OOC_STATISTICS)
					Statistics.recordOOCReaderTaskEnd(System.nanoTime() - startedAt);
				_spill.unpinPartitionForRead(_pinnedPartitionId);
			}
		}
	}
}
