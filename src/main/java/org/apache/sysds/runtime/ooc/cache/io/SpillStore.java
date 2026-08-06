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
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.runtime.util.LocalFileUtils;
import org.apache.sysds.utils.Statistics;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class SpillStore {
	private static final int WRITER_SIZE = 8;
	private static final long OVERFLOW = 8192 * 1024;
	private static final long MAX_PARTITION_SIZE = 8192 * 8192;
	private static final long IDLE_FLUSH_MS = 1;
	private static final int MAX_READ_AHEAD_COUNT = 64;
	private static final int MAX_DECLINED_OFFERS = 4;

	private final String _spillDir;
	private final ThreadPoolExecutor _writeExec;
	private final ThreadPoolExecutor _deleteExec;
	private final CloseableQueue<Tuple2<BlockEntry, OOCFuture<Void>>>[] _q;
	private final ConcurrentHashMap<BlockKey, SpillLocation> _locations = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Integer, PartitionFile> _partitions = new ConcurrentHashMap<>();
	private final AtomicInteger _partitionCounter = new AtomicInteger(0);
	private final Object _spillLock = new Object();
	private final AtomicLong _wCtr = new AtomicLong(0);
	private final AtomicBoolean _started = new AtomicBoolean(false);
	private final int _evictCallerId = OOCEventLog.registerCaller("write");

	@SuppressWarnings("unchecked")
	SpillStore() {
		_spillDir = LocalFileUtils.getUniqueWorkingDir("ooc_stream");
		_writeExec = new ThreadPoolExecutor(WRITER_SIZE, WRITER_SIZE, 0L, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(100000));
		_deleteExec = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100000));
		_q = new CloseableQueue[WRITER_SIZE];
	}

	boolean contains(BlockKey key) {
		return _locations.containsKey(key);
	}

	OOCFuture<Void> write(BlockEntry block) {
		start();
		OOCFuture<Void> future = new OOCFuture<>();
		try {
			long q = _wCtr.getAndAdd(block.getSize()) / OVERFLOW;
			int i = (int) (q % WRITER_SIZE);
			if(!_q[i].enqueueIfOpen(new Tuple2<>(block, future)))
				future.completeExceptionally(new DMLRuntimeException("OOC writer queue is closed"));
		}
		catch(InterruptedException ignored) {
			Thread.currentThread().interrupt();
			future.completeExceptionally(new DMLRuntimeException("Interrupted while scheduling OOC eviction"));
		}
		return future;
	}

	void delete(BlockKey key) {
		removeLocation(key);
	}

	/**
	 * Reads one block and, if {@code readAheadBudget} allows, keeps decoding the blocks that follow it in the same
	 * partition, offering each to the cache.
	 *
	 * @return the decoded object, or null if the read was interrupted
	 */
	Object read(BlockEntry block, long readAheadBudget, OOCCache cache) {
		SpillLocation location = _locations.get(block.getKey());
		if(location == null)
			throw new DMLRuntimeException("Failed to load spill location for: " + block.getKey());
		PartitionFile partition = _partitions.get(location.partitionId);
		if(partition == null)
			throw new DMLRuntimeException("Failed to load partition for: " + location.partitionId);

		try(RandomAccessFile raf = new RandomAccessFile(partition.filePath, "r")) {
			raf.seek(location.offset);
			OOCBufferedDataInputStream in = new OOCBufferedDataInputStream(raf);
			long ioStart = DMLScript.OOC_STATISTICS ? System.nanoTime() : 0;
			SpillableObject obj = SpillableObjectRegistry.read(in);
			if(DMLScript.OOC_STATISTICS) {
				Statistics.incrementOOCLoadFromDisk();
				Statistics.accumulateOOCLoadFromDiskTime(System.nanoTime() - ioStart);
				Statistics.accumulateOOCLoadFromDiskBytes(block.getSize());
			}
			if(readAheadBudget > 0)
				readAhead(partition, location.slot, in, readAheadBudget, cache);
			return obj;
		}
		catch(ClosedByInterruptException ignored) {
			return null;
		}
		catch(IOException e) {
			throw new DMLRuntimeException(e);
		}
	}

	/**
	 * Decodes the on-disk successors of the block at {@code slot} straight out of the already open stream. Stops as soon
	 * as the next record is not exactly where the layout index says it should be, which covers gaps left by abandoned
	 * writes, and stops early once the cache keeps declining offers.
	 */
	private void readAhead(PartitionFile partition, int slot, OOCBufferedDataInputStream in, long budget,
		OOCCache cache) {
		BlockLayoutIndex index = partition.index;
		int count = index.count();
		long bytes = 0;
		int taken = 0;
		int declined = 0;
		for(int next = slot + 1; next < count && bytes < budget && taken < MAX_READ_AHEAD_COUNT; next++) {
			long packedKey = index.keyAt(next);
			long start = index.startAt(next);
			if(packedKey == BlockLayoutIndex.NO_KEY || start != in.getPosition())
				return;
			SpillableObject obj;
			long ioStart = DMLScript.OOC_LOG_EVENTS ? System.nanoTime() : 0;
			try {
				obj = SpillableObjectRegistry.read(in);
			}
			catch(IOException ignored) {
				return;
			}
			bytes += index.endAt(next) - start;
			if(DMLScript.OOC_LOG_EVENTS)
				OOCEventLog.onDiskReadEvent(_evictCallerId, ioStart, System.nanoTime(), index.endAt(next) - start);
			if(cache.activate(BlockLayoutIndex.unpackKey(packedKey), obj)) {
				taken++;
				declined = 0;
			}
			else if(++declined >= MAX_DECLINED_OFFERS)
				return;
		}
	}

	/** Pins the partition holding {@code key} so it survives until the scheduled read runs. -1 if there is none. */
	int pinPartitionForRead(BlockKey key) {
		synchronized(_spillLock) {
			SpillLocation location = _locations.get(key);
			if(location == null)
				return -1;
			PartitionFile partition = _partitions.get(location.partitionId);
			if(partition == null)
				return -1;
			partition.incrementRefCount();
			return location.partitionId;
		}
	}

	void unpinPartitionForRead(int partitionId) {
		if(partitionId < 0)
			return;
		synchronized(_spillLock) {
			PartitionFile partition = _partitions.get(partitionId);
			if(partition != null)
				releaseIfUnused(partitionId, partition);
		}
	}

	void shutdown() {
		boolean started = _started.get();
		if(started) {
			try {
				for(int i = 0; i < WRITER_SIZE; i++)
					if(_q[i] != null)
						_q[i].close();
			}
			catch(InterruptedException ignored) {
			}
		}
		_writeExec.getQueue().clear();
		_writeExec.shutdownNow();
		_deleteExec.getQueue().clear();
		_deleteExec.shutdownNow();
		_locations.clear();
		_partitions.clear();
		if(started)
			LocalFileUtils.deleteFileIfExists(_spillDir);
	}

	private synchronized void start() {
		if(_started.compareAndSet(false, true)) {
			for(int i = 0; i < WRITER_SIZE; i++) {
				final int finalIdx = i;
				_q[i] = new CloseableQueue<>();
				_writeExec.submit(() -> writeLoop(_q[finalIdx]));
			}
		}
	}

	private void writeLoop(CloseableQueue<Tuple2<BlockEntry, OOCFuture<Void>>> q) {
		long byteCtr = 0;

		while(!q.isFinished()) {
			int partitionId = _partitionCounter.getAndIncrement();
			LocalFileUtils.createLocalFileIfNotExist(_spillDir);
			String filename = _spillDir + "/stream_batch_part_" + partitionId;

			PartitionFile partition = new PartitionFile(filename);
			_partitions.put(partitionId, partition);
			partition.incrementRefCount(); // Writer pin; released when the partition closes

			FileOutputStream fos = null;
			OOCBufferedDataOutputStream dos = null;
			ConcurrentLinkedDeque<Tuple3<Long, Long, OOCFuture<Void>>> waitingForFlush = null;

			try {
				fos = new FileOutputStream(filename);
				dos = new OOCBufferedDataOutputStream(fos);

				Tuple2<BlockEntry, OOCFuture<Void>> tpl;
				waitingForFlush = new ConcurrentLinkedDeque<>();
				boolean closePartition = false;

				while(!q.isFinished()) {
					tpl = q.poll(IDLE_FLUSH_MS, TimeUnit.MILLISECONDS);
					if(tpl == null) {
						flushReadable(dos, waitingForFlush);
						continue;
					}
					long ioStart = DMLScript.OOC_STATISTICS || DMLScript.OOC_LOG_EVENTS ? System.nanoTime() : 0;
					long wrote = writeOut(partition, partitionId, tpl._1(), tpl._2(), dos, waitingForFlush);

					if(DMLScript.OOC_STATISTICS && wrote > 0) {
						Statistics.incrementOOCEvictionWrite();
						Statistics.accumulateOOCEvictionWriteTime(System.nanoTime() - ioStart);
						Statistics.accumulateOOCEvictionWriteBytes(wrote);
					}

					byteCtr += wrote;
					if(byteCtr >= MAX_PARTITION_SIZE) {
						closePartition = true;
						byteCtr = 0;
						break;
					}

					if(DMLScript.OOC_LOG_EVENTS)
						OOCEventLog.onDiskWriteEvent(_evictCallerId, ioStart, System.nanoTime(), wrote);
				}

				if(!closePartition && q.close()) {
					while((tpl = q.take()) != null) {
						long ioStart = DMLScript.OOC_STATISTICS ? System.nanoTime() : 0;
						long wrote = writeOut(partition, partitionId, tpl._1(), tpl._2(), dos, waitingForFlush);
						byteCtr += wrote;

						if(DMLScript.OOC_STATISTICS && wrote > 0) {
							Statistics.incrementOOCEvictionWrite();
							Statistics.accumulateOOCEvictionWriteTime(System.nanoTime() - ioStart);
						}

						if(DMLScript.OOC_LOG_EVENTS)
							OOCEventLog.onDiskWriteEvent(_evictCallerId, ioStart, System.nanoTime(), wrote);
					}
				}
			}
			catch(IOException | InterruptedException ex) {
				ex.printStackTrace();
				throw new DMLRuntimeException(ex);
			}
			catch(Exception ignored) {
			}
			finally {
				IOUtilFunctions.closeSilently(dos);
				IOUtilFunctions.closeSilently(fos);
				if(waitingForFlush != null)
					flushQueue(Long.MAX_VALUE, waitingForFlush);
				releasePartitionWriter(partitionId);
			}
		}
	}

	private long writeOut(PartitionFile partition, int partitionId, BlockEntry entry, OOCFuture<Void> future,
		OOCBufferedDataOutputStream dos, ConcurrentLinkedDeque<Tuple3<Long, Long, OOCFuture<Void>>> flushQueue)
		throws IOException {

		BlockKey key = entry.getKey();
		if(_locations.containsKey(key)) {
			future.completeExceptionally(new DMLRuntimeException("Duplicate OOC spill location for: " + key));
			return 0;
		}

		long offsetBefore = dos.getPosition();
		if(future.isDone())
			return 0;

		SpillableObject so = (SpillableObject) entry.getDataUnsafe(); // Get data without requiring pin
		if(so == null)
			return 0;
		// A failed write leaves an undecodable range behind. It gets no index slot, so read-ahead stops there
		// instead of trying to decode across it.
		if(!SpillableObjectRegistry.tryWrite(dos, so))
			return 0;

		long offsetAfter = dos.getPosition();
		if(future.isDone())
			return offsetAfter - offsetBefore;
		flushQueue.offer(new Tuple3<>(offsetBefore, offsetAfter, future));

		int slot = partition.index.append(offsetBefore, offsetAfter, BlockLayoutIndex.packKey(key));
		addLocation(key, new SpillLocation(partitionId, offsetBefore, slot));
		if(future.isDone()) {
			removeLocation(key);
			return offsetAfter - offsetBefore;
		}
		flushQueue(dos.getFlushedPosition(), flushQueue);

		return offsetAfter - offsetBefore;
	}

	private void flushQueue(long offset, ConcurrentLinkedDeque<Tuple3<Long, Long, OOCFuture<Void>>> flushQueue) {
		Tuple3<Long, Long, OOCFuture<Void>> tmp;
		while((tmp = flushQueue.peek()) != null && tmp._2() <= offset) {
			flushQueue.poll();
			tmp._3().complete(null);
		}
	}

	private void flushReadable(OOCBufferedDataOutputStream dos,
		ConcurrentLinkedDeque<Tuple3<Long, Long, OOCFuture<Void>>> flushQueue) throws IOException {
		if(dos == null || flushQueue.isEmpty())
			return;
		dos.flush();
		flushQueue(dos.getFlushedPosition(), flushQueue);
	}

	private void addLocation(BlockKey key, SpillLocation location) {
		synchronized(_spillLock) {
			if(_locations.putIfAbsent(key, location) == null) {
				PartitionFile partition = _partitions.get(location.partitionId);
				if(partition != null)
					partition.incrementRefCount();
			}
		}
	}

	private void removeLocation(BlockKey key) {
		synchronized(_spillLock) {
			SpillLocation location = _locations.remove(key);
			if(location == null)
				return;
			PartitionFile partition = _partitions.get(location.partitionId);
			if(partition != null)
				releaseIfUnused(location.partitionId, partition);
		}
	}

	private void releasePartitionWriter(int partitionId) {
		synchronized(_spillLock) {
			PartitionFile partition = _partitions.get(partitionId);
			if(partition != null)
				releaseIfUnused(partitionId, partition);
		}
	}

	private void releaseIfUnused(int partitionId, PartitionFile partition) {
		if(partition.decrementRefCount() != 0 || !_partitions.remove(partitionId, partition))
			return;
		try {
			_deleteExec.execute(() -> LocalFileUtils.deleteFileIfExists(partition.filePath, true));
		}
		catch(RejectedExecutionException ignored) {
		}
	}

	/** Where a block lives: which partition, at which byte offset, and which slot of that partition's layout index. */
	private record SpillLocation(int partitionId, long offset, int slot) {
	}

	private static final class PartitionFile {
		final String filePath;
		final BlockLayoutIndex index;
		private final AtomicInteger refCount;

		PartitionFile(String filePath) {
			this.filePath = filePath;
			this.index = new BlockLayoutIndex();
			this.refCount = new AtomicInteger(0);
		}

		int incrementRefCount() {
			return refCount.incrementAndGet();
		}

		int decrementRefCount() {
			return refCount.decrementAndGet();
		}
	}
}
