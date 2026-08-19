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
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.runtime.ooc.stats.StreamTrace;
import org.apache.sysds.runtime.util.LocalFileUtils;
import org.apache.sysds.utils.Statistics;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ArrayBlockingQueue;
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
	private static final int MAX_DECLINED_OFFERS = 16;

	// Physical tile location is encoded as long
	// Max long value means that the tile hasn't been spilled
	// Layout: [partitionId(32 bytes), partitionOffset(32 bytes)]
	private static final long NONE = Long.MAX_VALUE;
	private static final int PARTITION_SHIFT = 32;
	private static final long PARTITION_MASK = (1L << 30) - 1;
	private static final long OFFSET_MASK = 0xFFFFFFFFL;

	private static final VarHandle PARTITIONS = MethodHandles.arrayElementVarHandle(PartitionFile[].class);

	private final String _spillDir;
	private final ThreadPoolExecutor _writeExec;
	private final ThreadPoolExecutor _deleteExec;
	private final CloseableQueue<Tuple2<BlockEntry, OOCFuture<Void>>>[] _q;
	private final AtomicInteger _partitionCounter = new AtomicInteger(0);
	private final Object _spillLock = new Object();
	private final AtomicLong _wCtr = new AtomicLong(0);
	private final AtomicBoolean _started = new AtomicBoolean(false);
	private final int _evictCallerId = OOCEventLog.registerCaller("write");
	private volatile PartitionFile[] _partitions = new PartitionFile[16];

	@SuppressWarnings("unchecked")
	SpillStore() {
		_spillDir = LocalFileUtils.getUniqueWorkingDir("ooc_stream");
		_writeExec = new ThreadPoolExecutor(WRITER_SIZE, WRITER_SIZE, 0L, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(100000));
		_deleteExec = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100000));
		_q = new CloseableQueue[WRITER_SIZE];
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

	void delete(BlockEntry block) {
		synchronized(_spillLock) {
			long location = block.getBackingLocation();
			if(notSpilled(location))
				return;
			block.setBackingLocation(NONE);
			releaseIfUnused(spillPartition(location));
		}
	}

	Object read(BlockEntry block, long readAheadBudget, OOCCache cache) {
		long location = block.getBackingLocation();
		if(notSpilled(location))
			throw new DMLRuntimeException("Failed to load spill location for: " + block.getKey());
		int partitionId = spillPartition(location);
		long offset = spillOffset(location);
		PartitionFile partition = partition(partitionId);
		if(partition == null)
			throw new DMLRuntimeException("Failed to load partition for: " + partitionId);

		try(RandomAccessFile raf = new RandomAccessFile(partitionPath(partitionId), "r")) {
			raf.seek(offset);
			OOCBufferedDataInputStream in = new OOCBufferedDataInputStream(raf);
			StreamTrace.spillRead(block.getKey().getStreamId(), block.getSize());
			long ioStart = DMLScript.OOC_STATISTICS ? System.nanoTime() : 0;
			SpillableObject obj = SpillableObjectRegistry.read(in);
			if(DMLScript.OOC_STATISTICS) {
				Statistics.incrementOOCLoadFromDisk();
				Statistics.accumulateOOCLoadFromDiskTime(System.nanoTime() - ioStart);
				Statistics.accumulateOOCLoadFromDiskBytes(block.getSize());
			}
			if(readAheadBudget > 0)
				readAhead(partition, partition.index.slotOf(offset), in, readAheadBudget, cache);
			return obj;
		}
		catch(ClosedByInterruptException ignored) {
			return null;
		}
		catch(IOException e) {
			throw new DMLRuntimeException(e);
		}
	}

	private void readAhead(PartitionFile partition, int slot, OOCBufferedDataInputStream in, long budget,
		OOCCache cache) {
		BlockLayoutIndex index = partition.index;
		int count = index.count();
		long bytes = 0;
		int declined = 0;
		for(int next = slot + 1; next < count && bytes < budget; next++) {
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
			if(cache.activate(BlockLayoutIndex.unpackKey(packedKey), obj))
				declined = 0;
			else if(++declined >= MAX_DECLINED_OFFERS)
				return;
		}
	}

	int pinPartitionForRead(BlockEntry block) {
		long location = block.getBackingLocation();
		if(notSpilled(location))
			return -1;
		int partitionId = spillPartition(location);
		synchronized(_spillLock) {
			PartitionFile partition = partition(partitionId);
			if(partition == null)
				return -1;
			partition.refCount++;
			return partitionId;
		}
	}

	void unpinPartitionForRead(int partitionId) {
		if(partitionId < 0)
			return;
		releaseIfUnused(partitionId);
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
		synchronized(_spillLock) {
			_partitions = new PartitionFile[0];
		}
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
			String filename = partitionPath(partitionId);
			PartitionFile partition = openPartition(partitionId);

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

					if(wrote > 0)
						StreamTrace.evictWrite(tpl._1().getKey().getStreamId(), wrote);
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

						if(wrote > 0)
							StreamTrace.evictWrite(tpl._1().getKey().getStreamId(), wrote);
						if(DMLScript.OOC_STATISTICS && wrote > 0) {
							Statistics.incrementOOCEvictionWrite();
							Statistics.accumulateOOCEvictionWriteTime(System.nanoTime() - ioStart);
						}

						if(DMLScript.OOC_LOG_EVENTS)
							OOCEventLog.onDiskWriteEvent(_evictCallerId, ioStart, System.nanoTime(), wrote);
					}
				}
			}
			catch(InterruptedException ex) {
				//writers are interrupted by a normal shutdown, so this is termination rather than a failure
				Thread.currentThread().interrupt();
				return;
			}
			catch(IOException ex) {
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
				releaseIfUnused(partitionId);
			}
		}
	}

	private long writeOut(PartitionFile partition, int partitionId, BlockEntry entry, OOCFuture<Void> future,
		OOCBufferedDataOutputStream dos, ConcurrentLinkedDeque<Tuple3<Long, Long, OOCFuture<Void>>> flushQueue)
		throws IOException {

		if(entry.getBackingLocation() != NONE) {
			future
				.completeExceptionally(new DMLRuntimeException("Duplicate OOC spill location for: " + entry.getKey()));
			return 0;
		}

		long offsetBefore = dos.getPosition();
		if(future.isDone())
			return 0;

		SpillableObject so = (SpillableObject) entry.getDataUnsafe(); // Get data without requiring pin
		if(so == null)
			return 0;
		if(!SpillableObjectRegistry.tryWrite(dos, so))
			return 0;

		long offsetAfter = dos.getPosition();
		if(future.isDone())
			return offsetAfter - offsetBefore;
		flushQueue.offer(new Tuple3<>(offsetBefore, offsetAfter, future));

		partition.index.append(offsetBefore, offsetAfter, BlockLayoutIndex.packKey(entry.getKey()));
		addLocation(entry, partitionId, offsetBefore);
		if(future.isDone()) {
			delete(entry);
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

	private void addLocation(BlockEntry entry, int partitionId, long offset) {
		synchronized(_spillLock) {
			if(entry.getBackingLocation() != NONE)
				return;
			PartitionFile partition = partition(partitionId);
			if(partition == null)
				return;
			partition.refCount++;
			entry.setBackingLocation(spill(partitionId, offset));
		}
	}

	private static long spill(int partitionId, long offset) {
		if(partitionId < 0 || partitionId > PARTITION_MASK)
			throw new IllegalArgumentException("Spill partition id out of range: " + partitionId);
		if(offset < 0 || offset > OFFSET_MASK)
			throw new IllegalArgumentException("Spill offset out of range: " + offset);
		return ((long) partitionId << PARTITION_SHIFT) | offset;
	}

	private static boolean notSpilled(long location) {
		return location == NONE;
	}

	private static int spillPartition(long location) {
		return (int) ((location >>> PARTITION_SHIFT) & PARTITION_MASK);
	}

	private static long spillOffset(long location) {
		return location & OFFSET_MASK;
	}

	private PartitionFile openPartition(int partitionId) {
		PartitionFile partition = new PartitionFile();
		partition.refCount = 1;
		synchronized(_spillLock) {
			PartitionFile[] partitions = _partitions;
			if(partitionId >= partitions.length) {
				int capacity = Math.max(partitions.length, 1);
				while(partitionId >= capacity)
					capacity <<= 1;
				partitions = new PartitionFile[capacity];
				System.arraycopy(_partitions, 0, partitions, 0, _partitions.length);
				_partitions = partitions;
			}
			PARTITIONS.setRelease(partitions, partitionId, partition);
		}
		return partition;
	}

	private void releaseIfUnused(int partitionId) {
		synchronized(_spillLock) {
			PartitionFile partition = partition(partitionId);
			if(partition == null || --partition.refCount != 0)
				return;
			PARTITIONS.setRelease(_partitions, partitionId, null);
			try {
				_deleteExec.execute(() -> LocalFileUtils.deleteFileIfExists(partitionPath(partitionId), true));
			}
			catch(RejectedExecutionException ignored) {
			}
		}
	}

	private PartitionFile partition(int partitionId) {
		PartitionFile[] partitions = _partitions;
		if(partitionId < 0 || partitionId >= partitions.length)
			return null;
		return (PartitionFile) PARTITIONS.getAcquire(partitions, partitionId);
	}

	private String partitionPath(int partitionId) {
		return _spillDir + "/stream_batch_part_" + partitionId;
	}

	private static final class PartitionFile {
		final BlockLayoutIndex index = new BlockLayoutIndex();
		int refCount;
	}
}
