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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.util.FastBufferedDataOutputStream;
import org.apache.sysds.runtime.util.LocalFileUtils;
import org.apache.sysds.utils.Statistics;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Eviction Manager for the Out-Of-Core stream cache
 * This is the base implementation for LRU, FIFO
 *
 * Design choice 1: Pure JVM-memory cache
 * What: Store MatrixBlock objects in a synchronized in-memory cache
 *   (Map + Deque for LRU/FIFO). Spill to disk by serializing MatrixBlock
 *   only when evicting.
 * Pros: Simple to implement; no off-heap management; easy to debug;
 *   no serialization race since you serialize only when evicting;
 *   fast cache hits (direct object access).
 * Cons: Heap usage counted roughly via serialized-size estimate — actual
 *   JVM object overhead not accounted; risk of GC pressure and OOM if
 *   estimates are off or if many small objects cause fragmentation;
 *   eviction may be more expensive (serialize on eviction).
 * <p>
 * Design choice 2:
 * <p>
 * This manager runtime memory management by caching serialized
 * ByteBuffers and spilling them to disk when needed.
 * <p>
 * * core function: Caches ByteBuffers (off-heap/direct) and
 * spills them to disk
 * * Eviction: Evicts a ByteBuffer by writing its contents to a file
 * * Granularity: Evicts one IndexedMatrixValue block at a time
 * * Data replay: get() will always return the data either from memory or
 *   by falling back to the disk
 * * Memory: Since the datablocks are off-heap (in ByteBuffer) or disk,
 *   there won't be OOM.
 *
 * Pros: Avoids heap OOM by keeping large data off-heap; predictable
 *   memory usage; good for very large blocks.
 * Cons: More complex synchronization; need robust off-heap allocator/free;
 *   must ensure serialization finishes before adding to queue or make evict
 *   wait on serialization; careful with native memory leaks.
 */
public class OOCEvictionManager {

	// Configuration: OOC buffer limit as percentage of heap
	private static final double OOC_BUFFER_PERCENTAGE = 0.10; // 15% of heap
	private static final double OOC_BUFFER_PERCENTAGE_HARD = 0.15;

	private static final double PARTITION_EVICTION_SIZE = 64 * 1024 * 1024; // 64 MB

	// Memory limit for ByteBuffers
	private static long _limit; // When spilling will be triggered
	private static long _hardLimit; // When cache puts and disk loads become blocking
	private static final AtomicLong _size = new AtomicLong(0);
	private static final int MAX_BATCH_BLOCKS = 100;
	private static final int MIN_BATCH_BLOCKS = 10;

	// Cache structures: map key -> MatrixBlock and eviction deque (head=oldest block)
	private static LinkedHashMap<String, BlockEntry> _cache = new LinkedHashMap<>();

	// Spill related structures
	private static ConcurrentHashMap<String, spillLocation> _spillLocations =  new ConcurrentHashMap<>();
	private static ConcurrentHashMap<Integer, partitionFile> _partitions = new ConcurrentHashMap<>();
	private static final AtomicInteger _partitionCounter = new AtomicInteger(0);

	// Track which partitions belong to which stream (for cleanup)
	private static final ConcurrentHashMap<Long, Set<String>> _streamPartitions = new ConcurrentHashMap<>();

	// For read scheduling
	private static final int _queueCapacity = 100000;
	private static ThreadPoolExecutor _readExec = new ThreadPoolExecutor(
		8,
		8,
		0L,
		TimeUnit.MILLISECONDS,
		new ArrayBlockingQueue<>(_queueCapacity),
		new ThreadPoolExecutor.CallerRunsPolicy());
	private static ConcurrentHashMap<String, LinkedList<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>>> _readRequests = new ConcurrentHashMap<>();
	private static ThreadPoolExecutor _writeExec = new ThreadPoolExecutor(
		5,
		5,
		0L,
		TimeUnit.MILLISECONDS,
		new ArrayBlockingQueue<>(_queueCapacity),
		new ThreadPoolExecutor.CallerRunsPolicy());
	private static ThreadPoolExecutor _prepExec = new ThreadPoolExecutor(
		1,
		1,
		0L,
		TimeUnit.MILLISECONDS,
		new ArrayBlockingQueue<>(1),
		new ThreadPoolExecutor.DiscardPolicy());
	private static final AtomicBoolean _prepRunning = new AtomicBoolean(false);
	private static final AtomicInteger _activeWriters = new AtomicInteger(0);
	private static final ReentrantLock _memLock = new ReentrantLock();
	private static final Condition _belowHardLimit = _memLock.newCondition();


	// Cache level lock
	private static final Object _cacheLock = new Object();
	
	// Spill directory for evicted blocks
	private static String _spillDir;

	public enum RPolicy {
		FIFO, LRU
	}
	private static RPolicy _policy = RPolicy.FIFO;

	private enum BlockState {
		HOT, // In-memory
		EVICTING, // Being written to disk (transition state)
		COLD // On disk
	}

	private static class spillLocation {
		// structure of spillLocation: file, offset
		final int partitionId;
		final long offset;

		spillLocation(int partitionId, long offset) {

			this.partitionId = partitionId;
			this.offset = offset;
		}
	}

	private static class partitionFile {
		final String filePath;
		//final long streamId;


		private partitionFile(String filePath, long streamId) {
			this.filePath = filePath;
			//this.streamId = streamId;
		}
	}

	// Per-block state container with own lock.
	private static class BlockEntry {
		private final ReentrantLock lock = new ReentrantLock();
		private final Condition stateUpdate = lock.newCondition();

		private BlockState state = BlockState.HOT;
		private IndexedMatrixValue value;
		private final long streamId;
		//private final int blockId;
		private final long size;

		BlockEntry(IndexedMatrixValue value, long streamId, int blockId, long size) {
			this.value = value;
			this.streamId = streamId;
			//this.blockId = blockId;
			this.size = size;
		}
	}

	static {
		_limit = (long)(Runtime.getRuntime().maxMemory() * OOC_BUFFER_PERCENTAGE); // e.g., 20% of heap
		_hardLimit = (long)(Runtime.getRuntime().maxMemory() * OOC_BUFFER_PERCENTAGE_HARD);
		_size.set(0);
		_spillDir = LocalFileUtils.getUniqueWorkingDir("ooc_stream");
		LocalFileUtils.createLocalFileIfNotExist(_spillDir);
	}

	public static void reset() {
		if (DMLScript.STATISTICS) {
			System.out.println(Statistics.displayOOCEvictionStats());
			Statistics.resetOOCEvictionStats();
		}
		TeeOOCInstruction.reset();
		if (!_cache.isEmpty()) {
			System.err.println("There are dangling elements in the OOC Eviction cache: " + _cache.size());
		}
		_size.set(0);
		_cache.clear();
		_spillLocations.clear();
		_partitions.clear();
		_partitionCounter.set(0);
		_streamPartitions.clear();
		_readRequests.clear();

		if (_readExec != null) {
			_readExec.getQueue().clear();
			_readExec.shutdownNow();
			_readExec = null;
		}
		if (_writeExec != null) {
			_writeExec.getQueue().clear();
			_writeExec.shutdownNow();
			_writeExec = null;
		}
		if (_prepExec != null) {
			_prepExec.getQueue().clear();
			_prepExec.shutdownNow();
			_prepExec = null;
		}
		// TODO delete spill dir
	}

	public static ThreadPoolExecutor getWriteExec() {
		if (_writeExec == null) {
			_writeExec = new ThreadPoolExecutor(
				10,
				10,
				0L,
				TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(_queueCapacity),
				new ThreadPoolExecutor.CallerRunsPolicy());
		}
		return _writeExec;
	}
	public static ThreadPoolExecutor getPrepExec() {
		if (_prepExec == null) {
			_prepExec = new ThreadPoolExecutor(
				1,
				1,
				0L,
				TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(1),
				new ThreadPoolExecutor.DiscardPolicy());
		}
		return _prepExec;
	}

	public static ThreadPoolExecutor getReadExec() {
		if (_readExec == null) {
			_readExec = new ThreadPoolExecutor(
				10,
				10,
				0L,
				TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(_queueCapacity),
				new ThreadPoolExecutor.CallerRunsPolicy());
		}
		return _readExec;
	}

	/**
	 * Removes a block from the cache without setting its data to null.
	 */
	public static void forget(long streamId, int blockId) {
		BlockEntry e;
		synchronized (_cacheLock) {
			e = _cache.remove(streamId + "_" + blockId);
		}

		if (e != null) {
			e.lock.lock();
			try {
				if (e.state == BlockState.HOT) {
					_size.addAndGet(-e.size);
					signalIfBelowHardLimit();
				}
			} finally {
				e.lock.unlock();
			}
			//System.out.println("Removed block " + streamId + "_" + blockId + " from cache (idx: " + (e.value != null ? e.value.getIndexes() : "?") + ")");
		}
	}

	/**
	 * Store a block in the OOC cache (serialize once)
	 */
	public static void put(long streamId, int blockId, IndexedMatrixValue value) {
		awaitBelowHardLimit();

		if (DMLScript.STATISTICS) {
			Statistics.incrementOOCEvictionPut();
		}

		MatrixBlock mb = (MatrixBlock) value.getValue();
		long size = estimateSerializedSize(mb);
		String key = streamId + "_" + blockId;

		BlockEntry newEntry = new BlockEntry(value, streamId, blockId, size);
		BlockEntry old;
		synchronized (_cacheLock) {
			old = _cache.put(key, newEntry); // remove old value, put new value
		}

		// Handle replacement with a new lock
		if (old != null) {
			old.lock.lock();
			try {
				if (old.state == BlockState.HOT) {
					_size.addAndGet(-old.size); // read and update size in atomic operation
					signalIfBelowHardLimit();
				}
			} finally {
				old.lock.unlock();
			}
		}

		_size.addAndGet(size);
		//make room if needed

		invokeWriterIfNecessary();
	}

	private static void invokeWriterIfNecessary() {
		if (_size.get() > _limit) {
			startEvictionPreparation();
		}
	}

	private static void startEvictionPreparation() {
		if (_prepRunning.compareAndSet(false, true)) {
			getPrepExec().submit(() -> {
				try {
					runEvictionPreparation();
				}
				finally {
					_prepRunning.set(false);
				}
			});
		}
	}

	private static void runEvictionPreparation() {
		while (_size.get() > _limit) {
			List<Map.Entry<String, BlockEntry>> batch = collectEvictionBatch();
			if (batch.isEmpty()) {
				break;
			}

			boolean shouldLaunch = batch.size() >= MIN_BATCH_BLOCKS || _size.get() >= _hardLimit;
			if (!shouldLaunch) {
				// Return entries to HOT to avoid leaving them in transient state.
				revertToHot(batch);
				break;
			}

			submitEvictionBatch(batch);
		}
	}

	private static List<Map.Entry<String, BlockEntry>> collectEvictionBatch() {
		//sanityCheckSize();
		long size = _size.get();
		long targetFreedSize = Math.max(size - _limit, (long) PARTITION_EVICTION_SIZE);
		List<Map.Entry<String, BlockEntry>> candidates = new ArrayList<>(MAX_BATCH_BLOCKS);
		long totalPlanned = 0;
		int lockBusy = 0;

		synchronized (_cacheLock) {
			Iterator<Map.Entry<String, BlockEntry>> iter = _cache.entrySet().iterator();

			while (iter.hasNext() && candidates.size() < MAX_BATCH_BLOCKS && totalPlanned < targetFreedSize) {
				Map.Entry<String, BlockEntry> e = iter.next();
				BlockEntry entry = e.getValue();

				if (entry.lock.tryLock()) {
					try {
						if (entry.state == BlockState.HOT) {
							entry.state = BlockState.EVICTING;
							candidates.add(e);
							totalPlanned += entry.size;
						}
					} finally {
						entry.lock.unlock();
					}
				}
				else {
					lockBusy++;
				}
			}
		}

		if (candidates.isEmpty()) {
			System.err.println("Eviction prep found no HOT candidates; size=" + size
				+ " limit=" + _limit + " hardLimit=" + _hardLimit
				+ " lockBusy=" + lockBusy);
		}
		return candidates;
	}

	private static void submitEvictionBatch(List<Map.Entry<String, BlockEntry>> batch) {
		_activeWriters.incrementAndGet();
		getWriteExec().submit(() -> {
			try {
				evict(batch);
			}
			finally {
				if (_activeWriters.decrementAndGet() == 0)
					signalWriterIdle();
			}
		});
	}

	private static void revertToHot(List<Map.Entry<String, BlockEntry>> batch) {
		for (Map.Entry<String, BlockEntry> e : batch) {
			BlockEntry be = e.getValue();
			be.lock.lock();
			try {
				if (be.state == BlockState.EVICTING) {
					be.state = BlockState.HOT;
					be.stateUpdate.signalAll();
				}
			} finally {
				be.lock.unlock();
			}
		}
	}

	public static void requestBlock(long streamId, int blockId, Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> consumer) {
		IndexedMatrixValue val = tryGet(streamId, blockId);

		if (val != null) {
			consumer.accept(new OOCStream.QueueCallback<>(val, null));
			return;
		}

		final String key = streamId + "_" + blockId;
		final MutableBoolean alreadyQueued = new MutableBoolean(true);

		_readRequests.compute(key, (k, v) -> {
			if (v == null) {
				v = new LinkedList<>();
				alreadyQueued.setValue(false);
			}
			v.add(consumer);
			return v;
		});

		if (!alreadyQueued.getValue()) {
			getReadExec().submit(() -> {
				IndexedMatrixValue mVal = loadFromDisk(streamId, blockId);
				LinkedList<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> callbacks = _readRequests.remove(key);
				OOCStream.QueueCallback<IndexedMatrixValue> v = new OOCStream.QueueCallback<>(mVal, null);

				if (callbacks.size() > 1)
					System.out.println("COULD REUSE: " + callbacks.size());

				for(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> callback : callbacks)
					callback.accept(v);
			});
		}
	}

	/**
	 * Get a block if it is hot (available in memory). Otherwise return null.
	 */
	public static IndexedMatrixValue tryGet(long streamId, int blockId) {
		String key = streamId + "_" + blockId;
		BlockEntry imv;

		if (DMLScript.STATISTICS) {
			Statistics.incrementOOCEvictionGet();
		}

		synchronized (_cacheLock) {
			imv = _cache.get(key);
			//System.err.println( "value of imv: " + imv);
			if (imv != null && _policy == RPolicy.LRU) {
				_cache.remove(key);
				_cache.put(key, imv); //add last semantic
			}
		}

		if (imv == null)
			return null; // Block not available

		// use lock and check state
		imv.lock.lock();
		try {
			// 1. wait for eviction to complete
			while (imv.state == BlockState.EVICTING) {
				try {
					imv.stateUpdate.await();
				} catch (InterruptedException e) {
					throw new DMLRuntimeException(e);
				}
			}

			// 2. check if the block is in HOT
			if (imv.state == BlockState.HOT) {
				return imv.value;
			}

		} finally {
			imv.lock.unlock();
		}

		return null;
	}

	/**
	 * Get a block from the OOC cache (deserialize on read)
	 */
	public static IndexedMatrixValue get(long streamId, int blockId) {
		IndexedMatrixValue val = tryGet(streamId, blockId);

		if (val != null)
			return val;

		// restore, since the block is COLD
		return loadFromDisk(streamId, blockId);
	}

	/**
	 * Evict ByteBuffers to disk
	 */
	private static void evict(List<Map.Entry<String, BlockEntry>> candidates) {
		if (candidates == null || candidates.isEmpty())
			return;

		long currentSize = _size.get();
		long totalFreedSize = 0;

		// --- 1. WRITE PHASE ---
		// write to partition file
		// 1. generate a new ID for the present "partition" (file)
		int partitionId = _partitionCounter.getAndIncrement();

		// Spill to disk
		String filename = _spillDir + "/stream_batch_part_" + partitionId;
		File spillDirFile = new File(_spillDir);
		if (!spillDirFile.exists()) {
			spillDirFile.mkdirs();
		}

		long ioStart = DMLScript.STATISTICS ? System.nanoTime() : 0;

		// 2. create the partition file metadata
		partitionFile partFile = new partitionFile(filename, 0);
		_partitions.put(partitionId, partFile);

		FileOutputStream fos = null;
		FastBufferedDataOutputStream dos = null;
		try {
			fos = new FileOutputStream(filename);
			dos = new FastBufferedDataOutputStream(fos);


			// loop over the list of blocks we collected
			for (Map.Entry<String,BlockEntry> tmp : candidates) {
				final String key = tmp.getKey();
				BlockEntry entry = tmp.getValue();
				boolean alreadySpilled = _spillLocations.containsKey(key);
				boolean wrote = false;

				try {
					if (!alreadySpilled) {
						// 1. get the current file position. this is the offset.
						// flush any buffered data to the file
						dos.flush();
						long offset = fos.getChannel().position();

						// 2. write indexes and block
						entry.value.getIndexes().write(dos); // write Indexes
						entry.value.getValue().write(dos);
						wrote = true;
						//System.out.println("written, partition id: " + _partitions.get(partitionId) + ", offset: " + offset);

						// 3. create the spillLocation
						spillLocation sloc = new spillLocation(partitionId, offset);
						_spillLocations.put(key, sloc);

						// 4. track file for cleanup
						_streamPartitions
										.computeIfAbsent(entry.streamId, k -> ConcurrentHashMap.newKeySet())
										.add(filename);
					}

					// 5. change state to COLD
					entry.lock.lock();
					long entrySize = entry.size;
					try {
						entry.value = null; // only release ref, don't mutate object
						entry.state = BlockState.COLD; // set state to cold, since writing to disk
						entry.stateUpdate.signalAll(); // wake up any "get()" threads
					} finally {
						entry.lock.unlock();
					}

					synchronized (_cacheLock) {
						_cache.put(key, entry); // add last semantic
					}

					if (DMLScript.STATISTICS) {
						if (wrote)
							Statistics.incrementOOCEvictionWrite();
					}

					long newSize = _size.addAndGet(-entrySize);
					totalFreedSize += entrySize;
					if (newSize < _hardLimit)
						signalIfBelowHardLimit();
				}
				catch (Throwable t) {
					// Recovery: return to HOT so we don't leave stranded EVICTING entries.
					entry.lock.lock();
					try {
						if (entry.state == BlockState.EVICTING) {
							entry.state = BlockState.HOT;
							entry.stateUpdate.signalAll();
						}
					} finally {
						entry.lock.unlock();
					}
					throw t;
				}
			}
		}
		catch(IOException ex) {
			throw new DMLRuntimeException(ex);
		} finally {
			if (DMLScript.STATISTICS)
				Statistics.accumulateOOCEvictionWriteTime(System.nanoTime() - ioStart);
			IOUtilFunctions.closeSilently(dos);
			IOUtilFunctions.closeSilently(fos);
			// Make sure waiters re-check even if no progress.
			signalIfBelowHardLimit();
		}

		// --- 3. ACCOUNTING PHASE ---
		if (DMLScript.STATISTICS) {
			System.err.println("Eviction batch summary: candidates=" + candidates.size()
				+ " freedBytes=" + totalFreedSize
				+ " sizeBefore=" + currentSize + " sizeAfter=" + _size.get());
		}
	}

	/**
	 * Load block from spill file
	 */
	private static IndexedMatrixValue loadFromDisk(long streamId, int blockId) {
		awaitBelowHardLimit();

		String key = streamId + "_" + blockId;

		long ioDuration = 0;
		// 1. find the blocks address (spill location)
		spillLocation sloc = _spillLocations.get(key);
		if (sloc == null) {
			throw new DMLRuntimeException("Failed to load spill location for: " + key);
		}

		partitionFile partFile = _partitions.get(sloc.partitionId);
		if (partFile == null) {
			throw new DMLRuntimeException("Failed to load partition for: " + sloc.partitionId);
		}

		String filename = partFile.filePath;
		//System.out.println("Reading from disk (" + Thread.currentThread().getId() + "): " + filename);

		// Create an empty object to read data into.
		MatrixIndexes ix = new  MatrixIndexes();
		MatrixBlock mb = new  MatrixBlock();

		try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
			raf.seek(sloc.offset);

			try {
				DataInputStream dis = new DataInputStream(
					new BufferedInputStream(Channels.newInputStream(raf.getChannel())));
				long ioStart = DMLScript.STATISTICS ? System.nanoTime() : 0;
				ix.readFields(dis); // 1. Read Indexes
				mb.readFields(dis); // 2. Read Block
				if (DMLScript.STATISTICS)
					ioDuration = System.nanoTime() - ioStart;
			} catch (IOException ex) {
				throw new DMLRuntimeException("Failed to load block " + key + " from " + filename, ex);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		// Read from disk and put into original indexed matrix value
		BlockEntry imvCacheEntry;
		synchronized (_cacheLock) {
			imvCacheEntry = _cache.get(key);
		}

		// 2. Check if it's null (the bug you helped fix before)
		if(imvCacheEntry == null) {
			throw new DMLRuntimeException("Block entry " + key + " was not in cache during load.");
		}

		imvCacheEntry.lock.lock();
		try {
			if (imvCacheEntry.state == BlockState.COLD) {
				imvCacheEntry.value = new IndexedMatrixValue(ix, mb);
				imvCacheEntry.state = BlockState.HOT;
				_size.addAndGet(imvCacheEntry.size);

				synchronized (_cacheLock) {
					_cache.remove(key);
					_cache.put(key, imvCacheEntry);
				}
			}

//			evict(); // when we add the block, we shall check for limit.
		} finally {
			imvCacheEntry.lock.unlock();
		}

		invokeWriterIfNecessary();

		if (DMLScript.STATISTICS) {
			Statistics.incrementOOCLoadFromDisk();
			Statistics.accumulateOOCLoadFromDiskTime(ioDuration);
		}

		return imvCacheEntry.value;
	}

	private static long estimateSerializedSize(MatrixBlock mb) {
		return mb.getExactSerializedSize();
	}

	private static void signalWriterIdle() {
		signalIfBelowHardLimit();
	}

	private static void signalIfBelowHardLimit() {
		_memLock.lock();
		try {
			if (_size.get() < _hardLimit)
				_belowHardLimit.signalAll();
		} finally {
			_memLock.unlock();
		}
	}

	private static void sanityCheckSize() {
		long accounted = _size.get();
		long recomputed = 0;

		synchronized (_cacheLock) {
			for (Map.Entry<String, BlockEntry> e : _cache.entrySet()) {
				BlockEntry be = e.getValue();
				// Count only blocks that should occupy memory
				if (be.state == BlockState.HOT || be.state == BlockState.EVICTING)
					recomputed += be.size;
			}
		}

		long diff = Math.abs(accounted - recomputed);
		// Log significant drift to help diagnose stuck hard-limit states.
		if (diff > (5 * 1024 * 1024)) { // 5MB tolerance
			System.err.println("Sanity check: accounted size=" + accounted
				+ " recomputed=" + recomputed + " diff=" + diff
				+ " limit=" + _limit + " hardLimit=" + _hardLimit);
		}
	}

	private static void awaitBelowHardLimit() {
		for (;;) {
			if (_size.get() < _hardLimit)
				return;

			System.err.println("Hard limit reached: " + _size.get());
			invokeWriterIfNecessary();

			_memLock.lock();
			try {
				while (_size.get() >= _hardLimit) {
					_belowHardLimit.await();
				}
				return;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new DMLRuntimeException(e);
			} finally {
				_memLock.unlock();
			}
		}
	}
	
	@SuppressWarnings("unused")
	private static Map.Entry<String, BlockEntry> removeFirstFromCache() {
		synchronized (_cacheLock) {

			if (_cache.isEmpty()) {
				return null;
			}
			//move iterator to first entry
			Iterator<Map.Entry<String, BlockEntry>> iter = _cache.entrySet().iterator();
			Map.Entry<String, BlockEntry> entry = iter.next();

			//remove current iterator entry
			iter.remove();

			return entry;
		}
	}
}
