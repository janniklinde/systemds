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
	private static final double OOC_BUFFER_PERCENTAGE = 0.01; // 15% of heap
	private static final double OOC_BUFFER_PERCENTAGE_IN_MEMORY = 0.01; // keep blocks resident until this threshold
	private static final double OOC_BUFFER_PERCENTAGE_HARD = 0.015;

	private static final double PARTITION_EVICTION_SIZE = 64 * 1024 * 1024; // 64 MB

	// Memory limit for ByteBuffers
	private static final ResourceManager _resourceManager;
	private static long _limit; // When spilling will be triggered
	private static long _inMemoryLimit; // Up to this limit, keep spilled blocks resident
	private static long _hardLimit; // When cache puts and disk loads become blocking
	/*private static final AtomicLong _residentSize = new AtomicLong(0); // HOT + WARM
	private static final AtomicLong _unspilledSize = new AtomicLong(0);*/ // HOT without spill location
	private static final AtomicInteger _pinCount = new AtomicInteger(0);
	private static final int PIN_WARN_THRESHOLD = 2;
	private static final int MAX_BATCH_BLOCKS = 150;
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
		1,
		1,
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
		WARM, // Spilled to disk but still resident in memory
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
		private int pins = 0;

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
		_inMemoryLimit = (long)(Runtime.getRuntime().maxMemory() * OOC_BUFFER_PERCENTAGE_IN_MEMORY);
		_hardLimit = (long)(Runtime.getRuntime().maxMemory() * OOC_BUFFER_PERCENTAGE_HARD);
		_resourceManager = new ResourceManager(_limit, _inMemoryLimit, _hardLimit, OOCEvictionManager::startEvictionPreparation);
		//_residentSize.set(0);
		//_unspilledSize.set(0);
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
		//_residentSize.set(0);
		//_unspilledSize.set(0);
		_resourceManager.reset();
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
				if (e.pins == 0 && (e.state == BlockState.HOT || e.state == BlockState.WARM)) {
					// TODO Warm blocks must be removed to avoid disk pollution
					_resourceManager.freeUnpinned(e.size);
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
		//awaitBelowHardLimit();

		if (DMLScript.STATISTICS) {
			Statistics.incrementOOCEvictionPut();
		}

		MatrixBlock mb = (MatrixBlock) value.getValue();
		long size = estimateSerializedSize(mb);

		_resourceManager.forceReserveAndUnpin(size);

		String key = streamId + "_" + blockId;

		BlockEntry newEntry = new BlockEntry(value, streamId, blockId, size);
		BlockEntry old;
		synchronized (_cacheLock) {
			old = _cache.put(key, newEntry); // remove old value, put new value
		}

		if (old != null)
			throw new IllegalStateException(); // We do not support overwriting items
	}

	private static void startEvictionPreparation() {
		//if (_prepRunning.compareAndSet(false, true)) {
			//System.out.println("RunPrep");
			getPrepExec().submit(() -> {
				try {
					runEvictionPreparation();
				}
				finally {
					//System.out.println("EndPrep");
					//_prepRunning.set(false);
				}
			});
		//}
	}

	private static void runEvictionPreparation() {
		//while (_residentSize.get() > _inMemoryLimit || _unspilledSize.get() > _limit) {
			List<Map.Entry<String, BlockEntry>> batch = collectEvictionBatch();
			if (batch.isEmpty()) {
				//System.err.println("Eviction batch is empty");
				return;
			}

			/*boolean shouldLaunch = batch.size() >= MIN_BATCH_BLOCKS || _residentSize.get() >= _hardLimit;
			if (!shouldLaunch) {
				//System.err.println("Should not launch eviction write");
				// Return entries to HOT to avoid leaving them in transient state.
				revertToHot(batch);
				break;
			}*/

			submitEvictionBatch(batch);
		//}
	}

	private static List<Map.Entry<String, BlockEntry>> collectEvictionBatch() {
		long size = _resourceManager.getReservedBytes();
		long targetFreedSize = Math.max(size - _limit, (long) PARTITION_EVICTION_SIZE);
		List<Map.Entry<String, BlockEntry>> candidates = new ArrayList<>(MAX_BATCH_BLOCKS);
		long totalPlanned = 0;
		int lockBusy = 0;
		int drops = 0;

		synchronized (_cacheLock) {
			Iterator<Map.Entry<String, BlockEntry>> iter = _cache.entrySet().iterator();

			while (iter.hasNext() && candidates.size() < MAX_BATCH_BLOCKS && totalPlanned < targetFreedSize) {
				Map.Entry<String, BlockEntry> e = iter.next();
				BlockEntry entry = e.getValue();
				String key = e.getKey();

				if (entry.lock.tryLock()) {
					try {
						boolean spilled = _spillLocations.containsKey(key);
						if (spilled && entry.value != null && entry.pins == 0) {
							// Drop from memory immediately (no write).
							entry.value = null;
							entry.state = BlockState.COLD;
							_resourceManager.freeUnpinned(entry.size);
							drops++;
						}
						else if (entry.state == BlockState.HOT && entry.pins == 0) {
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
				+ " lockBusy=" + lockBusy + " drops=" + drops);
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
				_activeWriters.decrementAndGet();
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
		String key = streamId + "_" + blockId;
		MutableBoolean couldPin = new  MutableBoolean(false);
		BlockEntry imv = tryGet(streamId, blockId, couldPin);

		if (couldPin.booleanValue()) {
			consumer.accept(new CachedQueueCallback(imv, null));
			return;
		}

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
				loadFromDisk(streamId, blockId, imv);

				LinkedList<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> callbacks = _readRequests.remove(key);

				if (callbacks.size() > 1) {
					changePins(imv, callbacks.size() - 1);
				}

				for(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> callback : callbacks) {
					callback.accept(new CachedQueueCallback(imv, null));
				}
			});
		}
	}

	/**
	 * Get a block if it is hot (available in memory). Otherwise return null.
	 */
	private static BlockEntry tryGet(long streamId, int blockId, MutableBoolean available) {
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
			throw new IllegalStateException();

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
			if (imv.state == BlockState.HOT || imv.state == BlockState.WARM) {
				pin(imv);
				available.setValue(true);
				return imv;
			}

		} finally {
			imv.lock.unlock();
		}

		return imv;
	}

	/**
	 * Get a block from the OOC cache (deserialize on read)
	 */
	public static OOCStream.QueueCallback<IndexedMatrixValue> getAndPin(long streamId, int blockId) {
		MutableBoolean couldPin = new  MutableBoolean(false);
		BlockEntry val = tryGet(streamId, blockId, couldPin);

		if (couldPin.booleanValue())
			return new CachedQueueCallback(val, null);

		loadFromDisk(streamId, blockId, val);
		// restore, since the block is COLD
		return new CachedQueueCallback(val, null);
	}

	/**
	 * Evict ByteBuffers to disk
	 */
	private static void evict(List<Map.Entry<String, BlockEntry>> candidates) {
		if (candidates == null || candidates.isEmpty())
			return;

		long currentSize = _resourceManager.getResidentBytes();
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

					// 5. change state based on pressure
					entry.lock.lock();
					long entrySize = entry.size;
					boolean dropFromMemory = entry.pins == 0 && entry.state != BlockState.COLD;
					try {
						if (dropFromMemory) {
							entry.value = null; // only release ref, don't mutate object
							entry.state = BlockState.COLD; // set state to cold, since writing to disk
							_resourceManager.freeUnpinned(entrySize);
							totalFreedSize += entrySize;
						}
						else if (entry.value != null) {
							entry.state = BlockState.WARM; // spilled but still resident
						}
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
		}

		// --- 3. ACCOUNTING PHASE ---
		if (DMLScript.STATISTICS) {
			System.err.println("Eviction batch summary: candidates=" + candidates.size()
				+ " freedBytes=" + totalFreedSize
				+ " sizeBefore=" + currentSize + " sizeAfter=" + _resourceManager.getResidentBytes());
		}
	}

	/**
	 * Load block from spill file
	 */
	private static void loadFromDisk(long streamId, int blockId, BlockEntry entry) {
		String key = streamId + "_" + blockId;
		_resourceManager.awaitAndReserve(entry.size);

		long ioDuration = 0;
		// 1. find the blocks address (spill location)
		spillLocation sloc = _spillLocations.get(key);
		if (sloc == null) {
			_resourceManager.freeReserved(entry.size);
			throw new DMLRuntimeException("Failed to load spill location for: " + key);
		}

		partitionFile partFile = _partitions.get(sloc.partitionId);
		if (partFile == null) {
			_resourceManager.freeReserved(entry.size);
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
				_resourceManager.freeReserved(entry.size);
				throw new DMLRuntimeException("Failed to load block " + key + " from " + filename, ex);
			}
		} catch (IOException e) {
			_resourceManager.freeReserved(entry.size);
			throw new RuntimeException(e);
		}

		entry.lock.lock();
		try {
			if (entry.state == BlockState.COLD) {
				entry.value = new IndexedMatrixValue(ix, mb);
				entry.state = BlockState.HOT;
				_resourceManager.unpinReserved(entry.size);
				pin(entry);

				synchronized (_cacheLock) {
					_cache.remove(key);
					_cache.put(key, entry);
				}
			} else {
				_resourceManager.freeReserved(entry.size);
				throw new IllegalStateException();
			}
		} finally {
			entry.lock.unlock();
		}

		if (DMLScript.STATISTICS) {
			Statistics.incrementOOCLoadFromDisk();
			Statistics.accumulateOOCLoadFromDiskTime(ioDuration);
		}
	}

	private static long estimateSerializedSize(MatrixBlock mb) {
		return mb.getExactSerializedSize();
	}

	public static void unpin(long streamId, int blockId) {
		String key = streamId + "_" + blockId;
		BlockEntry be;
		synchronized (_cacheLock) {
			be = _cache.get(key);
		}
		if (be == null)
			return;
		unpin(be);
	}

	private static void pin(BlockEntry be) {
		changePins(be, 1);
	}

	private static void unpin(BlockEntry be) {
		changePins(be, -1);
	}

	private static void changePins(BlockEntry be, int delta) {
		boolean freed = false;
		boolean evicted = false;
		be.lock.lock();
		try {
			if (be.state == BlockState.COLD)
				throw new IllegalStateException("Cannot modify pins of a cold block");
			if (be.value == null)
				throw new IllegalStateException("Cannot change pins on an evicted item");

			boolean incr = be.pins == 0 && delta > 0;
			be.pins += delta;
			boolean decr = be.pins == 0;

			if (incr) {
				int total = _pinCount.incrementAndGet();
				_resourceManager.pinUnpinned(be.size);
				if (total > PIN_WARN_THRESHOLD)
					System.err.println("Warning: high pinned count: " + total + " (threshold " + PIN_WARN_THRESHOLD + ")");
			}
			else if (decr) {
				freed = true;
				_pinCount.decrementAndGet();
				if (be.state == BlockState.WARM) {
					be.value = null;
					be.state = BlockState.COLD;
					evicted = true;
				}
			}

			if (be.pins < 0)
				throw new IllegalStateException();
		} finally {
			be.lock.unlock();
		}

		if (freed) {
			_resourceManager.unpinPinned(be.size);

			if (evicted)
				_resourceManager.freeUnpinned(be.size);
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

	static class CachedQueueCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final BlockEntry _result;
		private DMLRuntimeException _failure;
		private final AtomicBoolean _pinned;

		CachedQueueCallback(BlockEntry result, DMLRuntimeException failure) {
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
			IndexedMatrixValue ret = _result.value;
			if (ret == null)
				throw new IllegalStateException("Cannot get a cached item if it is not pinned in memory: " + _result.pins);
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
