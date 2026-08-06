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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.io.MatrixReader;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.packed.PackedBlock;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.runtime.ooc.stream.SourceOOCStream;
import org.apache.sysds.utils.Statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Origin backing store: blocks that were read from an external matrix file and never modified do not need to be spilled
 * at all, they can be re-read from the source. The initial scan records the physical record layout of every file it
 * walks, so a later re-read of one block can keep decoding the records that follow it and hand them to the cache
 * instead of paying one seek per small tile.
 */
final class SourceStore {
	private static final int READER_SIZE = 16;
	private static final int SOURCE_PACK_MAX_COUNT = 64;
	private static final int MAX_READ_AHEAD_COUNT = 64;
	private static final int MAX_DECLINED_OFFERS = 4;

	private final ThreadPoolExecutor _scanExec;
	private final ConcurrentHashMap<BlockKey, OOCIOHandler.SourceBlockDescriptor> _locations = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, BlockLayoutIndex> _layouts = new ConcurrentHashMap<>();
	private final int _scanCallerId = OOCEventLog.registerCaller("read_src");

	SourceStore() {
		_scanExec = new ThreadPoolExecutor(READER_SIZE, READER_SIZE, 0L, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(100000));
	}

	boolean contains(BlockKey key) {
		return _locations.containsKey(key);
	}

	void register(BlockKey key, OOCIOHandler.SourceBlockDescriptor descriptor) {
		_locations.put(key, descriptor);
		// The scan knows where a record sits but not yet which block it becomes; the identity is filled in here.
		BlockLayoutIndex index = _layouts.get(descriptor.path);
		if(index != null)
			index.setKey(index.slotOf(descriptor.offset), BlockLayoutIndex.packKey(key));
	}

	void delete(BlockKey key) {
		_locations.remove(key);
	}

	void shutdown() {
		_scanExec.getQueue().clear();
		_scanExec.shutdownNow();
		_locations.clear();
		_layouts.clear();
	}

	/**
	 * Reads one block back from its origin file and, if {@code readAheadBudget} allows, keeps decoding the records that
	 * follow it, offering each to the cache.
	 *
	 * @return the decoded object
	 */
	Object read(BlockEntry block, long readAheadBudget, OOCCache cache) {
		OOCIOHandler.SourceBlockDescriptor src = _locations.get(block.getKey());
		if(src == null)
			throw new DMLRuntimeException("Failed to load source location for: " + block.getKey());
		if(src.format != Types.FileFormat.BINARY)
			throw new DMLRuntimeException("Unsupported format for source read: " + src.format);

		long ioStart = DMLScript.OOC_STATISTICS ? System.nanoTime() : 0;
		Object data = src instanceof OOCIOHandler.GroupSourceBlockDescriptor gsrc ? readGroup(gsrc) : readSingle(src,
			readAheadBudget, cache);
		if(DMLScript.OOC_STATISTICS) {
			Statistics.incrementOOCLoadFromDisk();
			Statistics.accumulateOOCLoadFromDiskTime(System.nanoTime() - ioStart);
			Statistics.accumulateOOCLoadFromDiskBytes(block.getSize());
		}
		return data;
	}

	private Object readSingle(OOCIOHandler.SourceBlockDescriptor src, long readAheadBudget, OOCCache cache) {
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(src.path);
		MatrixIndexes ix = new MatrixIndexes();
		MatrixBlock mb = new MatrixBlock();

		try(SequenceFile.Reader reader = new SequenceFile.Reader(job, SequenceFile.Reader.file(path))) {
			reader.seek(src.offset);
			if(!reader.next(ix, mb))
				throw new DMLRuntimeException(
					"Failed to read source block at offset " + src.offset + " in " + src.path);
			if(readAheadBudget > 0)
				readAhead(src, reader, readAheadBudget, cache);
		}
		catch(IOException e) {
			throw new DMLRuntimeException(e);
		}
		return new IndexedMatrixValue(ix, mb);
	}

	/**
	 * Decodes the records following {@code src} from the already positioned reader. Stops as soon as the next record is
	 * not where the layout index says it should be, or once the cache keeps declining offers.
	 */
	private void readAhead(OOCIOHandler.SourceBlockDescriptor src, SequenceFile.Reader reader, long budget,
		OOCCache cache) throws IOException {
		BlockLayoutIndex index = _layouts.get(src.path);
		if(index == null)
			return;
		int slot = index.slotOf(src.offset);
		if(slot < 0)
			return;
		int count = index.count();
		long bytes = 0;
		int taken = 0;
		int declined = 0;
		for(int next = slot + 1; next < count && bytes < budget && taken < MAX_READ_AHEAD_COUNT; next++) {
			long packedKey = index.keyAt(next);
			long start = index.startAt(next);
			if(packedKey == BlockLayoutIndex.NO_KEY || start != reader.getPosition())
				return;
			MatrixIndexes indexes = new MatrixIndexes();
			MatrixBlock matrix = new MatrixBlock();
			long ioStart = DMLScript.OOC_LOG_EVENTS ? System.nanoTime() : 0;
			if(!reader.next(indexes, matrix))
				return;
			bytes += index.endAt(next) - start;
			if(DMLScript.OOC_LOG_EVENTS)
				OOCEventLog.onDiskReadEvent(_scanCallerId, ioStart, System.nanoTime(), index.endAt(next) - start);
			if(cache.activate(BlockLayoutIndex.unpackKey(packedKey), new IndexedMatrixValue(indexes, matrix))) {
				taken++;
				declined = 0;
			}
			else if(++declined >= MAX_DECLINED_OFFERS)
				return;
		}
	}

	private Object readGroup(OOCIOHandler.GroupSourceBlockDescriptor gsrc) {
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(gsrc.path);
		List<IndexedMatrixValue> values = new ArrayList<>(gsrc.count);
		try(SequenceFile.Reader reader = new SequenceFile.Reader(job, SequenceFile.Reader.file(path))) {
			reader.seek(gsrc.offset);
			for(OOCIOHandler.SourceBlockDescriptor descriptor : gsrc.blocks) {
				if(reader.getPosition() != descriptor.offset)
					throw new DMLRuntimeException(
						"Non-contiguous source pack at offset " + descriptor.offset + " in " + descriptor.path);
				MatrixIndexes indexes = new MatrixIndexes();
				MatrixBlock matrix = new MatrixBlock();
				if(!reader.next(indexes, matrix) || !indexes.equals(descriptor.indexes))
					throw new DMLRuntimeException(
						"Failed to read source block at offset " + descriptor.offset + " in " + descriptor.path);
				values.add(new IndexedMatrixValue(indexes, matrix));
			}
		}
		catch(IOException e) {
			throw new DMLRuntimeException(e);
		}
		if(!gsrc.packed)
			return values;
		Object[] packedValues = values.toArray();
		long[] sizes = gsrc.blocks.stream().mapToLong(descriptor -> descriptor.serializedSize).toArray();
		return PackedBlock.fromValues(packedValues, sizes);
	}

	CompletableFuture<OOCIOHandler.SourceReadResult> scan(OOCIOHandler.SourceReadRequest request,
		long maxBytesInFlight) {
		if(request.format != Types.FileFormat.BINARY)
			return CompletableFuture
				.failedFuture(new DMLRuntimeException("Unsupported format for source read: " + request.format));
		return readBinarySourceParallel(request, null, maxBytesInFlight);
	}

	CompletableFuture<OOCIOHandler.SourceReadResult> continueScan(OOCIOHandler.SourceReadContinuation continuation,
		long maxBytesInFlight) {
		if(!(continuation instanceof SourceReadState state))
			return CompletableFuture
				.failedFuture(new DMLRuntimeException("Unsupported continuation type: " + continuation));
		return readBinarySourceParallel(state.request, state, maxBytesInFlight);
	}

	private CompletableFuture<OOCIOHandler.SourceReadResult> readBinarySourceParallel(
		OOCIOHandler.SourceReadRequest request, SourceReadState state, long maxBytesInFlight) {
		final long byteLimit = maxBytesInFlight > 0 ? maxBytesInFlight : Long.MAX_VALUE;
		final AtomicLong bytesRead = new AtomicLong(0);
		final AtomicBoolean stop = new AtomicBoolean(false);
		final AtomicBoolean budgetHit = new AtomicBoolean(false);
		final AtomicReference<Throwable> error = new AtomicReference<>();
		final Object budgetLock = new Object();
		final CompletableFuture<OOCIOHandler.SourceReadResult> result = new CompletableFuture<>();
		final ConcurrentLinkedDeque<OOCIOHandler.SourceBlockDescriptor> descriptors = new ConcurrentLinkedDeque<>();

		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(request.path);

		Path[] files;
		AtomicLongArray filePositions;
		AtomicIntegerArray completed;

		try {
			FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
			MatrixReader.checkValidInputFile(fs, path);

			if(state == null) {
				List<Path> seqFiles = new ArrayList<>(Arrays.asList(IOUtilFunctions.getSequenceFilePaths(fs, path)));
				files = seqFiles.toArray(Path[]::new);
				filePositions = new AtomicLongArray(files.length);
				completed = new AtomicIntegerArray(files.length);
			}
			else {
				files = state.paths;
				filePositions = state.filePositions;
				completed = state.completed;
			}
		}
		catch(IOException e) {
			throw new DMLRuntimeException(e);
		}

		int activeTasks = 0;
		for(int i = 0; i < files.length; i++)
			if(completed.get(i) == 0)
				activeTasks++;

		final AtomicInteger remaining = new AtomicInteger(activeTasks);
		boolean anyTask = activeTasks > 0;

		for(int i = 0; i < files.length; i++) {
			if(completed.get(i) == 1)
				continue;
			final int fileIdx = i;
			try {
				_scanExec.submit(() -> {
					try {
						readSequenceFile(job, files[fileIdx], request, fileIdx, filePositions, completed, stop,
							budgetHit, bytesRead, byteLimit, budgetLock, descriptors);
					}
					catch(Throwable t) {
						error.compareAndSet(null, t);
						stop.set(true);
					}
					finally {
						if(remaining.decrementAndGet() == 0)
							completeResult(result, bytesRead, budgetHit, error, request, files, filePositions,
								completed, descriptors);
					}
				});
			}
			catch(RejectedExecutionException e) {
				error.compareAndSet(null, e);
				stop.set(true);
				if(remaining.decrementAndGet() == 0)
					completeResult(result, bytesRead, budgetHit, error, request, files, filePositions, completed,
						descriptors);
				break;
			}
		}

		if(!anyTask) {
			try {
				closeTarget(request.target, true);
				result.complete(new OOCIOHandler.SourceReadResult(bytesRead.get(), true, null, List.of()));
			}
			catch(DMLRuntimeException e) {
				result.completeExceptionally(e);
			}
		}

		return result;
	}

	private void completeResult(CompletableFuture<OOCIOHandler.SourceReadResult> future, AtomicLong bytesRead,
		AtomicBoolean budgetHit, AtomicReference<Throwable> error, OOCIOHandler.SourceReadRequest request, Path[] files,
		AtomicLongArray filePositions, AtomicIntegerArray completed,
		ConcurrentLinkedDeque<OOCIOHandler.SourceBlockDescriptor> descriptors) {
		Throwable err = error.get();
		if(err != null) {
			future.completeExceptionally(err instanceof Exception ? err : new Exception(err));
			return;
		}

		try {
			if(budgetHit.get()) {
				if(!request.keepOpenOnLimit)
					closeTarget(request.target, false);
				OOCIOHandler.SourceReadContinuation cont = new SourceReadState(request, files, filePositions,
					completed);
				future.complete(
					new OOCIOHandler.SourceReadResult(bytesRead.get(), false, cont, new ArrayList<>(descriptors)));
				return;
			}

			closeTarget(request.target, true);
			future
				.complete(new OOCIOHandler.SourceReadResult(bytesRead.get(), true, null, new ArrayList<>(descriptors)));
		}
		catch(DMLRuntimeException e) {
			future.completeExceptionally(e);
		}
	}

	private void readSequenceFile(JobConf job, Path path, OOCIOHandler.SourceReadRequest request, int fileIdx,
		AtomicLongArray filePositions, AtomicIntegerArray completed, AtomicBoolean stop, AtomicBoolean budgetHit,
		AtomicLong bytesRead, long byteLimit, Object budgetLock,
		ConcurrentLinkedDeque<OOCIOHandler.SourceBlockDescriptor> descriptors) throws IOException {
		MatrixIndexes key = new MatrixIndexes();
		List<IndexedMatrixValue> groupValues = new ArrayList<>();
		List<OOCIOHandler.SourceBlockDescriptor> groupDescriptors = new ArrayList<>();
		BlockLayoutIndex layout = _layouts.computeIfAbsent(path.toString(), p -> new BlockLayoutIndex());
		long groupLogicalBytes = 0;
		long groupRecordBytes = 0;
		long groupStart = -1;
		long groupEnd = -1;
		long packTarget = request.packTargetBytes;
		long maxRecordBytes = packTarget > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : 2 * packTarget;

		try(SequenceFile.Reader reader = new SequenceFile.Reader(job, SequenceFile.Reader.file(path))) {
			long pos = filePositions.get(fileIdx);
			if(pos > 0)
				reader.seek(pos);

			long ioStart = DMLScript.OOC_LOG_EVENTS ? System.nanoTime() : 0;
			while(!stop.get()) {
				long recordStart = reader.getPosition();
				MatrixBlock value = new MatrixBlock();
				if(!reader.next(key, value))
					break;
				long recordEnd = reader.getPosition();
				long blockSize = value.getExactSerializedSize();
				boolean shouldBreak = false;

				synchronized(budgetLock) {
					long currentBytes = bytesRead.get();
					if(stop.get())
						shouldBreak = true;
					else if(currentBytes > 0 && blockSize > byteLimit - currentBytes) {
						stop.set(true);
						budgetHit.set(true);
						shouldBreak = true;
					}
					else
						bytesRead.addAndGet(blockSize);
				}
				if(shouldBreak)
					break;

				// Record the physical layout so a later re-read of any of these blocks can decode its neighbours.
				layout.append(recordStart, recordEnd, BlockLayoutIndex.NO_KEY);

				MatrixIndexes outIdx = new MatrixIndexes(key);
				IndexedMatrixValue imv = new IndexedMatrixValue(outIdx, value);
				OOCIOHandler.SourceBlockDescriptor descriptor = new OOCIOHandler.SourceBlockDescriptor(path.toString(),
					request.format, outIdx, recordStart, (int) (recordEnd - recordStart), blockSize);

				boolean small = request.packThresholdBytes > 0 && blockSize < request.packThresholdBytes;
				long recordBytes = recordEnd - recordStart;
				boolean contiguous = groupValues.isEmpty() || recordStart == groupEnd;
				boolean canAdd = small && contiguous && groupValues.size() < SOURCE_PACK_MAX_COUNT &&
					groupLogicalBytes <= packTarget - blockSize && groupRecordBytes <= maxRecordBytes - recordBytes;
				if(!canAdd && !groupValues.isEmpty()) {
					flushSourceGroup(request, groupValues, groupDescriptors, groupStart, groupEnd, groupLogicalBytes);
					groupValues.clear();
					groupDescriptors.clear();
					groupLogicalBytes = 0;
					groupRecordBytes = 0;
					groupStart = -1;
					groupEnd = -1;
				}

				if(small) {
					if(groupValues.isEmpty())
						groupStart = recordStart;
					groupEnd = recordEnd;
					groupValues.add(imv);
					groupDescriptors.add(descriptor);
					groupLogicalBytes += blockSize;
					groupRecordBytes += recordBytes;
					if(groupLogicalBytes >= packTarget || groupRecordBytes >= maxRecordBytes ||
						groupValues.size() >= SOURCE_PACK_MAX_COUNT) {
						flushSourceGroup(request, groupValues, groupDescriptors, groupStart, groupEnd,
							groupLogicalBytes);
						groupValues.clear();
						groupDescriptors.clear();
						groupLogicalBytes = 0;
						groupRecordBytes = 0;
						groupStart = -1;
						groupEnd = -1;
					}
				}
				else
					emitSourceValue(request, imv, descriptor);
				descriptors.add(descriptor);
				filePositions.set(fileIdx, reader.getPosition());

				if(DMLScript.OOC_LOG_EVENTS) {
					long currTime = System.nanoTime();
					OOCEventLog.onDiskReadEvent(_scanCallerId, ioStart, currTime, blockSize);
					ioStart = currTime;
				}
			}

			if(!groupValues.isEmpty())
				flushSourceGroup(request, groupValues, groupDescriptors, groupStart, groupEnd, groupLogicalBytes);
			if(!stop.get())
				completed.set(fileIdx, 1);
		}
	}

	private static void emitSourceValue(OOCIOHandler.SourceReadRequest request, IndexedMatrixValue value,
		OOCIOHandler.SourceBlockDescriptor descriptor) {
		if(request.target instanceof SourceOOCStream source)
			source.enqueue(value, descriptor);
		else
			request.target.enqueue(value);
	}

	private static void flushSourceGroup(OOCIOHandler.SourceReadRequest request, List<IndexedMatrixValue> values,
		List<OOCIOHandler.SourceBlockDescriptor> blockDescriptors, long start, long end, long logicalBytes) {
		if(values.size() == 1) {
			emitSourceValue(request, values.get(0), blockDescriptors.get(0));
			return;
		}
		OOCIOHandler.SourceBlockDescriptor first = blockDescriptors.get(0);
		OOCIOHandler.GroupSourceBlockDescriptor group = new OOCIOHandler.GroupSourceBlockDescriptor(first.path,
			first.format, first.indexes, start, Math.toIntExact(end - start), logicalBytes, blockDescriptors, true);
		if(request.target instanceof SourceOOCStream source)
			source.enqueueGroup(new ArrayList<>(values), group);
		else
			for(IndexedMatrixValue value : values)
				request.target.enqueue(value);
	}

	private static void closeTarget(OOCStream<IndexedMatrixValue> target, boolean close) {
		if(!close)
			return;
		try {
			target.closeInput();
		}
		catch(Exception ex) {
			throw ex instanceof DMLRuntimeException ? (DMLRuntimeException) ex : new DMLRuntimeException(ex);
		}
	}

	private static final class SourceReadState implements OOCIOHandler.SourceReadContinuation {
		final OOCIOHandler.SourceReadRequest request;
		final Path[] paths;
		final AtomicLongArray filePositions;
		final AtomicIntegerArray completed;

		SourceReadState(OOCIOHandler.SourceReadRequest request, Path[] paths, AtomicLongArray filePositions,
			AtomicIntegerArray completed) {
			this.request = request;
			this.paths = paths;
			this.filePositions = filePositions;
			this.completed = completed;
		}
	}
}
