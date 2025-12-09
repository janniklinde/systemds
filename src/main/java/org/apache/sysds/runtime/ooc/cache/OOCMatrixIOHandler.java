package org.apache.sysds.runtime.ooc.cache;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.util.FastBufferedDataOutputStream;
import org.apache.sysds.runtime.util.LocalFileUtils;
import org.apache.sysds.utils.Statistics;
import scala.Tuple2;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OOCMatrixIOHandler implements OOCIOHandler {
	private final String _spillDir;
	private final ThreadPoolExecutor _writeExec;
	private final ThreadPoolExecutor _readExec;

	// Spill related structures
	private final ConcurrentHashMap<String, SpillLocation> _spillLocations =  new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Integer, PartitionFile> _partitions = new ConcurrentHashMap<>();
	private final AtomicInteger _partitionCounter = new AtomicInteger(0);

	public OOCMatrixIOHandler() {
		this._spillDir = LocalFileUtils.getUniqueWorkingDir("ooc_stream");
		_writeExec = new ThreadPoolExecutor(
			1,
			1,
			0L,
			TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(100000));
		_readExec = new ThreadPoolExecutor(
			5,
			5,
			0L,
			TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(100000));
	}

	@Override
	public void shutdown() {
		_writeExec.getQueue().clear();
		_writeExec.shutdown();
		_readExec.getQueue().clear();
		_readExec.shutdown();
		_spillLocations.clear();
		_partitions.clear();
		LocalFileUtils.deleteFileIfExists(_spillDir);
	}

	@Override
	public CompletableFuture<Void> scheduleEviction(BlockEntry block) {
		System.out.println("Schedule eviction: " + block.getKey());
		CompletableFuture<Void> future = new CompletableFuture<>();
		CloseableQueue<Tuple2<BlockEntry, CompletableFuture<Void>>> q = new CloseableQueue<>();
		try {
			q.enqueueIfOpen(new Tuple2<>(block, future));
			q.close();
		} catch (InterruptedException e) {
			throw new DMLRuntimeException(e);
		}
		try {
			_writeExec.submit(() -> evict(q));
		} catch (RejectedExecutionException e) {
			System.out.println("Eviction rejected: " + block.getKey());
			future.completeExceptionally(e);
		}
		future.whenComplete((e, r) -> System.out.println("Eviction done: " + block.getKey()));
		return future;
	}

	@Override
	public CompletableFuture<BlockEntry> scheduleRead(final BlockEntry block) {
		System.out.println("Schedule read: " + block.getKey());
		final CompletableFuture<BlockEntry> future = new CompletableFuture<>();
		try {
			_readExec.submit(() -> {
				try {
					loadFromDisk(block);
					future.complete(block);
				} catch (Throwable e) {
					e.printStackTrace();
					future.completeExceptionally(e);
				}

				System.out.println("Read done: " + block.getKey());
			});
		} catch (RejectedExecutionException e) {
			future.completeExceptionally(e);
		}
		return future;
	}

	@Override
	public CompletableFuture<Boolean> scheduleDeletion(BlockEntry block) {
		// TODO
		return CompletableFuture.completedFuture(true);
	}


	private void loadFromDisk(BlockEntry block) {
		String key = block.getKey().toFileKey();

		long ioDuration = 0;
		// 1. find the blocks address (spill location)
		SpillLocation sloc = _spillLocations.get(key);
		if (sloc == null)
			throw new DMLRuntimeException("Failed to load spill location for: " + key);

		PartitionFile partFile = _partitions.get(sloc.partitionId);
		if (partFile == null)
			throw new DMLRuntimeException("Failed to load partition for: " + sloc.partitionId);

		String filename = partFile.filePath;

		// Create an empty object to read data into.
		MatrixIndexes ix = new  MatrixIndexes();
		MatrixBlock mb = new  MatrixBlock();

		try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
			raf.seek(sloc.offset);

			DataInputStream dis = new DataInputStream(
				new BufferedInputStream(Channels.newInputStream(raf.getChannel())));
			long ioStart = DMLScript.STATISTICS ? System.nanoTime() : 0;
			ix.readFields(dis); // 1. Read Indexes
			mb.readFields(dis); // 2. Read Block
			if (DMLScript.STATISTICS)
				ioDuration = System.nanoTime() - ioStart;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		block.setDataUnsafe(new IndexedMatrixValue(ix, mb));

		if (DMLScript.STATISTICS) {
			Statistics.incrementOOCLoadFromDisk();
			Statistics.accumulateOOCLoadFromDiskTime(ioDuration);
		}
	}

	/**
	 * Evict ByteBuffers to disk
	 */
	private void evict(CloseableQueue<Tuple2<BlockEntry, CompletableFuture<Void>>> candidates) {
		// --- 1. WRITE PHASE ---
		// write to partition file
		// 1. generate a new ID for the present "partition" (file)
		int partitionId = _partitionCounter.getAndIncrement();

		LocalFileUtils.createLocalFileIfNotExist(_spillDir);

		// Spill to disk
		String filename = _spillDir + "/stream_batch_part_" + partitionId;

		long ioStart = DMLScript.STATISTICS ? System.nanoTime() : 0;

		// 2. create the partition file metadata
		PartitionFile partFile = new PartitionFile(filename);
		_partitions.put(partitionId, partFile);

		FileOutputStream fos = null;
		FastBufferedDataOutputStream dos = null;
		try {
			fos = new FileOutputStream(filename);
			dos = new FastBufferedDataOutputStream(fos);

			Tuple2<BlockEntry, CompletableFuture<Void>> tpl;
			int cnt = 0;

			// loop over the list of blocks we collected
			while ((tpl = candidates.take(10, TimeUnit.MILLISECONDS)) != null) {
				cnt++;
				BlockEntry entry = tpl._1;
				CompletableFuture<Void> future = tpl._2;
				boolean wrote = writeOut(partitionId, entry, fos, dos);

				if (DMLScript.STATISTICS) {
					if (wrote)
						Statistics.incrementOOCEvictionWrite();
				}

				future.complete(null);
			}
			if (candidates.close()) {
				while((tpl = candidates.take(1, TimeUnit.MILLISECONDS)) != null) {
					cnt++;
					BlockEntry entry = tpl._1;
					CompletableFuture<Void> future = tpl._2;
					boolean wrote = writeOut(partitionId, entry, fos, dos);

					if(DMLScript.STATISTICS) {
						if(wrote)
							Statistics.incrementOOCEvictionWrite();
					}

					future.complete(null);
				}
			}
			System.out.println("Total evictions: " + cnt);
		}
		catch(IOException | InterruptedException ex) {
			ex.printStackTrace();
			throw new DMLRuntimeException(ex);
		} catch (Exception e) {
			e.printStackTrace(); // TODO
		} finally {
			if (DMLScript.STATISTICS)
				Statistics.accumulateOOCEvictionWriteTime(System.nanoTime() - ioStart);
			IOUtilFunctions.closeSilently(dos);
			IOUtilFunctions.closeSilently(fos);
		}

		// --- 3. ACCOUNTING PHASE ---
		/*if (DMLScript.STATISTICS) {
			System.err.println("Eviction batch summary: candidates=" + candidates.size()
				+ " freedBytes=" + totalFreedSize
				+ " sizeBefore=" + currentSize + " sizeAfter=" + _resourceManager.getResidentBytes());
		}*/
	}

	private boolean writeOut(int partitionId, BlockEntry entry, FileOutputStream fos, FastBufferedDataOutputStream dos) throws IOException {
		String key = entry.getKey().toFileKey();
		boolean alreadySpilled = _spillLocations.containsKey(key);

		if (!alreadySpilled) {
			// 1. get the current file position. this is the offset.
			// flush any buffered data to the file
			//dos.flush();
			dos.flush();
			long offset = fos.getChannel().position();

			// 2. write indexes and block
			IndexedMatrixValue imv = (IndexedMatrixValue) entry.getDataUnsafe(); // Get data without requiring pin
			imv.getIndexes().write(dos); // write Indexes
			imv.getValue().write(dos);

			// 3. create the spillLocation
			SpillLocation sloc = new SpillLocation(partitionId, offset);
			_spillLocations.put(key, sloc);
			return true;
		}
		return false;
	}




	private static class SpillLocation {
		// structure of spillLocation: file, offset
		final int partitionId;
		final long offset;

		SpillLocation(int partitionId, long offset) {

			this.partitionId = partitionId;
			this.offset = offset;
		}
	}

	private static class PartitionFile {
		final String filePath;

		PartitionFile(String filePath) {
			this.filePath = filePath;
		}
	}
}
