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

package org.apache.sysds.test.component.ooc.cache;

import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreImpl;
import org.apache.sysds.runtime.ooc.store.SequentialAccessPattern;
import org.apache.sysds.utils.Statistics;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class MaterializedStoreImplStressTest {
	private static final long GIB = 1L << 30;
	private static final long MIB = 1L << 20;
	private static final long PACK_THRESHOLD_BYTES =
		Long.getLong("sysds.test.ooc.materializedStore.packThresholdBytes", 64L << 10);

	private static final long LOGICAL_BYTES =
		Long.getLong("sysds.test.ooc.materializedStore.bytes", 32L * GIB);
	private static final long CACHE_BYTES =
		Long.getLong("sysds.test.ooc.materializedStore.cacheBytes", GIB);
	private static final long HARD_BYTES =
		Long.getLong("sysds.test.ooc.materializedStore.hardBytes", CACHE_BYTES + 256L * MIB);
	private static final long PACK_BYTES =
		Long.getLong("sysds.test.ooc.materializedStore.packBytes", 512L << 10);
	private static final int TILE_ROWS =
		Integer.getInteger("sysds.test.ooc.materializedStore.tileRows", 1000);
	private static final int TILE_COLS =
		Integer.getInteger("sysds.test.ooc.materializedStore.tileCols", 1);
	private static final int PRODUCER_WORKERS =
		Integer.getInteger("sysds.test.ooc.materializedStore.producerWorkers",
			Math.max(2, Math.min(16, Runtime.getRuntime().availableProcessors())));
	private static final int ORDERED_PREFETCH =
		Integer.getInteger("sysds.test.ooc.materializedStore.orderedPrefetch", 4096);
	private static final int PACK_PREFETCH =
		Integer.getInteger("sysds.test.ooc.materializedStore.packPrefetch", 64);
	private static final long TIMEOUT_SECONDS =
		Long.getLong("sysds.test.ooc.materializedStore.timeoutSeconds", 1800L);

	@Ignore("Manual configurable multi-GiB stress test with real spill IO.")
	@Test
	public void testSequentialReaders() throws Exception {
		runStress(false);
	}

	@Ignore("Manual configurable multi-GiB stress test with real spill IO.")
	@Test
	public void testOpportunisticReaders() throws Exception {
		runStress(true);
	}

	private static void runStress(boolean opportunistic) throws Exception {
		validateConfiguration();
		boolean oldOOCStats = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		Statistics.resetOOCEvictionStats();

		long tileBytes = Math.multiplyExact(Math.multiplyExact((long)TILE_ROWS, TILE_COLS), 8L);
		int blocks = Math.toIntExact((LOGICAL_BYTES + tileBytes - 1) / tileBytes);
		long brokerBytes = Math.addExact(LOGICAL_BYTES, HARD_BYTES);
		long streamId = opportunistic ? 2 : 1;
		GlobalMemoryBroker broker = new GlobalMemoryBroker(brokerBytes);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker, brokerBytes);
		SyncMemoryAllowance readerAllowance = new SyncMemoryAllowance(broker, brokerBytes);
		OOCMatrixIOHandler io = new OOCMatrixIOHandler();
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(io, HARD_BYTES, CACHE_BYTES),
			PACK_THRESHOLD_BYTES, PACK_BYTES, 5);
		MaterializedStoreImpl<IndexedMatrixValue> store = new MaterializedStoreImpl<>(cache, streamId);
		ExecutorService producers = Executors.newFixedThreadPool(PRODUCER_WORKERS);

		try {
			long loadStart = System.currentTimeMillis();
			publishAsync(store, producer, blocks, tileBytes, producers);
			store.complete();
			Assert.assertEquals(blocks, store.size());
			waitFor(() -> producer.getUsedMemory() == 0);
			waitFor(() -> cache.getOwnedCacheSize() <= CACHE_BYTES);
			System.out.printf("MaterializedStore %s load: blocks=%d, tileBytes=%d, packs=%d, time=%d ms%n",
				name(opportunistic), blocks, tileBytes, cache.getPackGroupCount(),
				System.currentTimeMillis() - loadStart);

			if(opportunistic)
				replayOpportunistic(store, cache, streamId, blocks, readerAllowance);
			else
				replaySequential(store, cache, streamId, blocks, readerAllowance);

			System.out.printf("MaterializedStore %s %.3f GiB stress stats:%n%s%n",
				name(opportunistic), LOGICAL_BYTES / (double)GIB, Statistics.displayOOCEvictionStats());
		}
		finally {
			producers.shutdownNow();
			store.close();
			cache.shutdown();
			producer.destroy();
			readerAllowance.destroy();
			DMLScript.OOC_STATISTICS = oldOOCStats;
		}
	}

	private static void publishAsync(MaterializedStoreImpl<IndexedMatrixValue> store,
		SyncMemoryAllowance producer, int blocks, long tileBytes, ExecutorService producers) throws Exception {
		int batchTiles = Math.max(1, Math.toIntExact(PACK_BYTES / tileBytes));
		AtomicInteger nextIndex = new AtomicInteger();
		Future<?>[] tasks = new Future<?>[PRODUCER_WORKERS];
		for(int worker = 0; worker < PRODUCER_WORKERS; worker++) {
			tasks[worker] = producers.submit(() -> {
				MatrixBlock tile = new MatrixBlock(TILE_ROWS, TILE_COLS, 1.0);
				int[] indices = new int[batchTiles];
				IndexedMatrixValue[] values = new IndexedMatrixValue[batchTiles];
				long[] sizes = new long[batchTiles];
				int first;
				while((first = nextIndex.getAndAdd(batchTiles)) < blocks) {
					int len = Math.min(batchTiles, blocks - first);
					for(int i = 0; i < len; i++) {
						int index = first + i;
						indices[i] = index;
						values[i] = new IndexedMatrixValue(new MatrixIndexes(index + 1L, 1), tile);
						sizes[i] = tileBytes;
					}
					producer.reserveBlocking(Math.multiplyExact(tileBytes, len));
					store.publishPackPinned(indices, values, sizes, 0, len, producer);
				}
			});
		}
		for(Future<?> task : tasks)
			task.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	private static void replaySequential(MaterializedStoreImpl<IndexedMatrixValue> store,
		OOCPackedCache cache, long streamId, int blocks, SyncMemoryAllowance allowance) throws Exception {
		MaterializedStore.Reader<IndexedMatrixValue> first =
			store.openReader(new SequentialAccessPattern(blocks), allowance, ORDERED_PREFETCH);
		MaterializedStore.Reader<IndexedMatrixValue> second =
			store.openReader(new SequentialAccessPattern(blocks), allowance, ORDERED_PREFETCH);
		store.sealReaders();

		consumeSequential(first, cache, streamId, blocks, true, 0);
		waitFor(() -> allowance.getUsedMemory() == 0);
		consumeSequential(second, cache, streamId, blocks, false, 1);
		waitFor(() -> allowance.getUsedMemory() == 0);
	}

	private static void consumeSequential(MaterializedStore.Reader<IndexedMatrixValue> reader,
		OOCPackedCache cache, long streamId, int blocks, boolean retain, int pass) throws Exception {
		long start = System.currentTimeMillis();
		int expected = 0;
		while(reader.hasNext()) {
			int index;
			try(MaterializedStore.Lease<IndexedMatrixValue> lease = reader.next()) {
				index = lease.index();
				Assert.assertEquals(expected, index);
				assertValue(index, lease.value());
			}
			assertLocation(cache, streamId, index, retain, pass);
			expected++;
		}
		reader.close();
		Assert.assertEquals(blocks, expected);
		System.out.printf("MaterializedStore sequential pass %d: %d ms%n",
			pass, System.currentTimeMillis() - start);
	}

	private static void replayOpportunistic(MaterializedStoreImpl<IndexedMatrixValue> store,
		OOCPackedCache cache, long streamId, int blocks, SyncMemoryAllowance allowance) throws Exception {
		MaterializedStore.PackReader<IndexedMatrixValue> first =
			store.openOpportunisticReader(new SequentialAccessPattern(blocks), allowance, PACK_PREFETCH);
		MaterializedStore.PackReader<IndexedMatrixValue> second =
			store.openOpportunisticReader(new SequentialAccessPattern(blocks), allowance, PACK_PREFETCH);
		store.sealReaders();

		consumeOpportunistic(first, cache, streamId, blocks, true, 0);
		waitFor(() -> allowance.getUsedMemory() == 0);
		consumeOpportunistic(second, cache, streamId, blocks, false, 1);
		waitFor(() -> allowance.getUsedMemory() == 0);
	}

	private static void consumeOpportunistic(MaterializedStore.PackReader<IndexedMatrixValue> reader,
		OOCPackedCache cache, long streamId, int blocks, boolean retain, int pass) throws Exception {
		long start = System.currentTimeMillis();
		BitSet consumed = new BitSet(blocks);
		int count = 0;
		while(reader.hasNext()) {
			MaterializedStore.PackLease<IndexedMatrixValue> pack = reader.nextPack();
			int[] indices = new int[pack.size()];
			try {
				for(int slot = 0; slot < pack.size(); slot++) {
					int index = pack.index(slot);
					Assert.assertFalse("Duplicate index " + index + " in pass " + pass, consumed.get(index));
					assertValue(index, pack.value(slot));
					consumed.set(index);
					indices[slot] = index;
					count++;
				}
			}
			finally {
				pack.close();
			}
			for(int index : indices)
				assertLocation(cache, streamId, index, retain, pass);
		}
		reader.close();
		Assert.assertEquals(blocks, count);
		Assert.assertEquals(blocks, consumed.cardinality());
		System.out.printf("MaterializedStore opportunistic pass %d: %d ms%n",
			pass, System.currentTimeMillis() - start);
	}

	private static void assertValue(int index, IndexedMatrixValue value) {
		Assert.assertNotNull(value);
		Assert.assertEquals(index + 1L, value.getIndexes().getRowIndex());
		Assert.assertEquals(1.0, ((MatrixBlock)value.getValue()).get(0, 0), 0.0);
	}

	private static void assertLocation(OOCPackedCache cache, long streamId, int index, boolean retained, int pass) {
		boolean present = cache.getPackGroup(streamId, index) != null;
		if(present != retained)
			Assert.fail("Unexpected forgetting state after pass " + pass + " at index " + index);
	}

	private static void waitFor(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
		while(System.nanoTime() < deadline) {
			if(condition.getAsBoolean())
				return;
			Thread.sleep(1);
		}
		Assert.assertTrue(condition.getAsBoolean());
	}

	private static String name(boolean opportunistic) {
		return opportunistic ? "opportunistic" : "sequential";
	}

	private static void validateConfiguration() {
		long tileBytes = Math.multiplyExact(Math.multiplyExact((long)TILE_ROWS, TILE_COLS), 8L);
		if(LOGICAL_BYTES <= 0 || CACHE_BYTES <= 0 || HARD_BYTES <= CACHE_BYTES ||
			PACK_THRESHOLD_BYTES <= 0 || PACK_BYTES < PACK_THRESHOLD_BYTES)
			throw new IllegalArgumentException("Invalid materialized-store stress memory configuration");
		if(TILE_ROWS <= 0 || TILE_COLS <= 0 || PRODUCER_WORKERS <= 0)
			throw new IllegalArgumentException("Invalid materialized-store stress production configuration");
		if(tileBytes >= PACK_THRESHOLD_BYTES)
			throw new IllegalArgumentException("Tile size must remain below the packing threshold");
	}
}
