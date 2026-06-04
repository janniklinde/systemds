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

package org.apache.sysds.test.component.ooc.memory;

import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.BlockState;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.cache.OOCCacheScheduler;
import org.apache.sysds.runtime.ooc.cache.OOCLRUCacheScheduler;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

public class CachingStreamMemoryTrackingTest {
	private static final int ROWS = 32;
	private static final int COLS = 1;
	private static final int TILES = 4;
	private static final int PRESSURE_ROWS = 256 * 1024;
	private static final int PRESSURE_TILES = 8;
	private static final long WAIT_TIMEOUT_SEC = 5;
	private static final long STRESS_TARGET_BYTES = 4L << 30; // 4 GiB
	private static final long STRESS_CACHE_BYTES = 1L << 30; // 1 GiB
	private static final long STRESS_HARD_BYTES = STRESS_CACHE_BYTES + Math.max(40_000_000L, STRESS_CACHE_BYTES / 5);
	private static final long STRESS_WAIT_TIMEOUT_SEC = 120;

	@Before
	public void setUp() {
		OOCCacheManager.reset();
	}

	@After
	public void tearDown() {
		OOCCacheManager.reset();
	}

	@Test
	public void testCachingStreamHandsOverInMemoryCallbacksWithoutLeakingSourceAllowance() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(32_000_000L);
		SyncMemoryAllowance sourceAllowance = new SyncMemoryAllowance(broker);
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		CachingStream cached = new CachingStream(source);

		long bytes = tileBytes();
		sourceAllowance.reserveBlocking(bytes);
		source.enqueue(new InMemoryQueueCallback(tile(0, 7.0), null, sourceAllowance, bytes));

		Assert.assertEquals("Cache-level handover should release the producer-owned reservation.",
			0, sourceAllowance.getUsedMemory());

		CompletableFuture<Void> done = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger();
		cached.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					done.complete(null);
					return;
				}
				try(cb) {
					Assert.assertEquals(7.0 * ROWS * COLS, ((MatrixBlock) cb.get().getValue()).sum(), 0.0);
					count.incrementAndGet();
				}
			}
			catch(Throwable t) {
				done.completeExceptionally(t);
			}
		}, true);

		source.closeInput();
		done.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		Assert.assertEquals(1, count.get());
		Assert.assertEquals(0, sourceAllowance.getUsedMemory());
		cached.scheduleDeletion();
	}

	@Test
	public void testCachingStreamCanFeedCachedAllowance() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(64_000_000L);
		SyncMemoryAllowance sourceAllowance = new SyncMemoryAllowance(broker);
		CachedAllowance targetAllowance = new CachedAllowance(broker, TILES);
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		CachingStream cached = new CachingStream(source);
		CompletableFuture<Void> consumed = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger();
		long bytes = tileBytes();

		cached.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					consumed.complete(null);
					return;
				}
				int slot = (int) cb.get().getIndexes().getRowIndex() - 1;
				targetAllowance.handover(cb, slot);
				count.incrementAndGet();
			}
			catch(Throwable t) {
				consumed.completeExceptionally(t);
			}
		}, true);

		for(int i = 0; i < TILES; i++) {
			sourceAllowance.reserveBlocking(bytes);
			source.enqueue(new InMemoryQueueCallback(tile(i, i + 1), null, sourceAllowance, bytes));
		}
		source.closeInput();
		consumed.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

		Assert.assertEquals(TILES, count.get());
		Assert.assertEquals("All source-owned reservations should have moved into cache/CachedAllowance accounting.",
			0, sourceAllowance.getUsedMemory());

		for(int i = 0; i < TILES; i++) {
			OOCStream.QueueCallback<IndexedMatrixValue> cb = targetAllowance.get(i)
				.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			try(cb) {
				Assert.assertNotNull(cb);
				Assert.assertEquals((i + 1.0) * ROWS * COLS, ((MatrixBlock) cb.get().getValue()).sum(), 0.0);
			}
			targetAllowance.clear(i);
		}
		Assert.assertEquals(0, targetAllowance.getUsedMemory());
		cached.scheduleDeletion();
	}

	@Test
	public void testCachingStreamPressureSpillsAndReplays() throws Exception {
		long bytes = pressureTileBytes();
		OOCMatrixIOHandler ioHandler = new OOCMatrixIOHandler();
		OOCLRUCacheScheduler scheduler = new OOCLRUCacheScheduler(ioHandler, 4 * bytes, 12 * bytes, 0);
		installCache(ioHandler, scheduler);

		GlobalMemoryBroker broker = new GlobalMemoryBroker(4 * bytes);
		SyncMemoryAllowance sourceAllowance = new SyncMemoryAllowance(broker);
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		CachingStream cached = new CachingStream(source);

		for(int i = 0; i < PRESSURE_TILES; i++) {
			sourceAllowance.reserveBlocking(bytes);
			source.enqueue(new InMemoryQueueCallback(pressureTile(i, i + 1), null, sourceAllowance, bytes));
		}
		source.closeInput();

		waitFor(() -> sourceAllowance.getUsedMemory() == 0);
		waitFor(() -> scheduler.snapshot().stream().anyMatch(e -> e.getState() == BlockState.COLD));

		CachedAllowance targetAllowance = new CachedAllowance(broker, PRESSURE_TILES);
		CompletableFuture<Void> replayed = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger();
		cached.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					replayed.complete(null);
					return;
				}
				int slot = (int) cb.get().getIndexes().getRowIndex() - 1;
				targetAllowance.handover(cb, slot);
				count.incrementAndGet();
			}
			catch(Throwable t) {
				replayed.completeExceptionally(t);
			}
		}, true);

		replayed.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		Assert.assertEquals(PRESSURE_TILES, count.get());
		Assert.assertEquals(0, sourceAllowance.getUsedMemory());
		for(int i = 0; i < PRESSURE_TILES; i++) {
			OOCStream.QueueCallback<IndexedMatrixValue> cb = targetAllowance.get(i)
				.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			try(cb) {
				Assert.assertNotNull(cb);
				Assert.assertEquals((i + 1.0) * PRESSURE_ROWS * COLS,
					((MatrixBlock) cb.get().getValue()).sum(), 0.0);
			}
			targetAllowance.clear(i);
		}
		Assert.assertEquals(0, targetAllowance.getUsedMemory());
		cached.scheduleDeletion();
	}

	@Test
	public void testCachingStreamPendingHandoverIsImmediatelyReadableAndLaterCommits() throws Exception {
		long bytes = pressureTileBytes();
		OOCMatrixIOHandler ioHandler = new OOCMatrixIOHandler();
		OOCLRUCacheScheduler scheduler = new OOCLRUCacheScheduler(ioHandler, bytes - 1, bytes - 1, 0);
		installCache(ioHandler, scheduler);

		GlobalMemoryBroker broker = new GlobalMemoryBroker(2 * bytes);
		SyncMemoryAllowance sourceAllowance = new SyncMemoryAllowance(broker);
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		CachingStream cached = new CachingStream(source);
		cached.activateIndexing();

		sourceAllowance.reserveBlocking(bytes);
		source.enqueue(new InMemoryQueueCallback(pressureTile(0, 9.0), null, sourceAllowance, bytes));

		Assert.assertEquals("Pending handover should still be owned by the source allowance.",
			bytes, sourceAllowance.getUsedMemory());
		Assert.assertEquals(1, scheduler.snapshot().stream()
			.filter(e -> e.getState() == BlockState.HANDOVER_PENDING).count());

		OOCStream.QueueCallback<IndexedMatrixValue> cb = cached.peekCached(new MatrixIndexes(1, 1));
		try(cb) {
			Assert.assertEquals(9.0 * PRESSURE_ROWS * COLS, ((MatrixBlock) cb.get().getValue()).sum(), 0.0);
		}
		Assert.assertEquals(bytes, sourceAllowance.getUsedMemory());

		scheduler.updateLimits(2 * bytes, 2 * bytes);
		BlockKey triggerKey = new BlockKey(999, 999);
		scheduler.put(triggerKey, new Object(), 1);
		waitFor(() -> sourceAllowance.getUsedMemory() == 0);
		Assert.assertEquals(0, scheduler.snapshot().stream()
			.filter(e -> e.getState() == BlockState.HANDOVER_PENDING).count());

		scheduler.forget(triggerKey);
		source.closeInput();
		cached.scheduleDeletion();
	}

	@Test
	public void testPendingHandoverCanMoveIntoCachedAllowance() throws Exception {
		long bytes = pressureTileBytes();
		OOCMatrixIOHandler ioHandler = new OOCMatrixIOHandler();
		OOCLRUCacheScheduler scheduler = new OOCLRUCacheScheduler(ioHandler, bytes - 1, bytes - 1, 0);
		installCache(ioHandler, scheduler);

		GlobalMemoryBroker broker = new GlobalMemoryBroker(4 * bytes);
		SyncMemoryAllowance sourceAllowance = new SyncMemoryAllowance(broker);
		CachedAllowance targetAllowance = new CachedAllowance(broker, 1);
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		CachingStream cached = new CachingStream(source);
		cached.activateIndexing();

		sourceAllowance.reserveBlocking(bytes);
		source.enqueue(new InMemoryQueueCallback(pressureTile(0, 11.0), null, sourceAllowance, bytes));
		Assert.assertEquals(1, scheduler.snapshot().stream()
			.filter(e -> e.getState() == BlockState.HANDOVER_PENDING).count());

		OOCStream.QueueCallback<IndexedMatrixValue> cb = cached.peekCached(new MatrixIndexes(1, 1));
		targetAllowance.handover(cb, 0);

		Assert.assertEquals(0, sourceAllowance.getUsedMemory());
		Assert.assertEquals(bytes, targetAllowance.getUsedMemory());
		Assert.assertEquals(0, scheduler.snapshot().stream()
			.filter(e -> e.getState() == BlockState.HANDOVER_PENDING).count());

		OOCStream.QueueCallback<IndexedMatrixValue> target = targetAllowance.get(0)
			.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		try(target) {
			Assert.assertNotNull(target);
			Assert.assertEquals(11.0 * PRESSURE_ROWS * COLS, ((MatrixBlock) target.get().getValue()).sum(), 0.0);
		}

		targetAllowance.clear(0);
		Assert.assertEquals(0, targetAllowance.getUsedMemory());
		source.closeInput();
		cached.scheduleDeletion();
	}

	@Ignore("4 GiB OOC stress test; enable manually when validating eviction behavior.")
	@Test
	public void testCachingStreamScansFourGiBWithOneGiBCache() throws Exception {
		System.out.println("250x250");
		runCachingStreamScanStress(250, 250);
		System.out.println("250x1");
		runCachingStreamScanStress(250, 1);
	}

	private static void runCachingStreamScanStress(int rows, int cols) throws Exception {
		MatrixBlock tile = new MatrixBlock(rows, cols, 1.0);
		long bytes = tile.getExactSerializedSize();
		int blocks = (int)Math.ceil(STRESS_TARGET_BYTES / (double)bytes);
		double expectedSum = (double)blocks * rows * cols;

		OOCMatrixIOHandler ioHandler = new OOCMatrixIOHandler();
		OOCLRUCacheScheduler scheduler = new OOCLRUCacheScheduler(ioHandler, STRESS_CACHE_BYTES,
			STRESS_HARD_BYTES, Math.max(40_000_000L, STRESS_HARD_BYTES - STRESS_CACHE_BYTES));
		installCache(ioHandler, scheduler);

		GlobalMemoryBroker broker = new GlobalMemoryBroker(STRESS_CACHE_BYTES);
		SyncMemoryAllowance sourceAllowance = new SyncMemoryAllowance(broker, STRESS_CACHE_BYTES);
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		CachingStream cached = new CachingStream(source);
		try {
			for(int i = 0; i < blocks; i++) {
				sourceAllowance.reserveBlocking(bytes);
				source.enqueue(new InMemoryQueueCallback(
					new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1L), tile),
					null, sourceAllowance, bytes));
			}
			source.closeInput();
			waitFor(() -> sourceAllowance.getUsedMemory() == 0, STRESS_WAIT_TIMEOUT_SEC);
			waitFor(() -> scheduler.snapshot().stream().anyMatch(e -> e.getState() == BlockState.COLD),
				STRESS_WAIT_TIMEOUT_SEC);

			consumeFullScan(cached.getReadStream(), blocks, expectedSum);
			consumeFullScan(cached.getReadStream(), blocks, expectedSum);
		}
		finally {
			cached.scheduleDeletion();
			sourceAllowance.destroy();
			scheduler.shutdown();
			ioHandler.shutdown();
			OOCCacheManager.reset();
		}
	}

	private static void consumeFullScan(OOCStream<IndexedMatrixValue> stream, int expectedBlocks,
		double expectedSum) {
		int count = 0;
		double sum = 0;
		while(true) {
			OOCStream.QueueCallback<IndexedMatrixValue> cb = stream.dequeueCB();
			if(cb == null || cb.isEos())
				break;
			try(cb) {
				Assert.assertFalse(cb.isFailure());
				sum += ((MatrixBlock)cb.get().getValue()).sum();
				count++;
			}
		}
		Assert.assertEquals(expectedBlocks, count);
		Assert.assertEquals(expectedSum, sum, 0.0);
	}

	private static IndexedMatrixValue tile(int idx, double value) {
		return new IndexedMatrixValue(new MatrixIndexes(idx + 1L, 1L), new MatrixBlock(ROWS, COLS, value));
	}

	private static long tileBytes() {
		return new MatrixBlock(ROWS, COLS, 1.0).getExactSerializedSize();
	}

	private static IndexedMatrixValue pressureTile(int idx, double value) {
		return new IndexedMatrixValue(new MatrixIndexes(idx + 1L, 1L),
			new MatrixBlock(PRESSURE_ROWS, COLS, value));
	}

	private static long pressureTileBytes() {
		return new MatrixBlock(PRESSURE_ROWS, COLS, 1.0).getExactSerializedSize();
	}

	@SuppressWarnings("unchecked")
	private static void installCache(OOCIOHandler ioHandler, OOCCacheScheduler scheduler) throws Exception {
		Field ioField = OOCCacheManager.class.getDeclaredField("_ioHandler");
		ioField.setAccessible(true);
		((AtomicReference<OOCIOHandler>) ioField.get(null)).set(ioHandler);

		Field schedulerField = OOCCacheManager.class.getDeclaredField("_scheduler");
		schedulerField.setAccessible(true);
		((AtomicReference<OOCCacheScheduler>) schedulerField.get(null)).set(scheduler);
	}

	private static void waitFor(BooleanSupplier condition) throws Exception {
		waitFor(condition, WAIT_TIMEOUT_SEC);
	}

	private static void waitFor(BooleanSupplier condition, long timeoutSeconds) throws Exception {
		long timeoutNanos = TimeUnit.SECONDS.toNanos(timeoutSeconds);
		long start = System.nanoTime();
		while(System.nanoTime() - start < timeoutNanos) {
			if(condition.getAsBoolean())
				return;
			Thread.sleep(1);
		}
		Assert.assertTrue(condition.getAsBoolean());
	}
}
