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

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.utils.Statistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.BooleanSupplier;

public class OOCCacheImplStressTest {
	private static final int BLOCKS = 512;
	private static final long BYTES = 1_000;
	private static final long WAIT_TIMEOUT_SEC = 5;
	private static final long GIB = 1L << 30;
	private static final long MANUAL_STRESS_BYTES = 4L * GIB;
	private static final long MANUAL_CACHE_BYTES = GIB;
	private static final long MANUAL_HARD_BYTES = MANUAL_CACHE_BYTES + (256L << 20);
	private static final int MANUAL_STRESS_ROWS = 250;
	private static final int MANUAL_STRESS_COLS = 250;

	private FakeIOHandler _io;
	private GlobalMemoryBroker _broker;
	private SyncMemoryAllowance _producer;
	private SyncMemoryAllowance _consumerA;
	private SyncMemoryAllowance _consumerB;
	private OOCCacheImpl _cache;

	@Before
	public void setUp() {
		_io = new FakeIOHandler();
		_broker = new GlobalMemoryBroker(BLOCKS * BYTES * 8);
		_producer = new SyncMemoryAllowance(_broker, BLOCKS * BYTES * 4);
		_consumerA = new SyncMemoryAllowance(_broker, BLOCKS * BYTES * 4);
		_consumerB = new SyncMemoryAllowance(_broker, BLOCKS * BYTES * 4);
		_cache = new OOCCacheImpl(_io, BLOCKS * BYTES, 32 * BYTES);
	}

	@After
	public void tearDown() {
		if(_cache != null)
			_cache.shutdown();
		if(_producer != null)
			_producer.destroy();
		if(_consumerA != null)
			_consumerA.destroy();
		if(_consumerB != null)
			_consumerB.destroy();
	}

	@Test
	public void testStressOwnershipEvictionAndReplay() throws Exception {
		BlockEntry[] entries = new BlockEntry[BLOCKS];

		for(int i = 0; i < BLOCKS; i++) {
			_producer.reserveBlocking(BYTES);
			entries[i] = _cache.putPinned(key(i), payload(i), BYTES, _producer);
			Assert.assertEquals(0, _cache.getOwnedCacheSize());
		}
		Assert.assertEquals(BLOCKS * BYTES, _producer.getUsedMemory());

		for(int i = 0; i < BLOCKS; i += 2) {
			BlockEntry pinned = _cache.pinIfLive(key(i).getStreamId(), key(i).getSequenceNumber(), _consumerA);
			Assert.assertNotNull(pinned);
			OOCCache.UnpinHandle fast = _cache.unpin(entries[i], _producer);
			Assert.assertTrue(fast.isCommitted());
			Assert.assertEquals("Non-last unpin should release only the producer allowance.",
				BLOCKS * BYTES - (long)(i / 2 + 1) * BYTES, _producer.getUsedMemory());
			Assert.assertEquals((long)(i / 2 + 1) * BYTES, _consumerA.getUsedMemory());
		}

		for(int i = 1; i < BLOCKS; i += 2) {
			OOCCache.UnpinHandle handle = _cache.unpin(entries[i], _producer);
			Assert.assertTrue(handle.isCommitted());
		}
		Assert.assertEquals(0, _producer.getUsedMemory());

		waitFor(() -> _cache.getOwnedCacheSize() <= 32 * BYTES);
		Assert.assertTrue("Eviction should spill some cache-owned entries.", _io.getEvictionCount() > 0);

		for(int i = 0; i < BLOCKS; i += 2) {
			OOCCache.UnpinHandle handle = _cache.unpin(entries[i], _consumerA);
			if(!handle.isCommitted())
				handle.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		}
		Assert.assertEquals(0, _consumerA.getUsedMemory());
		waitFor(() -> _cache.getOwnedCacheSize() <= 32 * BYTES);

		for(int i = BLOCKS - 1; i >= 0; i -= 17) {
			BlockEntry entry = _cache.pin(key(i), _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(entry);
			IndexedMatrixValue imv = (IndexedMatrixValue) entry.getData();
			Assert.assertEquals(i + 1.0, ((MatrixBlock) imv.getValue()).sum(), 0.0);
			OOCCache.UnpinHandle handle = _cache.unpin(entry, _consumerB);
			if(!handle.isCommitted())
				handle.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		}
		Assert.assertEquals(0, _consumerB.getUsedMemory());
	}

	@Test
	public void testDeferredUnpinResolvedByLaterPin() throws Exception {
		OOCCacheImpl cache = new OOCCacheImpl(_io, 0, 0);
		try {
			_producer.reserveBlocking(BYTES);
			BlockEntry entry = cache.putPinned(key(10_000), payload(10_000), BYTES, _producer);

			OOCCache.UnpinHandle deferred = cache.unpin(entry, _producer);
			Assert.assertFalse(deferred.isCommitted());
			Assert.assertFalse(deferred.getCompletionFuture().isDone());
			Assert.assertEquals(BYTES, _producer.getUsedMemory());

			BlockEntry repinned = cache.pinIfLive(key(10_000).getStreamId(), key(10_000).getSequenceNumber(), _consumerB);
			Assert.assertNotNull(repinned);
			Assert.assertTrue(deferred.getCompletionFuture().isDone());
			Assert.assertEquals(0, _producer.getUsedMemory());
			Assert.assertEquals(BYTES, _consumerB.getUsedMemory());

			OOCCache.UnpinHandle second = cache.unpin(repinned, _consumerB);
			Assert.assertFalse(second.isCommitted());
			Assert.assertEquals(BYTES, _consumerB.getUsedMemory());

			cache.updateLimits(BYTES, BYTES);
			second.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertEquals(0, _consumerB.getUsedMemory());
			Assert.assertEquals(BYTES, cache.getOwnedCacheSize());
		}
		finally {
			cache.shutdown();
		}
	}

	@Ignore("Manual multi-GiB stress test; enable when validating large spill/replay behavior.")
	@Test
	public void mtest() throws Exception {
		for(int i = 0; i < 2; i++) {
			System.out.println("Iteration: " + i);
			long ms = System.currentTimeMillis();
			testManualFourGiBSpillAndReplay();
			System.out.println((System.currentTimeMillis() - ms) + " ms");
		}
	}

	private void testManualFourGiBSpillAndReplay() throws Exception {
		boolean oldOOCStats = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		Statistics.resetOOCEvictionStats();
		OOCMatrixIOHandler io = new OOCMatrixIOHandler();
		GlobalMemoryBroker broker = new GlobalMemoryBroker(MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker, MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		SyncMemoryAllowance reader = new SyncMemoryAllowance(broker, MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		OOCCacheImpl cache = new OOCCacheImpl(io, MANUAL_HARD_BYTES, MANUAL_CACHE_BYTES);
		try {
			MatrixBlock tile = new MatrixBlock(MANUAL_STRESS_ROWS, MANUAL_STRESS_COLS, 1.0);
			long bytes = MANUAL_STRESS_ROWS * (long) MANUAL_STRESS_COLS * 8;
			int blocks = Math.toIntExact((MANUAL_STRESS_BYTES + bytes - 1) / bytes);
			OOCCache.UnpinHandle handle = null;

			for(int i = 0; i < blocks; i++) {
				producer.reserveBlocking(bytes);
				BlockEntry entry = cache.putPinned(key(i), new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1), tile),
					bytes, producer);
				handle = cache.unpin(entry, producer);
			}
			if(!handle.isCommitted())
				handle.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			System.out.println("Spilling done!");

			waitFor(() -> cache.getOwnedCacheSize() <= MANUAL_CACHE_BYTES);
			int parallelism = 128;

			double expectedSum = MANUAL_STRESS_ROWS * (double) MANUAL_STRESS_COLS;
			for(int scan = 0; scan < 2; scan++) {
				DoubleAdder sum = new DoubleAdder();
				AtomicInteger inflight = new AtomicInteger(0);
				for(int i = 0; i < blocks; i++) {
					waitFor(() -> inflight.get() < parallelism);
					inflight.incrementAndGet();
					cache.pin(key(i), reader).thenAccept(entry -> {
						Assert.assertNotNull(entry);
						sum.add(((MatrixBlock) ((IndexedMatrixValue) entry.getData()).getValue()).sum());
						cache.unpin(entry, reader);
						inflight.decrementAndGet();
					});
				}
				waitFor(() -> inflight.get() == 0);
				Assert.assertEquals(expectedSum * blocks, sum.sum(), 0.0);
				Assert.assertEquals(0, reader.getUsedMemory());
				waitFor(() -> cache.getOwnedCacheSize() <= MANUAL_CACHE_BYTES);
			}
			System.out.printf("OOCCacheImpl manual 4GiB stress stats:%n%s%n", Statistics.displayOOCEvictionStats());
		}
		finally {
			cache.shutdown();
			io.shutdown();
			producer.destroy();
			reader.destroy();
			DMLScript.OOC_STATISTICS = oldOOCStats;
		}
	}

	private static BlockKey key(int i) {
		return new BlockKey(1, i);
	}

	private static IndexedMatrixValue payload(int i) {
		return new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1), new MatrixBlock(1, 1, i + 1.0));
	}

	private static void waitFor(BooleanSupplier condition) throws InterruptedException {
		long start = System.nanoTime();
		long timeout = TimeUnit.SECONDS.toNanos(WAIT_TIMEOUT_SEC);
		while(System.nanoTime() - start < timeout) {
			if(condition.getAsBoolean())
				return;
			Thread.sleep(10);
		}
		Assert.assertTrue(condition.getAsBoolean());
	}

	private static final class FakeIOHandler implements OOCIOHandler {
		private final Map<BlockKey, Object> _spilled = new HashMap<>();
		private final AtomicInteger _evictions = new AtomicInteger();

		@Override
		public void shutdown() {
			_spilled.clear();
		}

		@Override
		public CompletableFuture<Void> scheduleEviction(BlockEntry block) {
			_spilled.put(block.getKey(), BlockEntryTestAccess.getDataUnsafe(block));
			_evictions.incrementAndGet();
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public CompletableFuture<BlockEntry> scheduleRead(BlockEntry block) {
			Object data = _spilled.get(block.getKey());
			if(data == null)
				return CompletableFuture.completedFuture(null);
			BlockEntryTestAccess.setDataUnsafe(block, data);
			return CompletableFuture.completedFuture(block);
		}

		@Override
		public void prioritizeRead(BlockKey key, double priority) {
		}

		@Override
		public CompletableFuture<Boolean> scheduleDeletion(BlockEntry block) {
			_spilled.remove(block.getKey());
			return CompletableFuture.completedFuture(true);
		}

		@Override
		public void registerSourceLocation(BlockKey key, SourceBlockDescriptor descriptor) {
		}

		@Override
		public CompletableFuture<SourceReadResult> scheduleSourceRead(SourceReadRequest request) {
			return CompletableFuture.failedFuture(new UnsupportedOperationException());
		}

		@Override
		public CompletableFuture<SourceReadResult> continueSourceRead(SourceReadContinuation continuation,
			long maxBytesInFlight) {
			return CompletableFuture.failedFuture(new UnsupportedOperationException());
		}

		private int getEvictionCount() {
			return _evictions.get();
		}
	}
}
