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
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreImpl;
import org.apache.sysds.runtime.ooc.store.SequentialAccessPattern;
import org.apache.sysds.utils.Statistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class OOCCacheImplStressTest {
	private static final int BLOCKS = 512;
	private static final long BYTES = 1_000;
	private static final long WAIT_TIMEOUT_SEC = 60;
	private static final long GIB = 1L << 30;
	private static final long MANUAL_STRESS_BYTES = 4L * GIB;
	private static final long MANUAL_CACHE_BYTES = GIB;
	private static final long MANUAL_HARD_BYTES = MANUAL_CACHE_BYTES + (256L << 20);
	private static final int MANUAL_STRESS_ROWS = 500;
	private static final int MANUAL_STRESS_COLS = 1;
	private static final long MANUAL_PACK_SEAL_DELAY_MS = 5;

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

	@Test
	public void testPackedCacheGroupsSmallTilesAndReplaysWithFakeIO() throws Exception {
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(_io, 64 * BYTES, 16 * BYTES),
			2 * BYTES, 8 * BYTES, 0);
		try {
			BlockEntry[] entries = new BlockEntry[BLOCKS];
			OOCCache.UnpinHandle[] handles = new OOCCache.UnpinHandle[BLOCKS];

			for(int i = 0; i < BLOCKS; i++) {
				_producer.reserveBlocking(BYTES);
				entries[i] = cache.putPinned(key(i), payload(i), BYTES, _producer);
			}

			for(int i = 0; i < BLOCKS; i++)
				handles[i] = cache.unpin(entries[i], _producer);
			for(OOCCache.UnpinHandle handle : handles)
				if(!handle.isCommitted())
					handle.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

			cache.flushPacks();
			Assert.assertEquals(0, _producer.getUsedMemory());
			waitFor(() -> cache.getOwnedCacheSize() <= 16 * BYTES);
			Assert.assertTrue("Packed physical entries should be evicted.", _io.getEvictionCount() > 0);

			int readsBefore = _io.getReadCount();
			BlockEntry last = cache.pin(key(BLOCKS - 1), _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(last);
			Assert.assertTrue("Fake IO should replay an evicted packed block.", _io.getReadCount() > readsBefore);
			Assert.assertEquals((double) BLOCKS, ((MatrixBlock) ((IndexedMatrixValue) last.getData()).getValue()).sum(),
				0.0);
			Assert.assertEquals(8 * BYTES, _consumerB.getUsedMemory());

			BlockEntry samePack = cache.pin(key(BLOCKS - 2), _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(samePack);
			Assert.assertEquals((double) BLOCKS - 1,
				((MatrixBlock) ((IndexedMatrixValue) samePack.getData()).getValue()).sum(), 0.0);
			Assert.assertEquals("Same allowance pins in one pack should charge once.",
				8 * BYTES, _consumerB.getUsedMemory());

			OOCCache.UnpinHandle first = cache.unpin(last, _consumerB);
			if(!first.isCommitted())
				first.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertEquals("First logical unpin should keep the physical pack pinned.",
				8 * BYTES, _consumerB.getUsedMemory());

			OOCCache.UnpinHandle second = cache.unpin(samePack, _consumerB);
			if(!second.isCommitted())
				second.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertEquals(0, _consumerB.getUsedMemory());
		}
		finally {
			cache.shutdown();
		}
	}

	@Test
	public void testPackedCacheBatchAndSealedPackApisWithFakeIO() throws Exception {
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(_io, 64 * BYTES, 16 * BYTES),
			2 * BYTES, 8 * BYTES, 64 * BYTES, 1, 0, 0);
		try {
			long[] batchIds = new long[] {0, 1};
			Object[] batchData = new Object[] {payload(0), payload(1)};
			long[] batchSizes = new long[] {BYTES, BYTES};
			_producer.reserveBlocking(2 * BYTES);
			BlockEntry[] batchEntries = cache.putPackPinned(1, batchIds, batchData, batchSizes, _producer);

			_producer.reserveBlocking(BYTES);
			BlockEntry streamEntry = cache.putPinned(2, 0, payload(2), BYTES, _producer);

			_producer.reserveBlocking(BYTES);
			BlockEntry otherStreamEntry = cache.putPinned(4, 0, payload(3), BYTES, _producer);

			long[] sealedIds = new long[] {10, 11};
			Object[] sealedData = new Object[] {payload(10), payload(11)};
			long[] sealedSizes = new long[] {BYTES, BYTES};
			_producer.reserveBlocking(2 * BYTES);
			BlockEntry sealedPhysical = cache.putSealedPackPinned(3, sealedIds, sealedData, sealedSizes, _producer);

			for(BlockEntry entry : batchEntries)
				cache.unpin(entry, _producer);
			cache.unpin(streamEntry, _producer);
			cache.unpin(otherStreamEntry, _producer);
			cache.unpin(sealedPhysical, _producer);
			cache.flushPacks();
			waitFor(() -> _producer.getUsedMemory() == 0);

			BlockEntry batchReplay = cache.pin(1, 1, _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(batchReplay);
			Assert.assertEquals(2.0, ((MatrixBlock) ((IndexedMatrixValue) batchReplay.getData()).getValue()).sum(), 0.0);
			cache.unpin(batchReplay, _consumerB);
			waitFor(() -> _consumerB.getUsedMemory() == 0);

			BlockEntry streamReplay = cache.pin(2, 0, _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(streamReplay);
			Assert.assertEquals(3.0, ((MatrixBlock) ((IndexedMatrixValue) streamReplay.getData()).getValue()).sum(), 0.0);
			Assert.assertEquals("A per-stream pack must not include tiles from another stream.",
				BYTES, _consumerB.getUsedMemory());

			BlockEntry otherStreamReplay = cache.pin(4, 0, _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(otherStreamReplay);
			Assert.assertEquals(4.0,
				((MatrixBlock) ((IndexedMatrixValue) otherStreamReplay.getData()).getValue()).sum(), 0.0);
			Assert.assertEquals(2 * BYTES, _consumerB.getUsedMemory());
			cache.unpin(streamReplay, _consumerB);
			cache.unpin(otherStreamReplay, _consumerB);

			BlockEntry sealedReplay = cache.pin(3, 11, _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(sealedReplay);
			Assert.assertEquals(12.0, ((MatrixBlock) ((IndexedMatrixValue) sealedReplay.getData()).getValue()).sum(), 0.0);
			cache.unpin(sealedReplay, _consumerB);
			waitFor(() -> _consumerB.getUsedMemory() == 0);
		}
		finally {
			cache.shutdown();
		}
	}

	@Test
	public void testMaterializedStoreOfflinePackedReplayWithFakeIO() throws Exception {
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(_io, 64 * BYTES, 16 * BYTES),
			2 * BYTES, 8 * BYTES, 0);
		MaterializedStoreImpl<IndexedMatrixValue> store = new MaterializedStoreImpl<>(cache, 3);
		try {
			for(int i = 0; i < BLOCKS; i++) {
				_producer.reserveBlocking(BYTES);
				store.publishPinned(i, payload(i), BYTES, _producer);
			}
			store.complete();
			waitFor(() -> _producer.getUsedMemory() == 0);

			MaterializedStore.Reader<IndexedMatrixValue> first =
				store.openReader(new SequentialAccessPattern(BLOCKS), _consumerB, 32);
			MaterializedStore.Reader<IndexedMatrixValue> second =
				store.openReader(new SequentialAccessPattern(BLOCKS), _consumerB, 32);
			store.sealReaders();

			consumeStoreReader(first);
			waitFor(() -> _consumerB.getUsedMemory() == 0);
			consumeStoreReader(second);
			waitFor(() -> _consumerB.getUsedMemory() == 0);

			Assert.assertNull(cache.pin(3, 0, _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS));
		}
		finally {
			store.close();
			cache.shutdown();
		}
	}

	@Test
	public void testMaterializedStoreOpportunisticPackedReplayWithFakeIO() throws Exception {
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(_io, 64 * BYTES, 16 * BYTES),
			2 * BYTES, 8 * BYTES, 0);
		MaterializedStoreImpl<IndexedMatrixValue> store = new MaterializedStoreImpl<>(cache, 5);
		try {
			for(int i = 0; i < BLOCKS; i++) {
				_producer.reserveBlocking(BYTES);
				store.publishPinned(i, payload(i), BYTES, _producer);
			}
			store.complete();
			int expectedPacks = cache.getPackGroupCount();
			waitFor(() -> _producer.getUsedMemory() == 0);

			MaterializedStore.PackReader<IndexedMatrixValue> first =
				store.openOpportunisticReader(new SequentialAccessPattern(BLOCKS), _consumerB, 8);
			MaterializedStore.PackReader<IndexedMatrixValue> second =
				store.openOpportunisticReader(new SequentialAccessPattern(BLOCKS), _consumerB, 8);
			store.sealReaders();

			Assert.assertEquals(expectedPacks, consumeStorePackReader(first));
			waitFor(() -> _consumerB.getUsedMemory() == 0);
			Assert.assertEquals(expectedPacks, consumeStorePackReader(second));
			waitFor(() -> _consumerB.getUsedMemory() == 0);

			Assert.assertNull(cache.pin(5, 0, _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS));
		}
		finally {
			store.close();
			cache.shutdown();
		}
	}

	@Test
	public void testMaterializedStoreOpportunisticPackedReplayWithRealIO() throws Exception {
		OOCMatrixIOHandler io = new OOCMatrixIOHandler();
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(io, 64 * BYTES, 16 * BYTES),
			2 * BYTES, 8 * BYTES, 0);
		MaterializedStoreImpl<IndexedMatrixValue> store = new MaterializedStoreImpl<>(cache, 6);
		try {
			for(int i = 0; i < BLOCKS; i++) {
				_producer.reserveBlocking(BYTES);
				store.publishPinned(i, payload(i), BYTES, _producer);
			}
			store.complete();
			int expectedPacks = cache.getPackGroupCount();
			waitFor(() -> _producer.getUsedMemory() == 0);
			waitFor(() -> cache.getOwnedCacheSize() <= 16 * BYTES);

			MaterializedStore.PackReader<IndexedMatrixValue> first =
				store.openOpportunisticReader(new SequentialAccessPattern(BLOCKS), _consumerB, 8);
			MaterializedStore.PackReader<IndexedMatrixValue> second =
				store.openOpportunisticReader(new SequentialAccessPattern(BLOCKS), _consumerB, 8);
			store.sealReaders();

			Assert.assertEquals(expectedPacks, consumeStorePackReader(first));
			waitFor(() -> _consumerB.getUsedMemory() == 0);
			Assert.assertEquals(expectedPacks, consumeStorePackReader(second));
			waitFor(() -> _consumerB.getUsedMemory() == 0);

			Assert.assertNull(cache.pin(6, 0, _consumerB).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS));
		}
		finally {
			store.close();
			cache.shutdown();
			io.shutdown();
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

	@Ignore("Manual multi-GiB packed in-memory stress test; enable when validating packed logical access overhead.")
	@Test
	public void mtestPackedInMemory() throws Exception {
		for(int i = 0; i < 3; i++) {
			System.out.println("Packed in-memory iteration: " + i);
			long ms = System.currentTimeMillis();
			testManualPackedFourGiBInMemoryAccess();
			System.out.println((System.currentTimeMillis() - ms) + " ms");
		}
	}

	@Ignore("Manual multi-GiB packed spill/replay stress test; enable when validating packed disk IO.")
	@Test
	public void mtestPackedSpillAndReplay() throws Exception {
		for(int i = 0; i < 2; i++) {
			System.out.println("Packed spill/replay iteration: " + i);
			long ms = System.currentTimeMillis();
			testManualPackedFourGiBSpillAndReplay();
			System.out.println((System.currentTimeMillis() - ms) + " ms");
		}
	}

	@Ignore("Manual multi-GiB materialized-store stress test; enable when validating offline packed replay.")
	@Test
	public void mtestMaterializedStorePackedSpillAndReplay() throws Exception {
		for(int i = 0; i < 2; i++) {
			System.out.println("Materialized store packed spill/replay iteration: " + i);
			long ms = System.currentTimeMillis();
			testManualMaterializedStorePackedFourGiBSpillAndReplay();
			System.out.println((System.currentTimeMillis() - ms) + " ms");
		}
	}

	private void testManualPackedFourGiBInMemoryAccess() throws Exception {
		OOCMatrixIOHandler io = new OOCMatrixIOHandler();
		long tileBytes = MANUAL_STRESS_ROWS * (long) MANUAL_STRESS_COLS * 8;
		int blocks = Math.toIntExact((MANUAL_STRESS_BYTES + tileBytes - 1) / tileBytes);
		long packTargetBytes = 4L << 20;
		long packCount = (MANUAL_STRESS_BYTES + packTargetBytes - 1) / packTargetBytes;
		long physicalBytes = (packCount + 1) * packTargetBytes;
		long hardBytes = physicalBytes + (256L << 20);
		GlobalMemoryBroker broker = new GlobalMemoryBroker(MANUAL_STRESS_BYTES + hardBytes);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker, MANUAL_STRESS_BYTES + hardBytes);
		SyncMemoryAllowance reader = new SyncMemoryAllowance(broker, MANUAL_STRESS_BYTES + hardBytes);
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(io, hardBytes, physicalBytes),
			64L << 10, packTargetBytes, MANUAL_PACK_SEAL_DELAY_MS);
		try {
			MatrixBlock tile = new MatrixBlock(MANUAL_STRESS_ROWS, MANUAL_STRESS_COLS, 1.0);

			long loadStart = System.currentTimeMillis();
			for(int i = 0; i < blocks; i++) {
				producer.reserveBlocking(tileBytes);
				BlockEntry entry = cache.putPinned(key(i), new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1), tile),
					tileBytes, producer);
				cache.unpin(entry, producer);
			}
			cache.flushPacks();
			waitFor(() -> producer.getUsedMemory() == 0);
			System.out.println("Packed in-memory load done in " + (System.currentTimeMillis() - loadStart) + " ms");
			System.out.println("Packed in-memory blocks=" + blocks + ", tileBytes=" + tileBytes +
				", physicalBudget=" + physicalBytes + ", owned=" + cache.getOwnedCacheSize());

			for(int scan = 0; scan < 2; scan++) {
				long scanStart = System.currentTimeMillis();
				for(int i = 0; i < blocks; i++) {
					BlockEntry entry = cache.pin(key(i), reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
					Assert.assertNotNull("Null packed pin at scan=" + scan + ", block=" + i, entry);
					cache.unpin(entry, reader);
				}
				waitFor(() -> reader.getUsedMemory() == 0);
				System.out.println("Packed in-memory scan " + scan + " done in " +
					(System.currentTimeMillis() - scanStart) + " ms");
			}
		}
		finally {
			cache.shutdown();
			io.shutdown();
			producer.destroy();
			reader.destroy();
		}
	}

	private void testManualPackedFourGiBSpillAndReplay() throws Exception {
		boolean oldOOCStats = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		Statistics.resetOOCEvictionStats();
		OOCMatrixIOHandler io = new OOCMatrixIOHandler();
		long tileBytes = MANUAL_STRESS_ROWS * (long) MANUAL_STRESS_COLS * 8;
		int blocks = Math.toIntExact((MANUAL_STRESS_BYTES + tileBytes - 1) / tileBytes);
		long packTargetBytes = 1L << 19;
		GlobalMemoryBroker broker = new GlobalMemoryBroker(MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker, MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		SyncMemoryAllowance reader = new SyncMemoryAllowance(broker, MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(io, MANUAL_HARD_BYTES, MANUAL_CACHE_BYTES),
			64L << 10, packTargetBytes, MANUAL_PACK_SEAL_DELAY_MS);
		try {
			MatrixBlock tile = new MatrixBlock(MANUAL_STRESS_ROWS, MANUAL_STRESS_COLS, 1.0);

			long loadStart = System.currentTimeMillis();
			for(int i = 0; i < blocks; i++) {
				producer.reserveBlocking(tileBytes);
				BlockEntry entry = cache.putPinned(key(i), new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1), tile),
					tileBytes, producer);
				cache.unpin(entry, producer);
			}
			cache.flushPacks();
			waitFor(() -> producer.getUsedMemory() == 0);
			waitFor(() -> cache.getOwnedCacheSize() <= MANUAL_CACHE_BYTES);
			System.out.println("Packed spill/replay load done in " + (System.currentTimeMillis() - loadStart) + " ms");
			System.out.println("Packed spill/replay blocks=" + blocks + ", tileBytes=" + tileBytes +
				", owned=" + cache.getOwnedCacheSize());

			int parallelism = 1024*4;
			for(int scan = 0; scan < 2; scan++) {
				long scanStart = System.currentTimeMillis();
				AtomicInteger inflight = new AtomicInteger(0);
				int scanId = scan;
				for(int i = 0; i < blocks; i++) {
					waitFor(() -> inflight.get() < parallelism);
					inflight.incrementAndGet();
					int block = i;
					cache.pin(key(i).getStreamId(), key(i).getSequenceNumber(), reader).thenAccept(entry -> {
						Assert.assertNotNull("Null packed pin at scan=" + scanId + ", block=" + block, entry);
						cache.unpin(entry, reader);
						inflight.decrementAndGet();
					});
				}
				waitFor(() -> inflight.get() == 0);
				waitFor(() -> reader.getUsedMemory() == 0);
				waitFor(() -> cache.getOwnedCacheSize() <= MANUAL_CACHE_BYTES);
				System.out.println("Packed spill/replay scan " + scan + " done in " +
					(System.currentTimeMillis() - scanStart) + " ms");
			}
			System.out.printf("OOCPackedCache manual 4GiB stress stats:%n%s%n", Statistics.displayOOCEvictionStats());
		}
		finally {
			cache.shutdown();
			io.shutdown();
			producer.destroy();
			reader.destroy();
			DMLScript.OOC_STATISTICS = oldOOCStats;
		}
	}

	private void testManualMaterializedStorePackedFourGiBSpillAndReplay() throws Exception {
		boolean oldOOCStats = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		Statistics.resetOOCEvictionStats();
		OOCMatrixIOHandler io = new OOCMatrixIOHandler();
		long tileBytes = MANUAL_STRESS_ROWS * (long) MANUAL_STRESS_COLS * 8;
		int blocks = Math.toIntExact((MANUAL_STRESS_BYTES + tileBytes - 1) / tileBytes);
		long packTargetBytes = 1L << 19;
		GlobalMemoryBroker broker = new GlobalMemoryBroker(MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker, MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		SyncMemoryAllowance readerAllowance =
			new SyncMemoryAllowance(broker, MANUAL_STRESS_BYTES + MANUAL_HARD_BYTES);
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(io, MANUAL_HARD_BYTES, MANUAL_CACHE_BYTES),
			64L << 10, packTargetBytes, MANUAL_PACK_SEAL_DELAY_MS);
		MaterializedStoreImpl<IndexedMatrixValue> store = new MaterializedStoreImpl<>(cache, 1);
		try {
			MatrixBlock tile = new MatrixBlock(MANUAL_STRESS_ROWS, MANUAL_STRESS_COLS, 1.0);

			long loadStart = System.currentTimeMillis();
			for(int i = 0; i < blocks; i++) {
				producer.reserveBlocking(tileBytes);
				store.publishPinned(i, new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1), tile),
					tileBytes, producer);
			}
			store.complete();
			waitFor(() -> producer.getUsedMemory() == 0);
			waitFor(() -> cache.getOwnedCacheSize() <= MANUAL_CACHE_BYTES);
			System.out.println("Materialized store load done in " +
				(System.currentTimeMillis() - loadStart) + " ms");
			System.out.println("Materialized store blocks=" + blocks + ", tileBytes=" + tileBytes +
				", owned=" + cache.getOwnedCacheSize());

			MaterializedStore.Reader<IndexedMatrixValue> first = store.openReader(
				new SequentialAccessPattern(blocks), readerAllowance, 4096);
			MaterializedStore.Reader<IndexedMatrixValue> second = store.openReader(
				new SequentialAccessPattern(blocks), readerAllowance, 4096);
			store.sealReaders();

			List<MaterializedStore.Reader<IndexedMatrixValue>> scans = List.of(first, second);
			for(int scan = 0; scan < scans.size(); scan++) {
				long scanStart = System.currentTimeMillis();
				int expected = 0;
				MaterializedStore.Reader<IndexedMatrixValue> reader = scans.get(scan);
				while(reader.hasNext()) {
					try(MaterializedStore.Lease<IndexedMatrixValue> lease = reader.next()) {
						Assert.assertEquals(expected, lease.index());
						Assert.assertNotNull(lease.value());
					}
					expected++;
				}
				reader.close();
				Assert.assertEquals(blocks, expected);
				waitFor(() -> readerAllowance.getUsedMemory() == 0);
				waitFor(() -> cache.getOwnedCacheSize() <= MANUAL_CACHE_BYTES);
				System.out.println("Materialized store scan " + scan + " done in " +
					(System.currentTimeMillis() - scanStart) + " ms");
			}
			System.out.printf("Materialized store packed 4GiB stress stats:%n%s%n",
				Statistics.displayOOCEvictionStats());
		}
		finally {
			store.close();
			cache.shutdown();
			io.shutdown();
			producer.destroy();
			readerAllowance.destroy();
			DMLScript.OOC_STATISTICS = oldOOCStats;
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

			for(int scan = 0; scan < 2; scan++) {
				AtomicInteger inflight = new AtomicInteger(0);
				for(int i = 0; i < blocks; i++) {
					waitFor(() -> inflight.get() < parallelism);
					inflight.incrementAndGet();
					cache.pin(key(i), reader).thenAccept(entry -> {
						Assert.assertNotNull(entry);
						cache.unpin(entry, reader);
						inflight.decrementAndGet();
					});
				}
				waitFor(() -> inflight.get() == 0);
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

	private static void consumeStoreReader(MaterializedStore.Reader<IndexedMatrixValue> reader)
		throws InterruptedException {
		int expected = 0;
		while(reader.hasNext()) {
			try(MaterializedStore.Lease<IndexedMatrixValue> lease = reader.next()) {
				Assert.assertEquals(expected, lease.index());
				Assert.assertEquals(expected + 1.0,
					((MatrixBlock)lease.value().getValue()).sum(), 0.0);
				if(expected == 0) {
					try(MaterializedStore.Lease<IndexedMatrixValue> retained = lease.retain()) {
						Assert.assertSame(lease.value(), retained.value());
					}
				}
			}
			expected++;
		}
		reader.close();
		Assert.assertEquals(BLOCKS, expected);
	}

	private static int consumeStorePackReader(MaterializedStore.PackReader<IndexedMatrixValue> reader)
		throws InterruptedException {
		BitSet consumed = new BitSet(BLOCKS);
		int packs = 0;
		while(reader.hasNext()) {
			try(MaterializedStore.PackLease<IndexedMatrixValue> pack = reader.nextPack()) {
				packs++;
				for(int slot = 0; slot < pack.size(); slot++) {
					int index = pack.index(slot);
					Assert.assertFalse(consumed.get(index));
					Assert.assertEquals(index + 1.0,
						((MatrixBlock)pack.value(slot).getValue()).sum(), 0.0);
					consumed.set(index);
				}
			}
		}
		reader.close();
		Assert.assertEquals(BLOCKS, consumed.cardinality());
		return packs;
	}

	private static void waitFor(BooleanSupplier condition) throws InterruptedException {
		long start = System.nanoTime();
		long timeout = TimeUnit.SECONDS.toNanos(WAIT_TIMEOUT_SEC);
		while(System.nanoTime() - start < timeout) {
			if(condition.getAsBoolean())
				return;
			Thread.sleep(1);
		}
		Assert.assertTrue(condition.getAsBoolean());
	}

	private static final class FakeIOHandler implements OOCIOHandler {
		private final Map<BlockKey, Object> _spilled = new HashMap<>();
		private final AtomicInteger _evictions = new AtomicInteger();
		private final AtomicInteger _reads = new AtomicInteger();

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
		public OOCFuture<BlockEntry> scheduleRead(BlockEntry block) {
			_reads.incrementAndGet();
			Object data = _spilled.get(block.getKey());
			if(data == null)
				return OOCFuture.completed(null);
			BlockEntryTestAccess.setDataUnsafe(block, data);
			return OOCFuture.completed(block);
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

		private int getReadCount() {
			return _reads.get();
		}
	}
}
