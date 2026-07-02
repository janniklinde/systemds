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

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCStoreBinding;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.ToLongFunction;

public class OOCEvictionPolicyTest {
	private static final long STREAM_ID = 17;
	private static final long BYTES = 1000;
	private static final long WAIT_TIMEOUT_SEC = 10;

	@Test
	public void testPhysicalCacheEvictsInCustomPolicyOrder() throws Exception {
		RecordingIOHandler io = new RecordingIOHandler();
		GlobalMemoryBroker broker = new GlobalMemoryBroker(64 * BYTES);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker, 32 * BYTES);
		OOCCacheImpl cache = new OOCCacheImpl(io, 8 * BYTES, 8 * BYTES);
		try {
			cache.addEvictionPolicy(STREAM_ID, OOCEvictionPolicyTest::physicalScore);
			BlockEntry[] entries = new BlockEntry[4];
			for(int i = 0; i < entries.length; i++) {
				producer.reserveBlocking(BYTES);
				entries[i] = cache.putPinned(STREAM_ID, i, value(i), BYTES, producer);
			}
			for(BlockEntry entry : entries)
				await(cache.unpin(entry, producer));
			Assert.assertEquals(4 * BYTES, cache.getOwnedCacheSize());
			Assert.assertEquals(0, io.evictionCount());

			cache.updateLimits(8 * BYTES, 2 * BYTES);
			waitFor(() -> io.evictionCount() >= 2);

			List<BlockKey> evicted = io.evictedKeys();
			Assert.assertEquals(new BlockKey(STREAM_ID, 3), evicted.get(0));
			Assert.assertEquals(new BlockKey(STREAM_ID, 1), evicted.get(1));
		}
		finally {
			cache.shutdown();
			producer.destroy();
		}
	}

	@Test
	public void testPackedCacheEvictsPackInCustomLogicalPolicyOrder() throws Exception {
		RecordingIOHandler io = new RecordingIOHandler();
		GlobalMemoryBroker broker = new GlobalMemoryBroker(128 * BYTES);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker, 64 * BYTES);
		SyncMemoryAllowance reader = new SyncMemoryAllowance(broker, 64 * BYTES);
		OOCPackedCache cache = new OOCPackedCache(new OOCCacheImpl(io, 16 * BYTES, 16 * BYTES),
			2 * BYTES, 4 * BYTES, -1);
		try {
			cache.addEvictionPolicy(STREAM_ID, ix -> ix >= 4 ? 100 : 0);
			BlockEntry[] entries = new BlockEntry[8];
			for(int i = 0; i < entries.length; i++) {
				producer.reserveBlocking(BYTES);
				entries[i] = cache.putPinned(STREAM_ID, i, value(i), BYTES, producer);
			}
			cache.flushPacks();
			OOCCache.UnpinHandle[] unpins = new OOCCache.UnpinHandle[entries.length];
			for(int i = 0; i < entries.length; i++)
				unpins[i] = cache.unpin(entries[i], producer);
			for(OOCCache.UnpinHandle unpin : unpins)
				await(unpin);
			Assert.assertEquals(8 * BYTES, cache.getOwnedCacheSize());
			Assert.assertEquals(0, io.evictionCount());

			cache.updateLimits(16 * BYTES, 4 * BYTES);
			waitFor(() -> cache.getOwnedCacheSize() <= 4 * BYTES);
			Assert.assertTrue(io.evictionCount() >= 1);
			Assert.assertEquals(4 * BYTES, cache.getOwnedCacheSize());

			int readsBefore = io.readCount();
			BlockEntry lowScore = cache.pin(STREAM_ID, 0, reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(lowScore);
			Assert.assertEquals("The low-score pack should remain resident.", readsBefore, io.readCount());
			await(cache.unpin(lowScore, reader));

			BlockEntry highScore = cache.pin(STREAM_ID, 4, reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertNotNull(highScore);
			Assert.assertTrue("The high-score pack should have been spilled and replayed.",
				io.readCount() > readsBefore);
			await(cache.unpin(highScore, reader));
		}
		finally {
			cache.shutdown();
			producer.destroy();
			reader.destroy();
		}
	}

	@Test
	public void testStoreBindingAdaptsMatrixIndexPolicyToLayout() throws Exception {
		RecordingIOHandler io = new RecordingIOHandler();
		GlobalMemoryBroker broker = new GlobalMemoryBroker(64 * BYTES);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker, 32 * BYTES);
		OOCCacheImpl cache = new OOCCacheImpl(io, 8 * BYTES, 8 * BYTES);
		OOCStoreBinding binding = new OOCStoreBinding(null, cache, STREAM_ID,
			OOCStoreLayout.of(ix -> Math.toIntExact((ix.getRowIndex() - 1) * 10 + ix.getColumnIndex() - 1),
				index -> new MatrixIndexes(index / 10 + 1L, index % 10 + 1L)),
			producer, 0, 1, List.of(),
			List.<ToLongFunction<MatrixIndexes>>of(ix -> ix.getRowIndex() * 100 + ix.getColumnIndex()));
		try {
			long[] ids = new long[] {0, 3, 10, 13};
			for(long id : ids) {
				producer.reserveBlocking(BYTES);
				BlockEntry entry = cache.putPinned(STREAM_ID, id, value((int) id), BYTES, producer);
				await(cache.unpin(entry, producer));
			}
			cache.updateLimits(8 * BYTES, 3 * BYTES);
			waitFor(() -> io.evictionCount() >= 1);
			Assert.assertEquals(new BlockKey(STREAM_ID, 13), io.evictedKeys().get(0));
		}
		finally {
			binding.close();
			cache.shutdown();
			producer.destroy();
		}
	}

	private static long physicalScore(long ix) {
		if(ix == 3)
			return 100;
		if(ix == 1)
			return 90;
		if(ix == 2)
			return 20;
		return 10;
	}

	private static IndexedMatrixValue value(int i) {
		return new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1), new MatrixBlock(1, 1, i + 1.0));
	}

	private static void await(OOCCache.UnpinHandle handle) throws Exception {
		if(!handle.isCommitted())
			handle.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
	}

	private static void waitFor(BooleanSupplier condition) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_TIMEOUT_SEC);
		while(!condition.getAsBoolean() && System.nanoTime() < deadline)
			Thread.sleep(1);
		Assert.assertTrue(condition.getAsBoolean());
	}

	private static final class RecordingIOHandler implements OOCIOHandler {
		private final Map<BlockKey, Object> spilled = new ConcurrentHashMap<>();
		private final List<BlockKey> evicted = Collections.synchronizedList(new ArrayList<>());
		private final AtomicInteger reads = new AtomicInteger();

		@Override
		public void shutdown() {
			spilled.clear();
			evicted.clear();
		}

		@Override
		public CompletableFuture<Void> scheduleEviction(BlockEntry block) {
			spilled.put(block.getKey(), BlockEntryTestAccess.getDataUnsafe(block));
			evicted.add(block.getKey());
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public OOCFuture<BlockEntry> scheduleRead(BlockEntry block) {
			reads.incrementAndGet();
			Object data = spilled.get(block.getKey());
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
			spilled.remove(block.getKey());
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

		private int evictionCount() {
			return evicted.size();
		}

		private int readCount() {
			return reads.get();
		}

		private List<BlockKey> evictedKeys() {
			synchronized(evicted) {
				return new ArrayList<>(evicted);
			}
		}
	}
}
