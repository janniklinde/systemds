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

package org.apache.sysds.test.component.ooc.store;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;
import org.junit.Assert;
import org.junit.Test;

public class OperatorStateTableTest {
	private static final long BYTES = 1000;
	private static final long STREAM_ID = 9;

	@Test
	public void testInstallTakeRoundtrip() throws Exception {
		Fixture f = new Fixture();
		try {
			f.table.install(5, f.payload(7.0));
			//the producer reservation transferred to the cache through unpin
			Assert.assertEquals(0, f.producer.getUsedMemory());
			Assert.assertEquals(BYTES, f.cache.getOwnedCacheSize());

			OperatorStateTable.StateLease<IndexedMatrixValue> lease = f.table.take(5).get(10, TimeUnit.SECONDS);
			Assert.assertNotNull(lease);
			Assert.assertEquals(7.0, ((MatrixBlock)lease.value().getValue()).get(0, 0), 0.0);
			Assert.assertEquals(BYTES, f.region.getUsedMemory());
			lease.close();
			Assert.assertEquals(0, f.region.getUsedMemory());
			//exactly-once consumption: the slot and the cache entry are gone
			Assert.assertNull(f.table.take(5).get(10, TimeUnit.SECONDS));
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testDoubleInstallThrows() throws Exception {
		Fixture f = new Fixture();
		try {
			f.table.install(1, f.payload(1.0));
			ManagedPayload<IndexedMatrixValue> second = f.payload(2.0);
			try {
				f.table.install(1, second);
				Assert.fail("Installing into an occupied slot must fail");
			}
			catch(IllegalStateException expected) {
				//the rejected payload stays with the caller
				second.release();
			}
			try(OperatorStateTable.StateLease<IndexedMatrixValue> lease = f.table.take(1).get(10, TimeUnit.SECONDS)) {
				Assert.assertEquals(1.0, ((MatrixBlock)lease.value().getValue()).get(0, 0), 0.0);
			}
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testClearDropsValue() throws Exception {
		Fixture f = new Fixture();
		try {
			f.table.install(2, f.payload(3.0));
			Assert.assertEquals(BYTES, f.cache.getOwnedCacheSize());
			f.table.clear(2);
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
			Assert.assertNull(f.table.take(2).get(10, TimeUnit.SECONDS));
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testPeekDoesNotRemove() throws Exception {
		Fixture f = new Fixture();
		try {
			f.table.install(3, f.payload(4.0));
			try(OperatorStateTable.StateLease<IndexedMatrixValue> peeked = f.table.peek(3)) {
				Assert.assertNotNull(peeked);
				Assert.assertEquals(4.0, ((MatrixBlock)peeked.value().getValue()).get(0, 0), 0.0);
				Assert.assertEquals(BYTES, f.region.getUsedMemory());
			}
			Assert.assertEquals(0, f.region.getUsedMemory());
			try(OperatorStateTable.StateLease<IndexedMatrixValue> lease = f.table.take(3).get(10, TimeUnit.SECONDS)) {
				Assert.assertNotNull(lease);
			}
			Assert.assertNull(f.table.peek(3));
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testCloseDropsRemainingValues() throws Exception {
		Fixture f = new Fixture();
		try {
			f.table.install(0, f.payload(1.0));
			f.table.install(1, f.payload(2.0));
			Assert.assertEquals(2 * BYTES, f.cache.getOwnedCacheSize());
			f.table.close();
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testInstallOrTakeConcurrentReduction() throws Exception {
		Fixture f = new Fixture();
		int threads = 4;
		int valuesPerThread = 50;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		try {
			Future<?>[] tasks = new Future<?>[threads];
			for(int t = 0; t < threads; t++) {
				tasks[t] = pool.submit(() -> {
					for(int i = 0; i < valuesPerThread; i++)
						accumulate(f, 1.0);
					return null;
				});
			}
			for(Future<?> task : tasks)
				task.get(60, TimeUnit.SECONDS);

			try(OperatorStateTable.StateLease<IndexedMatrixValue> lease = f.table.take(0).get(10, TimeUnit.SECONDS)) {
				Assert.assertNotNull(lease);
				Assert.assertEquals(threads * valuesPerThread,
					((MatrixBlock)lease.value().getValue()).get(0, 0), 0.0);
			}
			Assert.assertEquals(0, f.producer.getUsedMemory());
			Assert.assertEquals(0, f.region.getUsedMemory());
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
		}
		finally {
			pool.shutdownNow();
			f.close();
		}
	}

	@Test
	public void testReferenceSurvivesCanonicalForget() throws Exception {
		Fixture f = new Fixture();
		BlockKey key = new BlockKey(17, 3);
		try {
			f.producer.reserveBlocking(BYTES);
			BlockEntry source = f.cache.putPinned(key, value(7.0), BYTES, f.producer);
			f.table.installReference(4, source);
			await(f.cache.unpin(source, f.producer));

			// Drop the source's canonical lifetime. The table's retained reference must remain valid.
			f.cache.dereference(key);
			f.cache.updateLimits(1L << 30, 0);
			awaitOwnedCache(f.cache, 0);
			try(OperatorStateTable.StateLease<IndexedMatrixValue> lease =
				f.table.take(4).get(10, TimeUnit.SECONDS)) {
				Assert.assertNotNull(lease);
				Assert.assertEquals(7.0, scalar(lease.value()), 0.0);
			}
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testReferenceInstallOrTakeLeavesIncomingPinned() throws Exception {
		Fixture f = new Fixture();
		BlockKey firstKey = new BlockKey(18, 0);
		BlockKey secondKey = new BlockKey(18, 1);
		try {
			f.producer.reserveBlocking(2 * BYTES);
			BlockEntry first = f.cache.putPinned(firstKey, value(3.0), BYTES, f.producer);
			Assert.assertNull(f.table.installReferenceOrTake(6, first).get(10, TimeUnit.SECONDS));
			await(f.cache.unpin(first, f.producer));
			f.cache.dereference(firstKey);

			BlockEntry second = f.cache.putPinned(secondKey, value(5.0), BYTES, f.producer);
			try(OperatorStateTable.StateLease<IndexedMatrixValue> existing =
				f.table.installReferenceOrTake(6, second).get(10, TimeUnit.SECONDS)) {
				Assert.assertNotNull(existing);
				Assert.assertEquals(3.0, scalar(existing.value()), 0.0);
				Assert.assertTrue("The unmatched incoming entry must remain with the caller.", second.isPinned());
			}

			await(f.cache.unpin(second, f.producer));
			f.cache.dereference(secondKey);
			Assert.assertEquals(0, f.producer.getUsedMemory());
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testPackedReferencesPreserveLogicalSlots() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 32);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance region = new SyncMemoryAllowance(broker);
		producer.setTargetMemory(1L << 30);
		region.setTargetMemory(1L << 30);
		OOCPackedCache cache = new OOCPackedCache(
			new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30),
			2 * BYTES, 2 * BYTES, 0, 0);
		OperatorStateTable<IndexedMatrixValue> table =
			new OperatorStateTable<>(cache, STREAM_ID, region);
		BlockKey firstKey = new BlockKey(21, 0);
		BlockKey secondKey = new BlockKey(21, 1);
		try {
			producer.reserveBlocking(2 * BYTES);
			BlockEntry first = cache.putPinned(firstKey, value(11.0), BYTES, producer);
			BlockEntry second = cache.putPinned(secondKey, value(13.0), BYTES, producer);

			table.installReference(5, first);
			table.installReference(9, second);
			table.installReference(12, first);
			OOCCache.UnpinHandle firstUnpin = cache.unpin(first, producer);
			OOCCache.UnpinHandle secondUnpin = cache.unpin(second, producer);
			await(firstUnpin);
			await(secondUnpin);
			cache.dereference(firstKey);
			cache.dereference(secondKey);
			Assert.assertNotNull("Table references must keep the first logical packed location addressable.",
				cache.getPackGroup(firstKey.getStreamId(), firstKey.getSequenceNumber()));
			Assert.assertNotNull("Table references must keep the second logical packed location addressable.",
				cache.getPackGroup(secondKey.getStreamId(), secondKey.getSequenceNumber()));
			cache.updateLimits(1L << 30, 0);
			awaitOwnedCache(cache, 0);

			OperatorStateTable.StateLease<IndexedMatrixValue> firstLease =
				table.take(5).get(10, TimeUnit.SECONDS);
			OperatorStateTable.StateLease<IndexedMatrixValue> secondLease =
				table.take(9).get(10, TimeUnit.SECONDS);
			OperatorStateTable.StateLease<IndexedMatrixValue> duplicateFirstLease =
				table.take(12).get(10, TimeUnit.SECONDS);
			Assert.assertNotNull(firstLease);
			Assert.assertNotNull(secondLease);
			Assert.assertNotNull(duplicateFirstLease);
			Assert.assertEquals(11.0, scalar(firstLease.value()), 0.0);
			Assert.assertEquals(13.0, scalar(secondLease.value()), 0.0);
			Assert.assertEquals(11.0, scalar(duplicateFirstLease.value()), 0.0);
			Assert.assertEquals("Logical pins from one pack and allowance charge the pack once.",
				2 * BYTES, region.getUsedMemory());
			firstLease.close();
			secondLease.close();
			duplicateFirstLease.close();
			awaitUsedMemory(region, 0);
		}
		finally {
			table.close();
			cache.shutdown();
			producer.destroy();
			region.destroy();
		}
	}

	@Test
	public void testInstallReferenceRequiresPinnedEntry() throws Exception {
		Fixture f = new Fixture();
		BlockKey key = new BlockKey(19, 0);
		try {
			f.producer.reserveBlocking(BYTES);
			BlockEntry source = f.cache.putPinned(key, value(2.0), BYTES, f.producer);
			await(f.cache.unpin(source, f.producer));
			try {
				f.table.installReference(7, source);
				Assert.fail("installReference must reject an unpinned entry");
			}
			catch(IllegalArgumentException expected) {
				//expected
			}
			try {
				f.table.installReferenceOrTake(7, source);
				Assert.fail("installReferenceOrTake must reject an unpinned entry");
			}
			catch(IllegalArgumentException expected) {
				//expected
			}
			Assert.assertNull(f.table.take(7).get(10, TimeUnit.SECONDS));
			f.cache.dereference(key);
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testPipelinedReferencesShareOnePack() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 32);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance region = new SyncMemoryAllowance(broker);
		producer.setTargetMemory(1L << 30);
		region.setTargetMemory(1L << 30);
		//large pack target and disabled seal timer: only forced seals could fragment
		OOCPackedCache cache = new OOCPackedCache(
			new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30),
			2 * BYTES, 100 * BYTES, -1, 0);
		OperatorStateTable<IndexedMatrixValue> table =
			new OperatorStateTable<>(cache, STREAM_ID, region);
		try {
			//producer unpins of pending tiles stay deferred until the pack seals and transfers
			OOCCache.UnpinHandle[] unpins = new OOCCache.UnpinHandle[3];
			for(int i = 0; i < 3; i++) {
				//live consumer after a materialization boundary: park a reference per arriving tile
				producer.reserveBlocking(BYTES);
				BlockEntry tile = cache.putPinned(new BlockKey(22, i), value(i + 1.0), BYTES, producer);
				table.installReference(i, tile);
				unpins[i] = cache.unpin(tile, producer);
				cache.dereference(new BlockKey(22, i));
				Assert.assertEquals("Reference installs on pending tiles must not seal packs.",
					0, cache.getPackGroupCount());
			}
			cache.flushPacks();
			for(OOCCache.UnpinHandle unpin : unpins)
				await(unpin);
			Assert.assertEquals("Pipelined parked tiles must share one pack.", 1, cache.getPackGroupCount());

			for(int i = 0; i < 3; i++) {
				try(OperatorStateTable.StateLease<IndexedMatrixValue> lease =
					table.take(i).get(10, TimeUnit.SECONDS)) {
					Assert.assertNotNull(lease);
					Assert.assertEquals(i + 1.0, scalar(lease.value()), 0.0);
				}
			}
			awaitUsedMemory(region, 0);
			awaitOwnedCache(cache, 0);
		}
		finally {
			table.close();
			cache.shutdown();
			producer.destroy();
			region.destroy();
		}
	}

	/**
	 * The accumulator loop of GroupedReduce/MapMMChain on the new contract: install, or take the
	 * existing value, merge outside the slot, retry with the merged payload.
	 */
	private static void accumulate(Fixture f, double value) {
		ManagedPayload<IndexedMatrixValue> candidate = f.payload(value);
		try {
			while(true) {
				OperatorStateTable.StateLease<IndexedMatrixValue> existing =
					f.table.installOrTake(0, candidate).get(10, TimeUnit.SECONDS);
				if(existing == null)
					return; //installed
				double merged;
				try(existing) {
					merged = ((MatrixBlock)existing.value().getValue()).get(0, 0)
						+ ((MatrixBlock)candidate.value().getValue()).get(0, 0);
				}
				candidate.release();
				candidate = f.payload(merged);
			}
		}
		catch(Exception e) {
			candidate.release();
			throw new RuntimeException(e);
		}
	}

	private static IndexedMatrixValue value(double scalar) {
		return new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, scalar));
	}

	private static double scalar(IndexedMatrixValue value) {
		return ((MatrixBlock)value.getValue()).get(0, 0);
	}

	private static void await(OOCCache.UnpinHandle handle) throws Exception {
		if(!handle.isCommitted())
			handle.getCompletionFuture().get(10, TimeUnit.SECONDS);
	}

	private static void awaitUsedMemory(SyncMemoryAllowance allowance, long expected) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while(allowance.getUsedMemory() != expected && System.nanoTime() < deadline)
			Thread.sleep(1);
		Assert.assertEquals(expected, allowance.getUsedMemory());
	}

	private static void awaitOwnedCache(OOCCache cache, long expected) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while(cache.getOwnedCacheSize() != expected && System.nanoTime() < deadline)
			Thread.sleep(1);
		Assert.assertEquals(expected, cache.getOwnedCacheSize());
	}

	private static final class Fixture {
		private final GlobalMemoryBroker broker;
		private final SyncMemoryAllowance producer;
		private final SyncMemoryAllowance region;
		private final OOCCacheImpl cache;
		private final OperatorStateTable<IndexedMatrixValue> table;

		private Fixture() {
			broker = new GlobalMemoryBroker(1L << 32);
			producer = new SyncMemoryAllowance(broker);
			producer.setTargetMemory(1L << 30);
			region = new SyncMemoryAllowance(broker);
			region.setTargetMemory(1L << 30);
			cache = new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30);
			table = new OperatorStateTable<>(cache, STREAM_ID, region);
		}

		private ManagedPayload<IndexedMatrixValue> payload(double scalar) {
			producer.reserveBlocking(BYTES);
			return new ManagedPayload<>(value(scalar), BYTES, producer);
		}

		private void close() {
			table.close();
			cache.shutdown();
			producer.destroy();
			region.destroy();
		}
	}
}
