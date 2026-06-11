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
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
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

		private ManagedPayload<IndexedMatrixValue> payload(double value) {
			producer.reserveBlocking(BYTES);
			IndexedMatrixValue imv =
				new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, value));
			return new ManagedPayload<>(imv, BYTES, producer);
		}

		private void close() {
			table.close();
			cache.shutdown();
			producer.destroy();
			region.destroy();
		}
	}
}
