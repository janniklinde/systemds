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

package org.apache.sysds.test.component.ooc.planning;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreBinding;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;
import org.apache.sysds.runtime.ooc.store.SequentialAccessPattern;
import org.apache.sysds.runtime.ooc.store.StoreBackedStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * The planner seam of the new architecture: migrated primitives receive an OperatorStateTable over
 * the global cache (region allowance, fresh stream id), unmigrated primitives keep CachedAllowance,
 * and OOCStoreBinding coordinates counted reader registration with automatic sealing.
 */
public class OOCPlannerSeamTest {
	private static final int ROWS = 32;
	private static final int COLS = 1;
	private static final long WAIT_TIMEOUT_SEC = 10;

	@After
	public void tearDown() {
		OOCCacheManager.reset();
	}

	@Test
	public void testMigratedPrimitiveGetsStateTableOverGlobalCache() throws Exception {
		StubPrimitive stub = new StubPrimitive(true, false);
		stub.start();

		Assert.assertNotNull("Migrated primitives must receive a state table.", stub.table);
		Assert.assertNull("Migrated primitives must not receive a CachedAllowance.", stub.cache);
		Assert.assertTrue(stub.executed);

		//the table is functional over the global cache with the region allowance
		SyncMemoryAllowance producer = new SyncMemoryAllowance(GlobalMemoryBroker.get());
		producer.setTargetMemory(1L << 26);
		long bytes = tileBytes();
		try {
			producer.reserveBlocking(bytes);
			stub.table.install(3, new ManagedPayload<>(tile(0, 5.0), bytes, producer));
			try(OperatorStateTable.StateLease<IndexedMatrixValue> lease =
				stub.table.take(3).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
				Assert.assertNotNull(lease);
				Assert.assertEquals(5.0 * ROWS * COLS, sum(lease.value()), 0.0);
			}
			awaitOwnedCache(OOCCacheManager.getGlobalCache(), 0);
		}
		finally {
			stub.table.close();
			stub.onComplete();
			producer.destroy();
		}
	}

	@Test
	public void testUnmigratedPrimitiveKeepsCachedAllowance() {
		StubPrimitive stub = new StubPrimitive(false, true);
		stub.start();
		Assert.assertNull(stub.table);
		Assert.assertNotNull("Unmigrated primitives keep the CachedAllowance path.", stub.cache);
		stub.onComplete();
	}

	@Test
	public void testStateTableTakesPrecedenceOverCachedAllowance() {
		StubPrimitive stub = new StubPrimitive(true, true);
		stub.start();
		Assert.assertNotNull(stub.table);
		Assert.assertNull("A primitive selecting the table path must not get a CachedAllowance too.",
			stub.cache);
		stub.table.close();
		stub.onComplete();
	}

	@Test
	public void testDistinctTablesGetDistinctStreamIds() throws Exception {
		StubPrimitive first = new StubPrimitive(true, false);
		StubPrimitive second = new StubPrimitive(true, false);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(GlobalMemoryBroker.get());
		producer.setTargetMemory(1L << 26);
		long bytes = tileBytes();
		try {
			first.start();
			second.start();
			//distinct stream ids: installs at the same slot index must not collide in the global cache
			producer.reserveBlocking(bytes);
			first.table.install(0, new ManagedPayload<>(tile(0, 1.0), bytes, producer));
			producer.reserveBlocking(bytes);
			second.table.install(0, new ManagedPayload<>(tile(0, 2.0), bytes, producer));
			try(OperatorStateTable.StateLease<IndexedMatrixValue> lease =
				first.table.take(0).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
				Assert.assertEquals(1.0 * ROWS * COLS, sum(lease.value()), 0.0);
			}
			try(OperatorStateTable.StateLease<IndexedMatrixValue> lease =
				second.table.take(0).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
				Assert.assertEquals(2.0 * ROWS * COLS, sum(lease.value()), 0.0);
			}
		}
		finally {
			first.table.close();
			second.table.close();
			first.onComplete();
			second.onComplete();
			producer.destroy();
		}
	}

	@Test
	public void testStoreBindingSealsAfterDeclaredRegistrationsAndClosesOnLastRelease() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 32);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance sinkAllowance = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance readerA = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance readerB = new SyncMemoryAllowance(broker);
		producer.setTargetMemory(1L << 30);
		sinkAllowance.setTargetMemory(1L << 30);
		readerA.setTargetMemory(1L << 30);
		readerB.setTargetMemory(1L << 30);
		OOCCache cache = new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30);
		int tiles = 4;
		long bytes = tileBytes();
		try {
			OOCStoreBinding binding = new OOCStoreBinding(cache, 51,
				ix -> (int)ix.getRowIndex() - 1, sinkAllowance, 2, 2);
			SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
			binding.attach(source);
			for(int i = 0; i < tiles; i++) {
				producer.reserveBlocking(bytes);
				source.enqueue(new InMemoryQueueCallback(tile(i, i + 1.0), null, producer, bytes));
			}
			source.closeInput();
			binding.completion().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertEquals(tiles, binding.store().size());

			MaterializedStore.Reader<IndexedMatrixValue> first =
				binding.openReader(new SequentialAccessPattern(tiles), readerA, 2);
			MaterializedStore.Reader<IndexedMatrixValue> second =
				binding.openReader(new SequentialAccessPattern(tiles), readerB, 2);
			try {
				binding.openReader(new SequentialAccessPattern(tiles), readerA, 2);
				Assert.fail("The binding must seal after the declared registrations.");
			}
			catch(IllegalStateException expected) {
				//expected: either the over-registration guard or the sealed store
			}

			Assert.assertEquals(expectedTiles(tiles), consume(new StoreBackedStream(first)));
			Assert.assertEquals(expectedTiles(tiles), consume(new StoreBackedStream(second)));
			binding.release();
			binding.release();
			awaitOwnedCache(cache, 0);
		}
		finally {
			cache.shutdown();
			producer.destroy();
			sinkAllowance.destroy();
			readerA.destroy();
			readerB.destroy();
		}
	}

	private static Map<Integer, Double> consume(StoreBackedStream stream) {
		Map<Integer, Double> tiles = new HashMap<>();
		while(true) {
			OOCStream.QueueCallback<IndexedMatrixValue> cb = stream.dequeueCB();
			if(cb.isEos())
				break;
			tiles.put((int)cb.get().getIndexes().getRowIndex() - 1, sum(cb.get()));
		}
		return tiles;
	}

	private static Map<Integer, Double> expectedTiles(int tiles) {
		Map<Integer, Double> expected = new HashMap<>();
		for(int i = 0; i < tiles; i++)
			expected.put(i, (i + 1.0) * ROWS * COLS);
		return expected;
	}

	private static IndexedMatrixValue tile(int idx, double value) {
		return new IndexedMatrixValue(new MatrixIndexes(idx + 1L, 1L), new MatrixBlock(ROWS, COLS, value));
	}

	private static long tileBytes() {
		return new MatrixBlock(ROWS, COLS, 1.0).getExactSerializedSize();
	}

	private static double sum(IndexedMatrixValue value) {
		return ((MatrixBlock)value.getValue()).sum();
	}

	private static void awaitOwnedCache(OOCCache cache, long expected) throws Exception {
		waitFor(() -> cache.getOwnedCacheSize() == expected);
		Assert.assertEquals(expected, cache.getOwnedCacheSize());
	}

	private static void waitFor(BooleanSupplier condition) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_TIMEOUT_SEC);
		while(!condition.getAsBoolean() && System.nanoTime() < deadline)
			Thread.sleep(1);
	}

	/**
	 * Minimal leaf primitive exercising the binding seam: declares which capability it wants and
	 * records what the planner supplied.
	 */
	private static final class StubPrimitive extends OOCPrimitive {
		private final boolean wantsTable;
		private final boolean wantsCache;
		private OperatorStateTable<IndexedMatrixValue> table;
		private CachedAllowance cache;
		private boolean executed;

		private StubPrimitive(boolean wantsTable, boolean wantsCache) {
			super(List.of());
			this.wantsTable = wantsTable;
			this.wantsCache = wantsCache;
		}

		@Override
		public boolean requiresStateTable() {
			return wantsTable;
		}

		@Override
		public void bindStateTable(OperatorStateTable<IndexedMatrixValue> table) {
			this.table = table;
		}

		@Override
		public boolean requiresCache() {
			return wantsCache;
		}

		@Override
		public void bindCache(CachedAllowance cache) {
			this.cache = cache;
		}

		@Override
		public void startExecution() {
			executed = true;
		}

		@Override
		public List<OOCStreamable<?>> getInputStreams() {
			return List.of();
		}

		@Override
		public List<OOCStreamable<?>> getOutputStreams() {
			return List.of();
		}

		@Override
		public void inferPatterns() {
			_pattern = OOCAccessPattern.ANY;
		}

		@Override
		public void requestPattern(OOCAccessPattern accessPattern) {
			_pattern = accessPattern;
		}
	}
}
