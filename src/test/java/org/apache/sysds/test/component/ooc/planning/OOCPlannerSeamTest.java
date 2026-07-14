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
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedInputRequest;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedView;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MultiplicityLiveness;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.ooc.store.OrderedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.StateLease;
import org.apache.sysds.runtime.ooc.store.SequentialAccessPattern;
import org.apache.sysds.runtime.ooc.store.StoreBackedStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * The planner seam of the new architecture: primitives keep private state tables at execution time,
 * unmigrated primitives keep CachedAllowance,
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
			try(StateLease<IndexedMatrixValue> lease =
				stub.table.take(3, stub.allowance).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
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
	public void testStreamStartSkipsAlreadyExecutingPrimitive() {
		StartGuardPrimitive child = new StartGuardPrimitive();
		StartGuardPrimitive parent = new StartGuardPrimitive(child);
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		stream.assignPrimitive(child);

		child.tryStartExecution();
		stream.start();

		Assert.assertTrue(child.executed);
		Assert.assertEquals("Starting a stream whose primitive is already executing must not re-enter planning.",
			0, parent.inferCount);
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
			try(StateLease<IndexedMatrixValue> lease =
				first.table.take(0, first.allowance).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
				Assert.assertEquals(1.0 * ROWS * COLS, sum(lease.value()), 0.0);
			}
			try(StateLease<IndexedMatrixValue> lease =
				second.table.take(0, second.allowance).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
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
			OOCMaterializedView binding = new OOCMaterializedView(null, cache, 51,
				ix -> (int)ix.getRowIndex() - 1, sinkAllowance, 2, 2);
			for(int i = 0; i < tiles; i++) {
				producer.reserveBlocking(bytes);
				binding.store().publishPinned(i, tile(i, i + 1.0), bytes, producer);
			}
			binding.store().complete();
			Assert.assertEquals(tiles, binding.store().size());

			OrderedMaterializedStoreReader<IndexedMatrixValue> first =
				binding.openReader(new SequentialAccessPattern(tiles), readerA, 2);
			OrderedMaterializedStoreReader<IndexedMatrixValue> second =
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
			binding.close();
			binding.close();
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

	@Test
	public void testSharedBoundaryConsumersShareOneBindingAndSealTogether() throws Exception {
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		SourceStub producer = new SourceStub();
		source.assignPrimitive(producer);
		ConsumerStub first = new ConsumerStub(producer, source);
		ConsumerStub second = new ConsumerStub(producer, source);
		SyncMemoryAllowance payloads = new SyncMemoryAllowance(GlobalMemoryBroker.get());
		payloads.setTargetMemory(1L << 26);
		SyncMemoryAllowance reader = new SyncMemoryAllowance(GlobalMemoryBroker.get());
		reader.setTargetMemory(1L << 26);
		long bytes = tileBytes();
		try {
			first.start();
			second.start();
			OOCMaterializedView firstView = first.getInputStream(0).materializedView();
			OOCMaterializedView secondView = second.getInputStream(0).materializedView();
			Assert.assertNotNull(firstView);
			Assert.assertSame("Consumers of one boundary must share one materialized input.",
				firstView, secondView);

			//the planner materializes the boundary once through the shared binding
			for(int i = 0; i < 2; i++) {
				payloads.reserveBlocking(bytes);
				source.enqueue(new InMemoryQueueCallback(tile(i, i + 1.0), null, payloads, bytes));
			}
			source.closeInput();
			firstView.completion().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

			//pre-counted reader set: the store seals only after BOTH consumers registered
			IndexedMaterializedStoreReader<IndexedMatrixValue> readerA =
				firstView.openIndexedReader(new MultiplicityLiveness(2, 1));
			Assert.assertFalse("Sealing must wait for the full declared reader set.",
				firstView.readersSealed().isDone());
			IndexedMaterializedStoreReader<IndexedMatrixValue> readerB =
				secondView.openIndexedReader(new MultiplicityLiveness(2, 1));
			firstView.readersSealed().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

			try(MaterializedStore.Lease<IndexedMatrixValue> lease =
				readerA.request(0, reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
				Assert.assertEquals(1.0 * ROWS * COLS, sum(lease.value()), 0.0);
			}
			try(MaterializedStore.Lease<IndexedMatrixValue> lease =
				readerB.request(1, reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
				Assert.assertEquals(2.0 * ROWS * COLS, sum(lease.value()), 0.0);
			}

			//a consumer constructed after sealing cannot join the boundary anymore
			ConsumerStub late = new ConsumerStub(producer, source);
			try {
				late.start();
				Assert.fail("A late consumer must not join a sealed materialized input.");
			}
			catch(RuntimeException expected) {
				//expected: the registry rejects joining after the reader set sealed
			}

			readerA.close();
			readerB.close();
			firstView.close();
			secondView.close();
			awaitOwnedCache(OOCCacheManager.getGlobalCache(), 0);
		}
		finally {
			payloads.destroy();
			reader.destroy();
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

	private static final class StartGuardPrimitive extends OOCPrimitive {
		private boolean executed;
		private int inferCount;

		private StartGuardPrimitive(OOCPrimitive... children) {
			super(List.of(children));
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
			inferCount++;
			_pattern = OOCAccessPattern.ANY;
		}

		@Override
		public void requestPattern(OOCAccessPattern accessPattern) {
			_pattern = accessPattern;
		}
	}

	/**
	 * Leaf producer stub so a shared source streamable has a primitive whose parents are the
	 * boundary's consumers (the registry discovers the consumer set through them).
	 */
	private static final class SourceStub extends OOCPrimitive {
		private SourceStub() {
			super(List.of());
		}

		@Override
		public void startExecution() {
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

	/**
	 * Boundary consumer stub: declares a materialized input over the shared source.
	 */
	private static final class ConsumerStub extends OOCPrimitive {
		private ConsumerStub(OOCPrimitive producer, OOCStreamable<IndexedMatrixValue> source) {
			super(List.of(producer), List.of(source), List.of());
		}

		@Override
		public OOCMaterializedInputRequest requiresMaterializedInput() {
			return new OOCMaterializedInputRequest(0, ix -> (int) ix.getRowIndex() - 1, 1, 1);
		}

		@Override
		public void startExecution() {
		}

		public void inferPatterns() {
			_pattern = OOCAccessPattern.ANY;
		}

		@Override
		public void requestPattern(OOCAccessPattern accessPattern) {
			_pattern = accessPattern;
		}
	}

	/**
	 * Minimal leaf primitive exercising the binding seam: declares which capability it wants and
	 * records what the planner supplied.
	 */
	private static final class StubPrimitive extends OOCPrimitive {
		private final boolean wantsTable;
		private final boolean wantsCache;
		private StateTable<IndexedMatrixValue> table;
		private SyncMemoryAllowance allowance;
		private CachedAllowance cache;
		private boolean executed;

		private StubPrimitive(boolean wantsTable, boolean wantsCache) {
			super(List.of());
			this.wantsTable = wantsTable;
			this.wantsCache = wantsCache;
		}

		@Override
		public boolean requiresCache() {
			return wantsCache && !wantsTable;
		}

		@Override
		public void bindCache(CachedAllowance cache) {
			this.cache = cache;
		}

		@Override
		public void startExecution() {
			if(wantsTable) {
				allowance = new SyncMemoryAllowance(GlobalMemoryBroker.get());
				allowance.setTargetMemory(1L << 26);
				table = new StateTable<>(OOCCacheManager.getGlobalCache(),
					CachingStream._streamSeq.getNextID());
			}
			executed = true;
		}

		@Override
		public void onComplete() {
			if(allowance != null)
				allowance.destroy();
			super.onComplete();
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
