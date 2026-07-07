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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.store.OOCStreamMaterializer;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.SequentialAccessPattern;
import org.apache.sysds.runtime.ooc.store.StoreBackedStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Parity of the CachingStream replacement (MaterializationSink + StoreBackedStream) with the legacy
 * pipeline: identical tiles, zero residual allowance bytes, and equivalent forgetting.
 */
public class OOCStreamMaterializerParityTest {
	private static final int ROWS = 32;
	private static final int COLS = 1;
	private static final int TILES = 6;
	private static final long STREAM_ID = 41;
	private static final long WAIT_TIMEOUT_SEC = 10;

	@Before
	public void setUp() {
		OOCCacheManager.reset();
	}

	@After
	public void tearDown() {
		OOCCacheManager.reset();
	}

	@Test
	public void testParityWithCachingStream() throws Exception {
		Map<Integer, Double> legacy = runLegacyPipeline();
		Assert.assertEquals(TILES, legacy.size());

		Fixture f = new Fixture(new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30));
		try {
			publishAll(f, List.of());
			Assert.assertEquals(TILES, f.store.size());
			awaitUsedMemory(f.producer, 0);

			MaterializedStore.Reader<IndexedMatrixValue> reader =
				f.store.openReader(new SequentialAccessPattern(TILES), f.reader, 4);
			f.store.sealReaders();
			Map<Integer, Double> replayed = consume(new StoreBackedStream(reader));

			Assert.assertEquals("Replay through sink + compat reader must produce the legacy tiles.",
				legacy, replayed);
			awaitUsedMemory(f.reader, 0);
			//all tiles consumed by the only reader: equivalent forgetting drains the cache completely
			awaitOwnedCache(f.cache, 0);
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testFailureLeavesStoreIncomplete() throws Exception {
		Fixture f = new Fixture(new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30));
		long bytes = tileBytes();
		try {
			AtomicInteger failureEos = new AtomicInteger();
			OOCStreamMaterializer sink = new OOCStreamMaterializer(f.store,
				OOCStreamMaterializerParityTest::linearize, f.sink,
				List.of(cb -> {
					if(cb.isFailure())
						failureEos.incrementAndGet();
				}));
			sink.attach(f.source);

			f.producer.reserveBlocking(bytes);
			f.source.enqueue(new InMemoryQueueCallback(tile(0, 1.0), null, f.producer, bytes));
			f.source.enqueue(OOCStream.eos(new DMLRuntimeException("boom")));

			try {
				sink.completion().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
				Assert.fail("Completion must propagate the source failure");
			}
			catch(java.util.concurrent.ExecutionException expected) {
				Assert.assertTrue(expected.getCause() instanceof DMLRuntimeException);
			}
			Assert.assertEquals("Live consumers must receive the failure EOS.", 1, failureEos.get());
			try {
				f.store.openReader(new SequentialAccessPattern(1), f.reader, 1);
				Assert.fail("A failed sink must leave the store incomplete");
			}
			catch(IllegalStateException expected) {
				//expected
			}
		}
		finally {
			f.close();
		}
	}

	private Map<Integer, Double> runLegacyPipeline() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 30);
		SyncMemoryAllowance sourceAllowance = new SyncMemoryAllowance(broker);
		sourceAllowance.setTargetMemory(1L << 28);
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		CachingStream cached = new CachingStream(source);
		long bytes = tileBytes();

		Map<Integer, Double> tiles = new ConcurrentHashMap<>();
		CompletableFuture<Void> done = new CompletableFuture<>();
		cached.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					done.complete(null);
					return;
				}
				try(cb) {
					tiles.put(linearize(cb.get().getIndexes()), sum(cb.get()));
				}
			}
			catch(Throwable t) {
				done.completeExceptionally(t);
			}
		}, true);

		for(int i = 0; i < TILES; i++) {
			sourceAllowance.reserveBlocking(bytes);
			source.enqueue(new InMemoryQueueCallback(tile(i, i + 1.0), null, sourceAllowance, bytes));
		}
		source.closeInput();
		done.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

		awaitUsedMemory(sourceAllowance, 0);
		cached.scheduleDeletion();
		sourceAllowance.destroy();
		return new HashMap<>(tiles);
	}

	private void publishAll(Fixture f, List<java.util.function.Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> liveConsumers)
		throws Exception {
		OOCStreamMaterializer sink = new OOCStreamMaterializer(f.store,
			OOCStreamMaterializerParityTest::linearize, f.sink, liveConsumers);
		sink.attach(f.source);
		long bytes = tileBytes();
		for(int i = 0; i < TILES; i++) {
			f.producer.reserveBlocking(bytes);
			f.source.enqueue(new InMemoryQueueCallback(tile(i, i + 1.0), null, f.producer, bytes));
		}
		f.source.closeInput();
		sink.completion().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
	}

	private static Map<Integer, Double> consume(StoreBackedStream stream) {
		Map<Integer, Double> tiles = new HashMap<>();
		while(true) {
			OOCStream.QueueCallback<IndexedMatrixValue> cb = stream.dequeueCB();
			if(cb.isEos())
				break;
			Assert.assertFalse(cb.isFailure());
			tiles.put(linearize(cb.get().getIndexes()), sum(cb.get()));
		}
		return tiles;
	}

	private static Map<Integer, Double> expectedTiles() {
		Map<Integer, Double> tiles = new HashMap<>();
		for(int i = 0; i < TILES; i++)
			tiles.put(i, (i + 1.0) * ROWS * COLS);
		return tiles;
	}

	private static int linearize(MatrixIndexes indexes) {
		return (int)indexes.getRowIndex() - 1;
	}

	private static double sum(IndexedMatrixValue value) {
		return ((MatrixBlock)value.getValue()).sum();
	}

	private static IndexedMatrixValue tile(int idx, double value) {
		return new IndexedMatrixValue(new MatrixIndexes(idx + 1L, 1L), new MatrixBlock(ROWS, COLS, value));
	}

	private static long tileBytes() {
		return new MatrixBlock(ROWS, COLS, 1.0).getExactSerializedSize();
	}

	private static void awaitUsedMemory(SyncMemoryAllowance allowance, long expected) throws Exception {
		waitFor(() -> allowance.getUsedMemory() == expected);
		Assert.assertEquals(expected, allowance.getUsedMemory());
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

	private static final class Fixture {
		private final GlobalMemoryBroker broker;
		private final SyncMemoryAllowance producer;
		private final SyncMemoryAllowance sink;
		private final SyncMemoryAllowance reader;
		private final OOCCache cache;
		private final SubscribableTaskQueue<IndexedMatrixValue> source;
		private final MaterializedStore<IndexedMatrixValue> store;

		private Fixture(OOCCache cache) {
			broker = new GlobalMemoryBroker(1L << 32);
			producer = new SyncMemoryAllowance(broker);
			producer.setTargetMemory(1L << 30);
			sink = new SyncMemoryAllowance(broker);
			sink.setTargetMemory(1L << 30);
			reader = new SyncMemoryAllowance(broker);
			reader.setTargetMemory(1L << 30);
			this.cache = cache;
			source = new SubscribableTaskQueue<>();
			store = new MaterializedStore<>(cache, STREAM_ID);
		}

		private void close() {
			store.close();
			cache.shutdown();
			producer.destroy();
			sink.destroy();
			reader.destroy();
		}
	}
}
