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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.store.OOCStreamMaterializer;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.JoinTable;
import org.junit.Assert;
import org.junit.Test;

/**
 * The rendezvous driver helper: install-or-take routed by callback kind — owned in-memory payloads,
 * the measured fallback for unmanaged callbacks, and counted logical references for pinned-lease
 * callbacks from materialized boundaries (with the pin held inside the helper until the rendezvous
 * resolves, TODO open issue 2).
 */
public class JoinTableTest {
	private static final int ROWS = 32;
	private static final int COLS = 1;
	private static final long WAIT_TIMEOUT_SEC = 10;

	@Test
	public void testOwnedInstallThenTakePairsBothOrders() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 32);
		SyncMemoryAllowance producer = allowance(broker);
		SyncMemoryAllowance region = allowance(broker);
		OOCCache cache = new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30);
		StateTable<IndexedMatrixValue> table = new StateTable<>(cache, 7);
		long bytes = tileBytes();
		try {
			//slot 0: left first; slot 1: right first — the second arrival always takes the partner
			Assert.assertNull(await(JoinTable.putIfAbsent(table, 0, managed(0, 1.0, producer, bytes),
				region)));
			Assert.assertNull(await(JoinTable.putIfAbsent(table, 1, managed(1, 2.0, producer, bytes),
				region)));

			JoinTable.Match match0 = await(JoinTable.putIfAbsent(table, 0,
				managed(0, 10.0, producer, bytes), region));
			Assert.assertNotNull(match0);
			Assert.assertEquals(10.0 * ROWS * COLS, sum(match0.own()), 0.0);
			Assert.assertEquals(1.0 * ROWS * COLS, sum(match0.partner()), 0.0);
			match0.own().close();
			match0.partner().close();

			//unmanaged fallback: measured and reserved on the supplied allowance
			JoinTable.Match match1 = await(JoinTable.putIfAbsent(table, 1,
				new OOCStream.SimpleQueueCallback<>(tile(1, 20.0), null), region));
			Assert.assertNotNull(match1);
			Assert.assertEquals(20.0 * ROWS * COLS, sum(match1.own()), 0.0);
			Assert.assertEquals(2.0 * ROWS * COLS, sum(match1.partner()), 0.0);
			match1.own().close();
			match1.partner().close();

			table.close();
			awaitOwnedCache(cache, 0);
			awaitUsedMemory(producer, 0);
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
	public void testReferenceRendezvousFromMaterializedBoundary() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 32);
		SyncMemoryAllowance producer = allowance(broker);
		SyncMemoryAllowance sinkAllowance = allowance(broker);
		SyncMemoryAllowance region = allowance(broker);
		OOCCache cache = new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30);
		MaterializedStore<IndexedMatrixValue> store = new MaterializedStore<>(cache, 11);
		StateTable<IndexedMatrixValue> table = new StateTable<>(cache, 12);
		long bytes = tileBytes();
		int tiles = 2;
		try {
			//live fan-out consumer routes every pinned alias into the rendezvous (boundary side)
			List<OOCFuture<JoinTable.Match>> installs = new ArrayList<>();
			SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
			OOCStreamMaterializer sink = new OOCStreamMaterializer(store,
				ix -> (int) ix.getRowIndex() - 1, sinkAllowance, List.of(cb -> {
					if(cb.isEos() || cb.isFailure())
						return;
					int slot = (int) cb.get().getIndexes().getRowIndex() - 1;
					synchronized(installs) {
						installs.add(JoinTable.putIfAbsent(table, slot,
							cb.keepOpen(), region));
					}
				}));
			sink.attach(source);
			for(int i = 0; i < tiles; i++) {
				producer.reserveBlocking(bytes);
				source.enqueue(new InMemoryQueueCallback(tile(i, i + 1.0), null, producer, bytes));
			}
			source.closeInput();
			sink.completion().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

			//all boundary tiles installed as references (no partner yet); the helper released the
			//pin once each rendezvous resolved, so ownership moved producer -> cache
			synchronized(installs) {
				Assert.assertEquals(tiles, installs.size());
				for(OOCFuture<JoinTable.Match> install : installs)
					Assert.assertNull(await(install));
			}
			awaitUsedMemory(producer, 0);

			//the partner side arrives with owned payloads and takes the referenced values
			for(int i = 0; i < tiles; i++) {
				JoinTable.Match match = await(JoinTable.putIfAbsent(table, i,
					managed(i, 100.0 + i, producer, bytes), region));
				Assert.assertNotNull(match);
				Assert.assertEquals((100.0 + i) * ROWS * COLS, sum(match.own()), 0.0);
				Assert.assertEquals((i + 1.0) * ROWS * COLS, sum(match.partner()), 0.0);
				match.own().close();
				match.partner().close();
			}

			table.close();
			store.complete();
			store.sealReaders();
			store.close();
			awaitOwnedCache(cache, 0);
			awaitUsedMemory(region, 0);
		}
		finally {
			table.close();
			store.close();
			cache.shutdown();
			producer.destroy();
			sinkAllowance.destroy();
			region.destroy();
		}
	}

	private static SyncMemoryAllowance allowance(GlobalMemoryBroker broker) {
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		allowance.setTargetMemory(1L << 28);
		return allowance;
	}

	private static InMemoryQueueCallback managed(int idx, double value, SyncMemoryAllowance producer, long bytes) {
		producer.reserveBlocking(bytes);
		return new InMemoryQueueCallback(tile(idx, value), null, producer, bytes);
	}

	private static <T> T await(OOCFuture<T> future) throws Exception {
		return future.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
	}

	private static IndexedMatrixValue tile(int idx, double value) {
		return new IndexedMatrixValue(new MatrixIndexes(idx + 1L, 1L), new MatrixBlock(ROWS, COLS, value));
	}

	private static long tileBytes() {
		return new MatrixBlock(ROWS, COLS, 1.0).getExactSerializedSize();
	}

	private static double sum(OOCStream.QueueCallback<IndexedMatrixValue> cb) {
		return ((MatrixBlock) cb.get().getValue()).sum();
	}

	private static void awaitOwnedCache(OOCCache cache, long expected) throws Exception {
		waitFor(() -> cache.getOwnedCacheSize() == expected);
		Assert.assertEquals(expected, cache.getOwnedCacheSize());
	}

	private static void awaitUsedMemory(SyncMemoryAllowance allowance, long expected) throws Exception {
		waitFor(() -> allowance.getUsedMemory() == expected);
		Assert.assertEquals(expected, allowance.getUsedMemory());
	}

	private static void waitFor(BooleanSupplier condition) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_TIMEOUT_SEC);
		while(!condition.getAsBoolean() && System.nanoTime() < deadline)
			Thread.sleep(1);
	}
}
