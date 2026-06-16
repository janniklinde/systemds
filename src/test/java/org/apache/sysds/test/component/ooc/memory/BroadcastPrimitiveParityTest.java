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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Correctness of the migrated Broadcast (MaterializedStore + IndexedReader with multiplicity-based
 * forgetting over the global cache) through the real planner pipeline: a row vector is broadcast
 * and added to every row block of a streamed matrix.
 */
public class BroadcastPrimitiveParityTest {
	private static final int ROWS = 4000;
	private static final int COLS = 2000;
	private static final int BLEN = 250;
	private static final int ROW_BLOCKS = ROWS / BLEN;
	private static final int COL_BLOCKS = COLS / BLEN;
	private static final long WAIT_TIMEOUT_SEC = 60;

	@After
	public void tearDown() {
		OOCCacheManager.reset();
	}

	@Test
	public void testStorePathProducesExpectedBroadcast() throws Exception {
		Map<MatrixIndexes, Double> store = run();

		Assert.assertEquals(ROW_BLOCKS * COL_BLOCKS, store.size());
		for(int rb = 1; rb <= ROW_BLOCKS; rb++) {
			for(int cb = 1; cb <= COL_BLOCKS; cb++) {
				Double sum = store.get(new MatrixIndexes(rb, cb));
				Assert.assertNotNull("Missing output tile (" + rb + "," + cb + ")", sum);
				Assert.assertEquals(8.0 * BLEN * BLEN, sum, 1e-9);
			}
		}
		//every broadcast tile was consumed exactly maxCount times and the store released:
		//multiplicity-based forgetting leaves nothing cached
		awaitOwnedCache(OOCCacheManager.getGlobalCache(), 0);
	}

	private Map<MatrixIndexes, Double> run() throws Exception {
		OOCStream<IndexedMatrixValue> broadcast = createMatrixStream(1, COLS);
		StreamContext bGenSc = new StreamContext(0, "op_datagen_bcast").addOutStream(broadcast);
		OOCInstructionUtils.dataGen(broadcast, ix -> new MatrixBlock(1,
			OOCUtils.getNumColsOfTile(ix, COLS, BLEN), 5.0), bGenSc);

		OOCStream<IndexedMatrixValue> streamed = createMatrixStream(ROWS, COLS);
		StreamContext sGenSc = new StreamContext(0, "op_datagen_streamed").addOutStream(streamed);
		OOCInstructionUtils.dataGen(streamed, ix -> new MatrixBlock(OOCUtils.getNumRowsOfTile(ix, ROWS, BLEN),
			OOCUtils.getNumColsOfTile(ix, COLS, BLEN), 3.0), sGenSc);

		OOCStream<IndexedMatrixValue> out = createMatrixStream(ROWS, COLS);
		StreamContext bcSc = new StreamContext(0, "op_broadcast").addOutStream(out);
		BinaryOperator plus = new BinaryOperator(Plus.getPlusFnObject());
		OOCInstructionUtils.broadcast(broadcast, streamed, out,
			(bcast, str) -> ((MatrixBlock) str.getValue())
				.binaryOperations(plus, (MatrixBlock) bcast.getValue()),
			imv -> Math.toIntExact(imv.getIndexes().getColumnIndex() - 1),
			imv -> Math.toIntExact(imv.getIndexes().getColumnIndex() - 1),
			COL_BLOCKS, ROW_BLOCKS, bcSc);

		Map<MatrixIndexes, Double> results = new ConcurrentHashMap<>();
		CompletableFuture<Void> done = new CompletableFuture<>();
		out.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					done.complete(null);
					return;
				}
				IndexedMatrixValue imv = cb.get();
				results.put(new MatrixIndexes(imv.getIndexes()), ((MatrixBlock) imv.getValue()).sum());
			}
			catch(Throwable t) {
				done.completeExceptionally(t);
			}
			finally {
				cb.close();
			}
		});

		out.start();
		done.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		return new HashMap<>(results);
	}

	private static OOCStream<IndexedMatrixValue> createMatrixStream(int rows, int cols) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixCharacteristics dc = new MatrixCharacteristics(rows, cols, BLEN, -1);
		stream.setData(new MatrixObject(ValueType.FP64, null, new MetaDataFormat(dc, FileFormat.BINARY)));
		return stream;
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
}
