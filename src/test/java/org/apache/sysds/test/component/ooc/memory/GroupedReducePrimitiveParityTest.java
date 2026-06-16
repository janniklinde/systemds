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
 * Correctness of the migrated GroupedReduce (OperatorStateTable accumulators over the global cache)
 * through the real planner pipeline.
 */
public class GroupedReducePrimitiveParityTest {
	private static final int ROWS = 16000;
	private static final int COLS = 2000;
	private static final int BLEN = 250;
	private static final int GROUPS = ROWS / BLEN;
	private static final long WAIT_TIMEOUT_SEC = 60;

	@After
	public void tearDown() {
		OOCCacheManager.reset();
	}

	@Test
	public void testTablePathProducesExpectedGroups() throws Exception {
		Map<MatrixIndexes, Double> table = run();

		Assert.assertEquals(GROUPS, table.size());
		for(int group = 0; group < GROUPS; group++) {
			Double sum = table.get(new MatrixIndexes(group + 1L, 1L));
			Assert.assertNotNull("Missing output tile for group " + group, sum);
			Assert.assertEquals(5.0 * BLEN * COLS, sum, 1e-9);
		}
		//all accumulator slots were taken at finalize and the table closed: nothing stays cached
		awaitOwnedCache(OOCCacheManager.getGlobalCache(), 0);
	}

	private Map<MatrixIndexes, Double> run() throws Exception {
		OOCStream<IndexedMatrixValue> in = createMatrixStream(ROWS, COLS);
		StreamContext genSc = new StreamContext(0, "op_datagen").addOutStream(in);
		OOCInstructionUtils.dataGen(in, ix -> new MatrixBlock(OOCUtils.getNumRowsOfTile(ix, ROWS, BLEN),
			OOCUtils.getNumColsOfTile(ix, COLS, BLEN), 5.0), genSc);

		OOCStream<IndexedMatrixValue> out = createMatrixStream(ROWS, BLEN);
		StreamContext reduceSc = new StreamContext(0, "op_row_reduce").addOutStream(out);
		OOCInstructionUtils.rowGroupedReduce(in, out, 2, GroupedReducePrimitiveParityTest::padPartialBlock,
			(left, right) -> left.binaryOperations(new BinaryOperator(Plus.getPlusFnObject()), right),
			mb -> mb, reduceSc);

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

	private static MatrixBlock padPartialBlock(MatrixBlock block) {
		if(block.getNumColumns() == BLEN)
			return block;
		MatrixBlock padded = new MatrixBlock(block.getNumRows(), BLEN, false);
		padded.copy(0, block.getNumRows() - 1, 0, block.getNumColumns() - 1, block, false);
		return padded;
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
