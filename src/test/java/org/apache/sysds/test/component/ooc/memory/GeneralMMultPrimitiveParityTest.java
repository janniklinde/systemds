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

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedInputRequest;
import org.apache.sysds.runtime.ooc.primitives.GeneralMMultOOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GeneralMMultPrimitiveParityTest {
	private static final int M = 2000;
	private static final int K = 20000;
	private static final int N = 50000;
	private static final int BLEN = 250;
	private static final long WAIT_TIMEOUT_SEC = 600;

	@After
	public void tearDown() {
		OOCCacheManager.reset();
	}

	@Test
	public void testMaterializedBGeneralMultiply() throws Exception {
		OOCStream<IndexedMatrixValue> a = matrixStream(M, K);
		OOCInstructionUtils.dataGen(a,
			ix -> constantTile(ix, M, K, 2.0), new StreamContext(0, "general_mm_a").addOutStream(a));

		OOCStream<IndexedMatrixValue> b = matrixStream(K, N);
		OOCInstructionUtils.dataGen(b,
			ix -> constantTile(ix, K, N, 3.0), new StreamContext(0, "general_mm_b").addOutStream(b));

		OOCStream<IndexedMatrixValue> out = matrixStream(M, N);
		AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator mm = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate);
		OOCInstructionUtils.matrixMultiply(a, b, out, mm, new BinaryOperator(Plus.getPlusFnObject()),
			new StreamContext(0, "general_mm").addOutStream(out));
		Assert.assertTrue(out.getPrimitive() instanceof GeneralMMultOOCPrimitive);
		OOCMaterializedInputRequest request = out.getPrimitive().requiresMaterializedInput();
		Assert.assertEquals(1, request.inputIndex());
		int bRowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(b.getDataCharacteristics()));
		int bColBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(b.getDataCharacteristics()));
		MatrixIndexes probe = new MatrixIndexes(Math.min(2, bRowBlocks), Math.min(3, bColBlocks));
		int expectedLinearIndex = Math.toIntExact(
			(probe.getRowIndex() - 1) * bColBlocks + probe.getColumnIndex() - 1);
		int linearIndex = request.preferredLayout().linearize(probe);
		Assert.assertEquals(expectedLinearIndex, linearIndex);
		Assert.assertEquals(probe, request.preferredLayout().delinearize(linearIndex));
		Assert.assertTrue("Packed pins must be budgeted by physical rather than logical size.",
			OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(1) > 1);

		Map<MatrixIndexes, Double> sums = new ConcurrentHashMap<>();
		CompletableFuture<Void> done = new CompletableFuture<>();
		out.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					done.complete(null);
					return;
				}
				IndexedMatrixValue value = cb.get();
				sums.put(new MatrixIndexes(value.getIndexes()), ((MatrixBlock)value.getValue()).sum());
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

		int outputRowBlocks = Math.toIntExact(OOCUtils.getNumRowBlocks(out.getDataCharacteristics()));
		int outputColBlocks = Math.toIntExact(OOCUtils.getNumColBlocks(out.getDataCharacteristics()));
		Assert.assertEquals(Math.multiplyExact(outputRowBlocks, outputColBlocks), sums.size());
		double expectedCellValue = 2.0 * 3.0 * K;
		for(int row = 1; row <= outputRowBlocks; row++) {
			for(int col = 1; col <= outputColBlocks; col++) {
				MatrixIndexes outputIndex = new MatrixIndexes(row, col);
				Double sum = sums.get(outputIndex);
				Assert.assertNotNull("Missing output tile (" + row + "," + col + ")", sum);
				double expectedTileSum = expectedCellValue *
					OOCUtils.getNumRowsOfTile(outputIndex, M, BLEN) *
					OOCUtils.getNumColsOfTile(outputIndex, N, BLEN);
				Assert.assertEquals(expectedTileSum, sum, 1e-9);
			}
		}
	}

	private static MatrixBlock constantTile(MatrixIndexes indexes, long rows, long cols, double value) {
		return new MatrixBlock(OOCUtils.getNumRowsOfTile(indexes, rows, BLEN),
			OOCUtils.getNumColsOfTile(indexes, cols, BLEN), value);
	}

	private static OOCStream<IndexedMatrixValue> matrixStream(long rows, long cols) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixCharacteristics dc = new MatrixCharacteristics(rows, cols, BLEN, -1);
		stream.setData(new MatrixObject(ValueType.FP64, null, new MetaDataFormat(dc, FileFormat.BINARY)));
		return stream;
	}
}
