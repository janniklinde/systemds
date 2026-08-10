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

package org.apache.sysds.test.component.ooc;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.LocalVariableMap;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.cp.IndexingCPInstruction;
import org.apache.sysds.runtime.instructions.ooc.AppendOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.IndexingOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.utils.Statistics;
import org.junit.Assert;
import org.junit.Test;

public class RepartitionInstructionSpillTest {
	@Test
	public void testRightIndexingSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
			MatrixObject in = matrixObject(400, 400, 200);
			MatrixObject out = matrixObject(300, 300, 200);
			in.setStreamHandle(input);
			ec.setVariable("A", in);
			ec.setVariable("C", out);
			input.enqueue(tile(1, 1, 200, 200, 11));

			IndexingCPInstruction cp = IndexingCPInstruction
				.parseInstruction("CP°rightIndex°A·MATRIX·FP64°51·SCALAR·INT64·true°350·SCALAR·INT64·true°"
					+ "51·SCALAR·INT64·true°350·SCALAR·INT64·true°C·MATRIX·FP64");
			IndexingOOCInstruction.parseInstruction(cp).processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();

			input.enqueue(tile(1, 2, 200, 200, 12));
			input.enqueue(tile(2, 1, 200, 200, 21));
			input.enqueue(tile(2, 2, 200, 200, 22));
			input.closeInput();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					if(value.getIndexes().equals(new MatrixIndexes(1, 1))) {
						MatrixBlock block = (MatrixBlock) value.getValue();
						Assert.assertEquals(11, block.get(0, 0), 0);
						Assert.assertEquals(12, block.get(0, 199), 0);
						Assert.assertEquals(21, block.get(199, 0), 0);
						Assert.assertEquals(22, block.get(199, 199), 0);
					}
					blocks++;
				}
			Assert.assertEquals(4, blocks);
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testAppendSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> leftInput = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> rightInput = new SubscribableTaskQueue<>();
			MatrixObject left = matrixObject(200, 100, 200);
			MatrixObject right = matrixObject(200, 100, 200);
			MatrixObject out = matrixObject(200, 200, 200);
			left.setStreamHandle(leftInput);
			right.setStreamHandle(rightInput);
			ec.setVariable("A", left);
			ec.setVariable("B", right);
			ec.setVariable("C", out);
			leftInput.enqueue(tile(1, 1, 200, 100, 1));
			leftInput.closeInput();

			AppendOOCInstruction.parseInstruction("OOC°append°A·MATRIX·FP64°B·MATRIX·FP64°C·MATRIX·FP64°true")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();

			rightInput.enqueue(tile(1, 1, 200, 100, 2));
			rightInput.closeInput();
			try(OOCStream.QueueCallback<IndexedMatrixValue> callback = result.dequeueCB()) {
				MatrixBlock block = (MatrixBlock) callback.get().getValue();
				Assert.assertEquals(1, block.get(0, 0), 0);
				Assert.assertEquals(2, block.get(0, 199), 0);
			}
			Assert.assertNull(result.dequeueCB());
		}
		finally {
			reset(statistics);
		}
	}

	private static boolean prepareSpillCache() {
		OOCCacheManager.reset();
		boolean statistics = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		Statistics.resetOOCEvictionStats();
		OOCCacheManager.getGlobalCache().updateLimits(2_000_000, 100_000);
		return statistics;
	}

	private static void waitForSpill() throws InterruptedException {
		for(int attempt = 0; attempt < 100 && Statistics.getOOCEvictionWriteCount() == 0; attempt++)
			Thread.sleep(10);
		Assert.assertTrue("Expected instruction state to spill", Statistics.getOOCEvictionWriteCount() > 0);
	}

	private static IndexedMatrixValue tile(long row, long col, int rows, int cols, double value) {
		return new IndexedMatrixValue(new MatrixIndexes(row, col), new MatrixBlock(rows, cols, value));
	}

	private static MatrixObject matrixObject(long rows, long cols, int blocksize) {
		return new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(rows, cols, blocksize), FileFormat.BINARY));
	}

	private static void reset(boolean statistics) {
		OOCCacheManager.reset();
		DMLScript.OOC_STATISTICS = statistics;
	}
}
