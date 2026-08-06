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

package org.apache.sysds.test.functions.ooc;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandlerImpl;
import org.apache.sysds.runtime.ooc.stream.SourceOOCStream;
import org.apache.sysds.runtime.controlprogram.parfor.LocalTaskQueue;
import org.apache.sysds.runtime.io.MatrixWriter;
import org.apache.sysds.runtime.io.MatrixWriterFactory;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SourceReadOOCIOHandlerTest extends AutomatedTestBase {
	private static final String TEST_NAME = "SourceReadOOCIOHandler";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + SourceReadOOCIOHandlerTest.class.getSimpleName() + "/";

	private OOCIOHandlerImpl handler;

	@Override
	@Before
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
		handler = new OOCIOHandlerImpl();
	}

	@After
	public void tearDown() {
		if (handler != null)
			handler.shutdown();
	}

	@Test
	public void testSourceReadCompletes() throws Exception {
		getAndLoadTestConfiguration(TEST_NAME);
		final int rows = 4;
		final int cols = 4;
		final int blen = 2;

		MatrixBlock src = MatrixBlock.randOperations(rows, cols, 1.0, -1, 1, "uniform", 7);
		String fname = input("binary_full");
		writeBinaryMatrix(src, fname, blen);

		SubscribableTaskQueue<IndexedMatrixValue> target = new SubscribableTaskQueue<>();
		OOCIOHandler.SourceReadRequest req = new OOCIOHandler.SourceReadRequest(fname, Types.FileFormat.BINARY,
			rows, cols, blen, src.getNonZeros(), Long.MAX_VALUE, true, target);

		OOCIOHandler.SourceReadResult res = handler.scheduleSourceRead(req).get();
		// Drain after EOF
		MatrixBlock reconstructed = drainToMatrix(target, rows, cols, blen);

		TestUtils.compareMatrices(src, reconstructed, 1e-12);
		Assert.assertTrue(res.eof);
		Assert.assertNull(res.continuation);
		Assert.assertNotNull(res.blocks);
		Assert.assertEquals((rows / blen) * (cols / blen), res.blocks.size());
		Assert.assertTrue(res.blocks.stream().allMatch(b -> b.indexes != null));
	}

	@Test
	public void testSourceReadStopsOnBudgetAndContinues() throws Exception {
		getAndLoadTestConfiguration(TEST_NAME);
		final int rows = 4;
		final int cols = 4;
		final int blen = 2;

		MatrixBlock src = MatrixBlock.randOperations(rows, cols, 1.0, -1, 1, "uniform", 13);
		String fname = input("binary_budget");
		writeBinaryMatrix(src, fname, blen);

		long singleBlockSize = new MatrixBlock(blen, blen, false).getExactSerializedSize();
		long budget = singleBlockSize + 1; // ensure we stop before the second block

		SubscribableTaskQueue<IndexedMatrixValue> target = new SubscribableTaskQueue<>();
		OOCIOHandler.SourceReadRequest req = new OOCIOHandler.SourceReadRequest(fname, Types.FileFormat.BINARY,
			rows, cols, blen, src.getNonZeros(), budget, true, target);

		OOCIOHandler.SourceReadResult first = handler.scheduleSourceRead(req).get();
		Assert.assertFalse(first.eof);
		Assert.assertNotNull(first.continuation);
		Assert.assertNotNull(first.blocks);

		OOCIOHandler.SourceReadResult second = handler.continueSourceRead(first.continuation, Long.MAX_VALUE).get();
		Assert.assertTrue(second.eof);
		Assert.assertNull(second.continuation);
		Assert.assertNotNull(second.blocks);
		Assert.assertEquals((rows / blen) * (cols / blen), first.blocks.size() + second.blocks.size());

		MatrixBlock reconstructed = drainToMatrix(target, rows, cols, blen);
		TestUtils.compareMatrices(src, reconstructed, 1e-12);
	}

	@Test
	public void testLargeTileSeparatesSmallSourceTiles() throws Exception {
		getAndLoadTestConfiguration(TEST_NAME);
		int rows = 10;
		int cols = 30;
		int blocksize = 10;
		MatrixBlock source = new MatrixBlock(rows, cols, true);
		source.set(0, 0, 1);
		for(int row = 0; row < rows; row++)
			for(int col = 10; col < 20; col++)
				source.set(row, col, 2);
		source.set(0, 20, 3);
		source.recomputeNonZeros();
		String file = input("binary_pack_boundary");
		writeBinaryMatrix(source, file, blocksize);

		MatrixBlock small = source.slice(0, rows - 1, 0, blocksize - 1);
		MatrixBlock large = source.slice(0, rows - 1, blocksize, 2 * blocksize - 1);
		long threshold = (small.getExactSerializedSize() + large.getExactSerializedSize()) / 2;
		SourceOOCStream target = new SourceOOCStream(false);
		AtomicInteger groups = new AtomicInteger();
		AtomicInteger singles = new AtomicInteger();
		target.setSubscriber(callback -> {
			try(callback) {
				if(callback.isEos())
					return;
				if(callback instanceof SourceOOCStream.SourceGroupCallback)
					groups.incrementAndGet();
				else
					singles.incrementAndGet();
			}
		});
		OOCIOHandler.SourceReadRequest request = new OOCIOHandler.SourceReadRequest(file, Types.FileFormat.BINARY, rows,
			cols, blocksize, source.getNonZeros(), Long.MAX_VALUE, true, target, threshold, 2 * threshold);
		OOCIOHandler.SourceReadResult result = handler.scheduleSourceRead(request).get();

		Assert.assertTrue(result.eof);
		Assert.assertEquals(3, result.blocks.size());
		Assert.assertEquals(2, result.blocks.stream().filter(block -> block.serializedSize < threshold).count());
		Assert.assertEquals(1, result.blocks.stream().filter(block -> block.serializedSize >= threshold).count());
		Assert.assertEquals(0, groups.get());
		Assert.assertEquals(3, singles.get());
	}

	private void writeBinaryMatrix(MatrixBlock mb, String fname, int blen) throws Exception {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
		writer.writeMatrixToHDFS(mb, fname, mb.getNumRows(), mb.getNumColumns(), blen, mb.getNonZeros());
	}

	private MatrixBlock drainToMatrix(SubscribableTaskQueue<IndexedMatrixValue> target, int rows, int cols, int blen) {
		List<IndexedMatrixValue> blocks = new ArrayList<>();
		IndexedMatrixValue tmp;
		while((tmp = target.dequeue()) != LocalTaskQueue.NO_MORE_TASKS) {
			blocks.add(tmp);
		}

		MatrixBlock out = new MatrixBlock(rows, cols, false);
		for (IndexedMatrixValue imv : blocks) {
			int rowOffset = (int)((imv.getIndexes().getRowIndex() - 1) * blen);
			int colOffset = (int)((imv.getIndexes().getColumnIndex() - 1) * blen);
			((MatrixBlock)imv.getValue()).putInto(out, rowOffset, colOffset, true);
		}
		out.recomputeNonZeros();
		return out;
	}
}
