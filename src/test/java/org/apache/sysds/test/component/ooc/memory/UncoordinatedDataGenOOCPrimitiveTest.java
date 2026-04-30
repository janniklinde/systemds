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

import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.parfor.LocalTaskQueue;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.io.MatrixWriter;
import org.apache.sysds.runtime.io.MatrixWriterFactory;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.primitives.UncoordinatedDataGenOOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.SourceOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class UncoordinatedDataGenOOCPrimitiveTest extends AutomatedTestBase {
	private static final String TEST_NAME = "UncoordinatedDataGenOOCPrimitive";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + UncoordinatedDataGenOOCPrimitiveTest.class.getSimpleName() + "/";

	private OOCMatrixIOHandler handler;

	@Override
	@Before
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
		handler = new OOCMatrixIOHandler();
	}

	@After
	public void tearDown() {
		if(handler != null)
			handler.shutdown();
	}

	@Test
	public void testSourceReadThroughUncoordinatedPrimitive() throws Exception {
		getAndLoadTestConfiguration(TEST_NAME);
		final int rows = 6;
		final int cols = 5;
		final int blen = 2;

		MatrixBlock src = MatrixBlock.randOperations(rows, cols, 1.0, -1, 1, "uniform", 31);
		String fname = input("binary_src_uncoordinated");
		writeBinaryMatrix(src, fname, blen);

		OOCStream<IndexedMatrixValue> out = createMatrixStream(rows, cols, blen);
		AtomicReference<UncoordinatedDataGenOOCPrimitive> primitiveRef = new AtomicReference<>();
		AtomicInteger sourceBlocks = new AtomicInteger();
		AtomicInteger producerCalls = new AtomicInteger();

		UncoordinatedDataGenOOCPrimitive primitive = new UncoordinatedDataGenOOCPrimitive(out, allow -> {
			try {
				producerCalls.incrementAndGet();
				SourceOOCStream source = new SourceOOCStream();
				OOCIOHandler.SourceReadRequest req = new OOCIOHandler.SourceReadRequest(fname, Types.FileFormat.BINARY,
					rows, cols, blen, src.getNonZeros(), allow, true, source);
				OOCIOHandler.SourceReadResult res = handler.scheduleSourceRead(req).get();

				IndexedMatrixValue imv;
				while((imv = source.dequeue()) != LocalTaskQueue.NO_MORE_TASKS) {
					OOCIOHandler.SourceBlockDescriptor desc = source.getDescriptor(imv.getIndexes());
					Assert.assertNotNull("Missing source descriptor for " + imv.getIndexes(), desc);
					sourceBlocks.incrementAndGet();
					primitiveRef.get().emit(imv, desc);
				}

				Assert.assertTrue("Test expects the read to finish in one producer call.", res.eof);
				primitiveRef.get().shutdown();
			}
			catch(Throwable t) {
				out.propagateFailure(DMLRuntimeException.of(t));
				primitiveRef.get().shutdown();
			}
		}, 100, new StreamContext(0, TEST_NAME).addOutStream(out));
		primitiveRef.set(primitive);
		out.assignPrimitive(primitive);

		MatrixBlock reconstructed = new MatrixBlock(rows, cols, false);
		CompletableFuture<Void> done = new CompletableFuture<>();
		AtomicInteger outBlocks = new AtomicInteger();

		out.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					done.complete(null);
					return;
				}

				IndexedMatrixValue imv = cb.get();
				int rowOffset = (int)((imv.getIndexes().getRowIndex() - 1) * blen);
				int colOffset = (int)((imv.getIndexes().getColumnIndex() - 1) * blen);
				((MatrixBlock) imv.getValue()).putInto(reconstructed, rowOffset, colOffset, true);
				outBlocks.incrementAndGet();
			}
			catch(Throwable t) {
				done.completeExceptionally(t);
			}
			finally {
				cb.close();
			}
		});

		out.start();
		done.join();
		reconstructed.recomputeNonZeros();

		int expectedBlocks = (int)(((rows - 1) / blen + 1) * ((cols - 1) / blen + 1));
		Assert.assertEquals(1, producerCalls.get());
		Assert.assertEquals(expectedBlocks, sourceBlocks.get());
		Assert.assertEquals(expectedBlocks, outBlocks.get());
		TestUtils.compareMatrices(src, reconstructed, 1e-12);
	}

	private void writeBinaryMatrix(MatrixBlock mb, String fname, int blen) throws Exception {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
		writer.writeMatrixToHDFS(mb, fname, mb.getNumRows(), mb.getNumColumns(), blen, mb.getNonZeros());
	}

	private static OOCStream<IndexedMatrixValue> createMatrixStream(long rows, long cols, int blen) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixCharacteristics dc = new MatrixCharacteristics(rows, cols, blen, -1);
		stream.setData(new MatrixObject(ValueType.FP64, null, new MetaDataFormat(dc, FileFormat.BINARY)));
		return stream;
	}
}
