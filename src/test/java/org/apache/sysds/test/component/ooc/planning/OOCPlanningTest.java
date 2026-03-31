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

import org.apache.sysds.common.Types;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.functionobjects.SwapIndex;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.instructions.ooc.OOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;
import org.apache.sysds.runtime.util.LocalFileUtils;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class OOCPlanningTest extends OOCInstruction {
	private static final int BLOCK_SIZE = 1000;
	private static final int MATRIX_ROWS = 10000;
	private static final int MATRIX_COLS = 10000;
	private static final int VECTOR_ROWS = 100000000;
	private static final int VECTOR_COLS = 1;

	private enum DataGenOrder {
		ROW_MAJOR,
		COL_MAJOR
	}

	private static final class UntrackedDataGen {
		private final OOCStream<MatrixIndexes> indexes;
		private final OOCStream<IndexedMatrixValue> output;

		private UntrackedDataGen(OOCStream<MatrixIndexes> indexes, OOCStream<IndexedMatrixValue> output) {
			this.indexes = indexes;
			this.output = output;
		}
	}

	public OOCPlanningTest() {
		super(null, null, "");
	}

	//@Test
	public void test() throws ExecutionException, InterruptedException {
		for(int i = 0; i < 30; i++)
			runPlanningScenario(true);
		long millis = System.currentTimeMillis();
		for(int i = 0; i < 10; i++)
			runPlanningScenario(true);
		millis = System.currentTimeMillis() - millis;
		System.out.println("With Tracking: " + millis + "ms");
	}

	//@Test
	public void testWithoutTrackingPrimitives() throws ExecutionException, InterruptedException {
		for(int i = 0; i < 30; i++)
			runPlanningScenario(false);
		long millis = System.currentTimeMillis();
		for(int i = 0; i < 10; i++)
			runPlanningScenario(false);
		millis = System.currentTimeMillis() - millis;
		System.out.println("Without Tracking: " + millis + "ms");
	}

	@Test
	public void testMapOnly() throws ExecutionException, InterruptedException {
		for(int i = 0; i < 60; i++)
			runMapOnlyScenario(true, MATRIX_ROWS, MATRIX_COLS, DataGenOrder.ROW_MAJOR);
		long millis = System.currentTimeMillis();
		for(int i = 0; i < 20; i++)
			runMapOnlyScenario(true, MATRIX_ROWS, MATRIX_COLS, DataGenOrder.ROW_MAJOR);
		millis = System.currentTimeMillis() - millis;
		System.out.println("Map Only With Tracking: " + millis + "ms");
	}

	@Test
	public void testMapOnlyWithoutTrackingPrimitives() throws ExecutionException, InterruptedException {
		for(int i = 0; i < 60; i++)
			runMapOnlyScenario(false, MATRIX_ROWS, MATRIX_COLS, DataGenOrder.ROW_MAJOR);
		long millis = System.currentTimeMillis();
		for(int i = 0; i < 20; i++)
			runMapOnlyScenario(false, MATRIX_ROWS, MATRIX_COLS, DataGenOrder.ROW_MAJOR);
		millis = System.currentTimeMillis() - millis;
		System.out.println("Map Only Without Tracking: " + millis + "ms");
	}

	@Test
	public void testMapOnlyVector() throws ExecutionException, InterruptedException {
		for(int i = 0; i < 10; i++)
			runMapOnlyScenario(true, VECTOR_ROWS, VECTOR_COLS, DataGenOrder.ROW_MAJOR);
		long millis = System.currentTimeMillis();
		for(int i = 0; i < 5; i++)
			runMapOnlyScenario(true, VECTOR_ROWS, VECTOR_COLS, DataGenOrder.ROW_MAJOR);
		millis = System.currentTimeMillis() - millis;
		System.out.println("Map Only Vector With Tracking: " + millis + "ms");
	}

	@Test
	public void testMapOnlyVectorWithoutTrackingPrimitives() throws ExecutionException, InterruptedException {
		for(int i = 0; i < 10; i++)
			runMapOnlyScenario(false, VECTOR_ROWS, VECTOR_COLS, DataGenOrder.ROW_MAJOR);
		long millis = System.currentTimeMillis();
		for(int i = 0; i < 5; i++)
			runMapOnlyScenario(false, VECTOR_ROWS, VECTOR_COLS, DataGenOrder.ROW_MAJOR);
		millis = System.currentTimeMillis() - millis;
		System.out.println("Map Only Vector Without Tracking: " + millis + "ms");
	}

	private void runPlanningScenario(boolean tracked) throws ExecutionException, InterruptedException {
		DMLConfig oldConf = ConfigurationManager.getDMLConfig();
		DMLConfig conf = new DMLConfig();
		conf.setTextValue(DMLConfig.LOCAL_TMP_DIR, "testTemp/OOCPlanning");
		ConfigurationManager.setGlobalConfig(conf);
		try {
			LocalFileUtils.createWorkingDirectory();
			OOCCacheManager.getCache();

			OOCStream<IndexedMatrixValue> t2 = createWritableStream();
			OOCStream<IndexedMatrixValue> join = createWritableStream();

			if(tracked) {
				OOCStream<IndexedMatrixValue> dGen1 = createTrackedDataGenStream(1.0);
				OOCStream<IndexedMatrixValue> dGen2 = createTrackedDataGenStream(2.0);
				transposeMapOOC(dGen2, t2, imv -> ((MatrixBlock) imv.getValue()).getInMemorySize(), this::transposeBlock);
				joinZipOOC(dGen1, t2, join, l -> 8000152L, this::addBlocks);

				CompletableFuture<Void> future = subscribeToCompletion(join);
				join.start();
				future.get();
			}
			else {
				UntrackedDataGen gen1 = createUntrackedDataGenStream(1.0);
				UntrackedDataGen gen2 = createUntrackedDataGenStream(2.0);
				mapOOC(gen2.output, t2, imv -> new IndexedMatrixValue(transposeIndexes(imv), transposeBlock(imv)));
				joinOOC(gen1.output, t2, join,
					(l, r) -> new IndexedMatrixValue(l.getIndexes(), addBlocks(l, r)),
					IndexedMatrixValue::getIndexes);

				CompletableFuture<Void> future = subscribeToCompletion(join);
				join.start();
				populateIndexStream(gen1.indexes, MATRIX_ROWS, MATRIX_COLS, DataGenOrder.ROW_MAJOR);
				populateIndexStream(gen2.indexes, MATRIX_ROWS, MATRIX_COLS, DataGenOrder.COL_MAJOR);
				future.get();
			}
		}
		finally {
			OOCCacheManager.reset();
			ConfigurationManager.setGlobalConfig(oldConf);
		}
	}

	private void runMapOnlyScenario(boolean tracked, int rows, int cols, DataGenOrder order)
		throws ExecutionException, InterruptedException {
		DMLConfig oldConf = ConfigurationManager.getDMLConfig();
		DMLConfig conf = new DMLConfig();
		conf.setTextValue(DMLConfig.LOCAL_TMP_DIR, "testTemp/OOCPlanning");
		ConfigurationManager.setGlobalConfig(conf);
		try {
			LocalFileUtils.createWorkingDirectory();
			OOCCacheManager.getCache();

			OOCStream<IndexedMatrixValue> out = createWritableStream();

			if(tracked) {
				OOCStream<IndexedMatrixValue> dGen = createTrackedDataGenStream(rows, cols, 1.0);
				mapOOC(dGen, out, imv -> ((MatrixBlock) imv.getValue()).getInMemorySize(), task ->
					task.setOutput(new IndexedMatrixValue(task.input().getIndexes(), multiplyBlock(task.input()))));

				CompletableFuture<Void> future = subscribeToCompletion(out);
				dGen.start();
				out.start();
				future.get();
			}
			else {
				UntrackedDataGen gen = createUntrackedDataGenStream(rows, cols, 1.0);
				mapOOC(gen.output, out, imv -> new IndexedMatrixValue(imv.getIndexes(), multiplyBlock(imv)));

				CompletableFuture<Void> future = subscribeToCompletion(out);
				out.start();
				populateIndexStream(gen.indexes, rows, cols, order);
				future.get();
			}
		}
		finally {
			OOCCacheManager.reset();
			ConfigurationManager.setGlobalConfig(oldConf);
		}
	}

	private CompletableFuture<Void> subscribeToCompletion(OOCStream<IndexedMatrixValue> out) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		out.setSubscriber(cb -> {
			try(cb) {
				if(cb.isEos())
					future.complete(null);
			}
		});
		return future;
	}

	private OOCStream<IndexedMatrixValue> createDataGenStream() {
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		out.setData(new MatrixObject(Types.ValueType.FP64, "null",
			new MetaData(new MatrixCharacteristics(MATRIX_ROWS, MATRIX_COLS, BLOCK_SIZE))));
		return out;
	}

	private OOCStream<IndexedMatrixValue> createDataGenStream(int rows, int cols) {
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		out.setData(new MatrixObject(Types.ValueType.FP64, "null",
			new MetaData(new MatrixCharacteristics(rows, cols, BLOCK_SIZE))));
		return out;
	}

	private OOCStream<IndexedMatrixValue> createTrackedDataGenStream(double value) {
		return createTrackedDataGenStream(MATRIX_ROWS, MATRIX_COLS, value);
	}

	private OOCStream<IndexedMatrixValue> createTrackedDataGenStream(int rows, int cols, double value) {
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		out.setData(new MatrixObject(Types.ValueType.FP64, "null",
			new MetaData(new MatrixCharacteristics(rows, cols, BLOCK_SIZE))));
		plannableDataGenOOC(out, ix -> getBlockInMemorySize(ix, rows, cols), task ->
			task.setOutput(new IndexedMatrixValue(task.input(), createFilledBlock(task.input(), rows, cols, value))));
		return out;
	}

	private UntrackedDataGen createUntrackedDataGenStream(double value) {
		return createUntrackedDataGenStream(MATRIX_ROWS, MATRIX_COLS, value);
	}

	private UntrackedDataGen createUntrackedDataGenStream(int rows, int cols, double value) {
		OOCStream<MatrixIndexes> indexes = createWritableStream();
		OOCStream<IndexedMatrixValue> out = createDataGenStream(rows, cols);
		mapOOC(indexes, out, ix -> new IndexedMatrixValue(ix, createFilledBlock(ix, rows, cols, value)));
		return new UntrackedDataGen(indexes, out);
	}

	private void populateIndexStream(OOCStream<MatrixIndexes> out, int rows, int cols, DataGenOrder order) {
		int rowBlocks = (rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
		int colBlocks = (cols + BLOCK_SIZE - 1) / BLOCK_SIZE;
		if(order == DataGenOrder.ROW_MAJOR) {
			for(int bi = 1; bi <= rowBlocks; bi++) {
				for(int bj = 1; bj <= colBlocks; bj++)
					out.enqueue(new MatrixIndexes(bi, bj));
			}
		}
		else {
			for(int bj = 1; bj <= colBlocks; bj++) {
				for(int bi = 1; bi <= rowBlocks; bi++)
					out.enqueue(new MatrixIndexes(bi, bj));
			}
		}
		out.closeInput();
	}

	private MatrixBlock createFilledBlock(MatrixIndexes ix, int rows, int cols, double value) {
		int blockRows = getBlockRows(ix, rows);
		int blockCols = getBlockCols(ix, cols);
		return new MatrixBlock(blockRows, blockCols, value);
	}

	private long getBlockInMemorySize(MatrixIndexes ix, int rows, int cols) {
		return (long) getBlockRows(ix, rows) * getBlockCols(ix, cols) * Double.BYTES + 152L;
	}

	private int getBlockRows(MatrixIndexes ix, int rows) {
		return Math.min(BLOCK_SIZE, rows - ((int) ix.getRowIndex() - 1) * BLOCK_SIZE);
	}

	private int getBlockCols(MatrixIndexes ix, int cols) {
		return Math.min(BLOCK_SIZE, cols - ((int) ix.getColumnIndex() - 1) * BLOCK_SIZE);
	}

	private MatrixIndexes transposeIndexes(IndexedMatrixValue imv) {
		return new MatrixIndexes(imv.getIndexes().getColumnIndex(), imv.getIndexes().getRowIndex());
	}

	private MatrixBlock transposeBlock(IndexedMatrixValue imv) {
		return (MatrixBlock) imv.getValue().reorgOperations(
			new ReorgOperator(SwapIndex.getSwapIndexFnObject()), new MatrixBlock(), -1, -1, -1);
	}

	private MatrixBlock addBlocks(IndexedMatrixValue l, IndexedMatrixValue r) {
		return (MatrixBlock) l.getValue().binaryOperations(
			new BinaryOperator(Plus.getPlusFnObject()), r.getValue(), new MatrixBlock());
	}

	private MatrixBlock multiplyBlock(IndexedMatrixValue imv) {
		return (MatrixBlock) imv.getValue().scalarOperations(
			new RightScalarOperator(Multiply.getMultiplyFnObject(), 2.0), new MatrixBlock());
	}

	@Override
	public void processInstruction(ExecutionContext ec) {}
}
