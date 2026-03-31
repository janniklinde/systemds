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
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;
import org.apache.sysds.runtime.util.LocalFileUtils;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class OOCPlanningTest extends OOCInstruction {
	public OOCPlanningTest() {
		super(null, null, "");
	}

	@Test
	public void test() throws ExecutionException, InterruptedException {
		for(int i = 0; i < 30; i++) {
			runPlanningScenario(true);
		}
		long millis = System.currentTimeMillis();
		for(int i = 0; i < 10; i++) {
			runPlanningScenario(true);
		}
		millis = System.currentTimeMillis() - millis;
		System.out.println("With Tracking: " + millis + "ms");
	}

	@Test
	public void testWithoutTrackingPrimitives() throws ExecutionException, InterruptedException {
		for(int i = 0; i < 30; i++) {
			runPlanningScenario(false);
		}
		long millis = System.currentTimeMillis();
		for(int i = 0; i < 10; i++) {
			runPlanningScenario(false);
		}
		millis = System.currentTimeMillis() - millis;
		System.out.println("Without Tracking: " + millis + "ms");
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
				OOCStream<IndexedMatrixValue> dGen1;
				OOCStream<IndexedMatrixValue> dGen2;

				if(tracked) {
					dGen1 = createTrackedDataGenStream(1.0);
					dGen2 = createTrackedDataGenStream(2.0);
					transposeMapOOC(dGen2, t2, imv -> ((MatrixBlock) imv.getValue()).getInMemorySize(), this::transposeBlock);
					joinZipOOC(dGen1, t2, join, l -> 8000152L, this::addBlocks);
				}
				else {
					dGen1 = createDataGenStream();
					dGen2 = createDataGenStream();
					addOutStream(t2, join);
					mapOOC(dGen2, t2, imv -> new IndexedMatrixValue(transposeIndexes(imv), transposeBlock(imv)));
					joinOOC(dGen1, t2, join,
						(l, r) -> new IndexedMatrixValue(l.getIndexes(), addBlocks(l, r)),
						IndexedMatrixValue::getIndexes);
				}

			CompletableFuture<Void> future = new CompletableFuture<>();
			join.setSubscriber(cb -> {
				try(cb) {
					if(cb.isEos()) {
						System.out.println("EOS");
						future.complete(null);
						return;
					}
					//System.out.println("Received: " + cb.get().getIndexes());
				}
				});
				join.start();
				if(!tracked) {
					populateDataGenStream(dGen1, 1.0);
					populateDataGenStream(dGen2, 2.0);
				}
				future.get();
			}
			finally {
			OOCCacheManager.reset();
			ConfigurationManager.setGlobalConfig(oldConf);
		}
	}

	private OOCStream<IndexedMatrixValue> createDataGenStream() {
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		out.setData(new MatrixObject(Types.ValueType.FP64, "null",
			new MetaData(new MatrixCharacteristics(10000, 10000, 1000))));
		return out;
	}

	private OOCStream<IndexedMatrixValue> createTrackedDataGenStream(double value) {
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		out.setData(new MatrixObject(Types.ValueType.FP64, "null",
			new MetaData(new MatrixCharacteristics(10000, 10000, 1000))));
		plannableDataGenOOC(out, ix -> 8000152L, task ->
			task.setOutput(new IndexedMatrixValue(task.input(), new MatrixBlock(1000, 1000, value))));
		return out;
	}

	private void populateDataGenStream(OOCStream<IndexedMatrixValue> out, double value) {
		for(int bi = 1; bi <= 10; bi++) {
			for(int bj = 1; bj <= 10; bj++)
				out.enqueue(new IndexedMatrixValue(new MatrixIndexes(bi, bj), new MatrixBlock(1000, 1000, value)));
		}
		out.closeInput();
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

	@Override
	public void processInstruction(ExecutionContext ec) {}
}
