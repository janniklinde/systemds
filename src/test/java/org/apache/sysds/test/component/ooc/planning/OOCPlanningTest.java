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
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.functionobjects.SwapIndex;
import org.apache.sysds.runtime.instructions.ooc.OOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaData;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class OOCPlanningTest extends OOCInstruction {
	public OOCPlanningTest() {
		super(null, null, "");
	}

	@Test
	public void test() throws ExecutionException, InterruptedException {
		OOCStream<IndexedMatrixValue> dGen1 = createWritableStream();
		dGen1.setData(new MatrixObject(Types.ValueType.FP64, "null", new MetaData(new MatrixCharacteristics(10000, 10000, 1000))));
		plannableDataGenOOC(dGen1, ix -> new IndexedMatrixValue(ix, new MatrixBlock(1000, 1000, 1.0)));
		OOCStream<IndexedMatrixValue> dGen2 = createWritableStream();
		dGen2.setData(new MatrixObject(Types.ValueType.FP64, "null", new MetaData(new MatrixCharacteristics(10000, 10000, 1000))));
		plannableDataGenOOC(dGen2, ix -> new IndexedMatrixValue(ix, new MatrixBlock(1000, 1000, 1.0)));
		OOCStream<IndexedMatrixValue> t2 = createWritableStream();
		transposeMapOOC(dGen2, t2, imv -> (MatrixBlock)imv.getValue().reorgOperations(new ReorgOperator(SwapIndex.getSwapIndexFnObject()), new MatrixBlock(), -1, -1, -1));
		OOCStream<IndexedMatrixValue> join = createWritableStream();
		joinZipOOC(dGen1, t2, join, (l, r) -> {
			return (MatrixBlock)l.getValue().binaryOperations(new BinaryOperator(Plus.getPlusFnObject()), r.getValue(), new MatrixBlock());
		});

		join.start();

		CompletableFuture<Void> future = new CompletableFuture<>();
		join.setSubscriber(cb -> {
			if(cb.isEos()) {
				System.out.println("EOS");
				future.complete(null);
				return;
			}
			System.out.println("Received: " + cb.get().getIndexes());
		});
		future.get();
	}

	@Override
	public void processInstruction(ExecutionContext ec) {}
}
