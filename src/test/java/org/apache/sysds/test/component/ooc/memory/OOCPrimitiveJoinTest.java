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
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class OOCPrimitiveJoinTest {
	private static final int ROWS = 16384*2;
	private static final int COLS = 16384*2;
	private static final int K = 5;
	private static final int REPS = 15;
	private static int BLEN = 256;
	private static int TILES = ((ROWS-1) / BLEN + 1) * ((COLS-1) / BLEN + 1);

	@Test
	public void test() {
		/*System.out.println("BLEN,Time[ms]");
		for(int pow = 4; pow <= 10; pow++) {
			BLEN = 2 << pow;
			TILES = ((ROWS-1) / BLEN + 1) * ((COLS-1) / BLEN + 1);
			for(int i = 0; i < REPS; i++) {
				testDataGenEquiMapChain();
			}

			long millis = System.currentTimeMillis();
			for(int i = 0; i < REPS; i++) {
				testDataGenEquiMapChain();
			}
			System.out.println(BLEN + "," + (System.currentTimeMillis() - millis));
		}*/
		testDataGenEquiMapChain();
	}

	private void testDataGenEquiMapChain() {
		List<List<OOCStream<IndexedMatrixValue>>> streamsList = new ArrayList<>();
		streamsList.add(new ArrayList<>());
		streamsList.add(new ArrayList<>());
		streamsList.add(new ArrayList<>());
		List<List<StreamContext>> scMapsList = new ArrayList<>();
		scMapsList.add(new ArrayList<>());
		scMapsList.add(new ArrayList<>());
		scMapsList.add(new ArrayList<>());
		for(int i = 0; i < 2; i++) {
			var streams = streamsList.get(i);
			var scMaps = scMapsList.get(i);
			for(int k = 0; k < K + 1; k++) {
				streams.add(createMatrixStream());
				scMaps.add(new StreamContext(0, "op_" + k).addOutStream(streams.get(streams.size() - 1)));
				if(k == 0)
					OOCInstructionUtils.dataGen(streams.get(k),
						ix -> new MatrixBlock(OOCUtils.getNumRowsOfTile(ix, ROWS, BLEN),
							OOCUtils.getNumColsOfTile(ix, COLS, BLEN), 5.0), scMaps.get(k));
				else
					OOCInstructionUtils.equiMap(streams.get(k - 1), streams.get(k),
						mb -> mb.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 2.0),
							new MatrixBlock()), scMaps.get(k));
			}
		}

		var streams = streamsList.get(2);
		var scMaps = scMapsList.get(2);
		var l = streamsList.get(0);
		var r = streamsList.get(1);
		streams.add(createMatrixStream());
		scMaps.add(new StreamContext(0, "op_join").addOutStream(streams.get(0)));
		OOCInstructionUtils.equiJoin(List.of(l.get(l.size()-1), r.get(r.size()-1)), streams.get(0),
			col -> col.get(0).binaryOperations(new BinaryOperator(Plus.getPlusFnObject()), col.get(1)),
			scMaps.get(0));
		for(int k = 1; k < K; k++) {
			streams.add(createMatrixStream());
			scMaps.add(new StreamContext(0, "op_" + k).addOutStream(streams.get(streams.size() - 1)));
			OOCInstructionUtils.equiMap(streams.get(k - 1), streams.get(k),
				mb -> mb.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 2.0),
					new MatrixBlock()), scMaps.get(k));
		}

		CompletableFuture<Void> future = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger();

		streams.get(streams.size()-1).setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					future.complete(null);
					return;
				}

				IndexedMatrixValue imv = cb.get();
				double checksum = ((MatrixBlock) imv.getValue()).sum();
				/*if(checksum < (7.0 + (K-1)*2.0) * imv.getValue().getNumRows() * imv.getValue().getNumColumns() - 1e-9
					|| checksum > (7.0 + (K-1)*2.0) * imv.getValue().getNumRows() * imv.getValue().getNumColumns() + 1e-9)
					throw new AssertionError("Wrong checksum: " + checksum + " at " + imv.getIndexes());*/
				count.incrementAndGet();
			}
			catch(Throwable t) {
				future.completeExceptionally(t);
			}
			finally {
				cb.close();
			}
		});

		streamsList.get(0).get(1).getPrimitive().requestPattern(OOCAccessPattern.COL_MAJOR);
		streamsList.get(1).get(1).getPrimitive().requestPattern(OOCAccessPattern.ROW_MAJOR);
		streams.get(streams.size()-1).start();
		future.join();

		Assert.assertEquals(TILES, count.get());
	}

	private static OOCStream<IndexedMatrixValue> createMatrixStream() {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixCharacteristics dc = new MatrixCharacteristics(ROWS, COLS, BLEN, -1);
		stream.setData(new MatrixObject(ValueType.FP64, null, new MetaDataFormat(dc, FileFormat.BINARY)));
		return stream;
	}
}
