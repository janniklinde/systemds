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
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class OOCPrimitiveChainTest {
	private static final int ROWS = 100000000;
	private static final int COLS = 100;
	private static final int BLEN = 200;
	private static final int K = 10;
	private static final int TILES = ROWS / BLEN;

	@Test
	public void testDataGenEquiMapChain() {
		List<OOCStream<IndexedMatrixValue>> streams = new ArrayList<>();
		List<StreamContext> scMaps = new ArrayList<>();
		for(int k = 0; k < K+1; k++) {
			streams.add(createMatrixStream());
			scMaps.add(new StreamContext(0, "op_" + k).addOutStream(streams.get(streams.size()-1)));
			if(k == 0)
				OOCInstructionUtils.dataGen(streams.get(k), ix -> new MatrixBlock(BLEN, COLS, 5.0), scMaps.get(k));
			else
				OOCInstructionUtils.equiMapBlock(streams.get(k-1), streams.get(k),
					mb -> mb.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 2.0), new MatrixBlock()), scMaps.get(k));
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
				if(checksum < (7.0 + (K-1)*2.0) * BLEN * COLS - 1e-9 || checksum > (7.0 + (K-1)*2.0) * BLEN * COLS + 1e-9)
					throw new AssertionError("Wrong checksum: " + checksum + " at " + imv.getIndexes());
				count.incrementAndGet();
			}
			catch(Throwable t) {
				future.completeExceptionally(t);
			}
			finally {
				cb.close();
			}
		});

		streams.get(streams.size()-1).start();
		future.join();

		Assert.assertEquals(TILES, count.get());
	}

	private static OOCStream<IndexedMatrixValue> createMatrixStream() {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixCharacteristics dc = new MatrixCharacteristics((long) TILES * BLEN, COLS, BLEN, -1);
		stream.setData(new MatrixObject(ValueType.FP64, null, new MetaDataFormat(dc, FileFormat.BINARY)));
		return stream;
	}
}
