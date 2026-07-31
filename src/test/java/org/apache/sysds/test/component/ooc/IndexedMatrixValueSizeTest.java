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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.junit.Test;

public class IndexedMatrixValueSizeTest {

	/**
	 * The size of a value is what the OOC memory system charges for holding it. A dense-allocated block with few
	 * non-zeros serializes in sparse format, so its serialized size is a small fraction of its footprint: charging
	 * that would let the cache hold an unbounded number of such blocks while believing itself far below its limit.
	 */
	@Test
	public void denseBlockWithFewNonZerosIsChargedItsFootprint() {
		MatrixBlock block = new MatrixBlock(1000, 1000, false);
		block.allocateDenseBlock();
		for(int i = 0; i < 1000; i++)
			block.set(i, i, 1d);
		block.setNonZeros(1000);

		long footprint = block.getInMemorySize();
		assertTrue("expected a dense allocation", footprint > 8_000_000L);
		assertTrue("expected a much smaller serialized form", block.getExactSerializedSize() < footprint / 4);

		IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1), block);
		assertEquals(footprint, value.size());
	}

	@Test
	public void sparseBlockIsChargedItsFootprint() {
		MatrixBlock block = new MatrixBlock(1000, 1000, true);
		for(int i = 0; i < 1000; i++)
			block.set(i, i, 1d);

		IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1), block);
		assertTrue(value.size() >= block.getInMemorySize());
		assertTrue(value.size() >= block.getExactSerializedSize());
	}
}
