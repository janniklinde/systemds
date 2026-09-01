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

package org.apache.sysds.test.component.matrix;

import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

public class GroupedAggregateParallelTest {
	private static final int ROWS = 1024;
	private static final int COLS = 512;
	private static final int GROUPS = 7;

	@Test
	public void testGroupedAggregateMeanParallel() {
		runParallelTest("mean");
	}

	@Test
	public void testGroupedAggregateVarianceParallel() {
		runParallelTest("variance");
	}

	@Test
	public void testGroupedAggregateCountParallel() {
		runParallelTest("count");
	}

	@Test
	public void testGroupedAggregateMinParallel() {
		runParallelTest("min");
	}

	@Test
	public void testGroupedAggregateMaxParallel() {
		runParallelTest("max");
	}

	@Test
	public void testGroupedAggregateSumParallel() {
		runParallelTest("sum");
	}

	private void runParallelTest(String fn) {
		MatrixBlock target = MatrixBlock.randOperations(ROWS, COLS, 1.0, -2, 2, "uniform", 7);
		MatrixBlock groups = new MatrixBlock(ROWS, 1, false);
		for(int row = 0; row < ROWS; row++)
			groups.set(row, 0, row % GROUPS + 1);
		Operator op = InstructionUtils.parseGroupedAggOperator(fn, null);

		MatrixBlock sequential = groups.groupedAggOperations(target, null, new MatrixBlock(), GROUPS, op, 1);
		MatrixBlock parallel = groups.groupedAggOperations(target, null, new MatrixBlock(), GROUPS, op, 8);

		TestUtils.compareMatrices(sequential, parallel, 1e-12);
	}
}
