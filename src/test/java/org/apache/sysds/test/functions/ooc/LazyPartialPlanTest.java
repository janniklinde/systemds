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

import java.io.IOException;

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LazyPartialPlanTest extends AutomatedTestBase {
	private static final String TEST_NAME = "LazyPartialPlan";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + LazyPartialPlanTest.class.getSimpleName() + "/";

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testUnusedOutputRemainsUncompiled() throws IOException {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			MatrixBlock x = DataConverter.convertToMatrixBlock(getRandomMatrix(1200, 1100, 1, 2, 1, 7));
			MatrixBlock y = DataConverter.convertToMatrixBlock(getRandomMatrix(1200, 1100, 1, 2, 1, 8));
			writeBinaryWithMTD("X", x);
			writeBinaryWithMTD("Y", y);
			programArgs = new String[] {"-explain", "-stats", "-ooc", "-args", input("X"), input("Y"), output("A")};
			runTest(true, false, null, -1);

			MatrixBlock actual = DataConverter.readMatrixFromHDFS(output("A"), Types.FileFormat.BINARY, 1200, 1100,
				1000);
			MatrixBlock expected = x.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 7),
				new MatrixBlock());
			TestUtils.compareMatrices(actual, expected, 0);
			Assert.assertTrue(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + "lazyooc"));
			Assert.assertTrue(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.PLUS));
			Assert.assertFalse(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.MULT));
		}
		finally {
			resetExecMode(platformOld);
		}
	}
}
