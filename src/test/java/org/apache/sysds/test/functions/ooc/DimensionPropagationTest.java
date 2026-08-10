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

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.io.MatrixWriter;
import org.apache.sysds.runtime.io.MatrixWriterFactory;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DimensionPropagationTest extends AutomatedTestBase {
	private final static String TEST_NAME = "DimensionPropagation";
	private final static String TEST_DIR = "functions/ooc/";
	private final static String TEST_CLASS_DIR = TEST_DIR + DimensionPropagationTest.class.getSimpleName() + "/";
	private final static long ulps = 128;

	private final static String INPUT_X = "X";
	private final static String OUTPUT_NAME = "res";

	private final static int rows = 2500;
	private final static int cols = 300;
	private final static int blen = 1000;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	/**
	 * removeEmpty resolves a data-dependent row count at runtime. Every operator after it starts from a compile-time
	 * unknown, so the chain only completes if each one republishes the dimensions it can derive.
	 */
	@Test
	public void testDimensionsSurviveDataDependentRowCount() {
		ExecMode platformOld = setExecMode(ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			writeInputs();

			programArgs = new String[] {"-stats", "-ooc", "-args", input(INPUT_X), output(OUTPUT_NAME)};
			runTest(true, false, null, -1);

			String prefix = Instruction.OOC_INST_PREFIX;
			Assert.assertTrue("OOC wasn't used for removeEmpty",
				heavyHittersContainsString(prefix + Opcodes.RMEMPTY));
			Assert.assertTrue("OOC wasn't used for transpose",
				heavyHittersContainsString(prefix + Opcodes.TRANSPOSE));

			programArgs = new String[] {"-stats", "-args", input(INPUT_X), output(OUTPUT_NAME + "_target")};
			runTest(true, false, null, -1);

			MatrixBlock actual = DataConverter.readMatrixFromHDFS(output(OUTPUT_NAME), Types.FileFormat.BINARY, cols,
				cols, blen);
			MatrixBlock expected = DataConverter.readMatrixFromHDFS(output(OUTPUT_NAME + "_target"),
				Types.FileFormat.BINARY, cols, cols, blen);
			//the tsmm sums block-wise, so the result differs from CP in the last bits rather than exactly
			TestUtils.compareMatricesBitAvgDistance(expected, actual, ulps, ulps);
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private void writeInputs() throws Exception {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);

		double[][] x = getRandomMatrix(rows, cols, 0, 1, 1, 7);
		MatrixBlock xBlock = DataConverter.convertToMatrixBlock(x);
		writer.writeMatrixToHDFS(xBlock, input(INPUT_X), rows, cols, blen, xBlock.getNonZeros());
		HDFSTool.writeMetaDataFile(input(INPUT_X + ".mtd"), Types.ValueType.FP64,
			new MatrixCharacteristics(rows, cols, blen, xBlock.getNonZeros()), Types.FileFormat.BINARY);
	}
}
