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

import java.io.IOException;

public class ReshapeConsumeTest extends AutomatedTestBase {
	private static final String TEST_NAME1 = "MatrixReshapeConsumeRowWise";
	private static final String TEST_NAME2 = "MatrixReshapeConsumeColWise";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + ReshapeConsumeTest.class.getSimpleName() + "/";
	private static final String INPUT_NAME = "X";
	private static final String OUTPUT_NAME = "Y";
	private static final double eps = 1e-8;
	private static final int blen = 1000;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1));
		addTestConfiguration(TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2));
	}

	@Test
	public void testReshapeConsumeRowWiseSingleColumn() {
		runReshapeConsumeTest(TEST_NAME1, 2400, 1, 16, 150);
	}

	@Test
	public void testReshapeConsumeRowWiseNarrow() {
		runReshapeConsumeTest(TEST_NAME1, 2400, 3, 48, 150);
	}

	@Test
	public void testReshapeConsumeColWiseSingleRow() {
		runReshapeConsumeTest(TEST_NAME2, 1, 2400, 150, 16);
	}

	private void runReshapeConsumeTest(String testName, int rlen, int clen, int rows, int cols) {
		ExecMode platformOld = setExecMode(ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(testName);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + testName + ".dml";

			MatrixBlock X = DataConverter.convertToMatrixBlock(getRandomMatrix(rlen, clen, 1, 7, 1, 7));
			MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
			writer.writeMatrixToHDFS(X, input(INPUT_NAME), rlen, clen, blen, X.getNonZeros());
			HDFSTool.writeMetaDataFile(input(INPUT_NAME + ".mtd"), Types.ValueType.FP64,
				new MatrixCharacteristics(rlen, clen, blen, X.getNonZeros()), Types.FileFormat.BINARY);

			programArgs = new String[] {"-explain", "-stats", "-ooc", "-args", input(INPUT_NAME), String.valueOf(rlen),
				String.valueOf(clen), String.valueOf(rows), String.valueOf(cols), output(OUTPUT_NAME)};
			runTest(true, false, null, -1);
			Assert.assertTrue("OOC wasn't used for reshape",
				heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.RESHAPE));

			programArgs = new String[] {"-explain", "-stats", "-args", input(INPUT_NAME), String.valueOf(rlen),
				String.valueOf(clen), String.valueOf(rows), String.valueOf(cols), output(OUTPUT_NAME + "_target")};
			runTest(true, false, null, -1);

			MatrixBlock actual = DataConverter.readMatrixFromHDFS(output(OUTPUT_NAME), Types.FileFormat.BINARY, rows,
				cols, blen);
			MatrixBlock expected = DataConverter.readMatrixFromHDFS(output(OUTPUT_NAME + "_target"),
				Types.FileFormat.BINARY, rows, cols, blen);
			TestUtils.compareMatrices(expected, actual, eps);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}
}
