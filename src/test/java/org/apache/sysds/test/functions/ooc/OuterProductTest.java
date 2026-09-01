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

public class OuterProductTest extends AutomatedTestBase {
	private static final String TEST_NAME = "OuterProduct";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + OuterProductTest.class.getSimpleName() + "/";
	private static final double eps = 1e-8;
	private static final int blen = 1000;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testOuterProductSingleBlock() {
		runOuterProductTest(400, 300);
	}

	@Test
	public void testOuterProductMultiBlockRowVector() {
		runOuterProductTest(64, 2400);
	}

	@Test
	public void testOuterProductMultiBlockBoth() {
		runOuterProductTest(2400, 2400);
	}

	@Test
	public void testOuterProductSingleCell() {
		runOuterProductTest(1, 2400);
	}

	private void runOuterProductTest(int rows, int cols) {
		ExecMode platformOld = setExecMode(ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			writeInput("u", DataConverter.convertToMatrixBlock(getRandomMatrix(rows, 1, 1, 7, 1, 7)), rows, 1);
			writeInput("v", DataConverter.convertToMatrixBlock(getRandomMatrix(1, cols, 1, 7, 1, 3)), 1, cols);

			programArgs = new String[] {"-explain", "-stats", "-ooc", "-args", input("u"), input("v"), output("R")};
			runTest(true, false, null, -1);
			Assert.assertTrue("OOC wasn't used for the outer product",
				heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.PLUS));

			programArgs = new String[] {"-explain", "-stats", "-args", input("u"), input("v"), output("R_target")};
			runTest(true, false, null, -1);

			MatrixBlock actual = DataConverter.readMatrixFromHDFS(output("R"), Types.FileFormat.BINARY, rows, cols,
				blen);
			MatrixBlock expected = DataConverter.readMatrixFromHDFS(output("R_target"), Types.FileFormat.BINARY, rows,
				cols, blen);
			TestUtils.compareMatrices(expected, actual, eps);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private void writeInput(String name, MatrixBlock block, int rows, int cols) throws IOException {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
		writer.writeMatrixToHDFS(block, input(name), rows, cols, blen, block.getNonZeros());
		HDFSTool.writeMetaDataFile(input(name + ".mtd"), Types.ValueType.FP64,
			new MatrixCharacteristics(rows, cols, blen, block.getNonZeros()), Types.FileFormat.BINARY);
	}
}
