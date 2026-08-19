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

public class PlannerUnaryTest extends AutomatedTestBase {
	private static final String TEST_NAME = "PlannerUnary";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + PlannerUnaryTest.class.getSimpleName() + "/";
	private static final int ROWS = 400;
	private static final int COLS = 400;
	private static final int BLOCK_SIZE = 100;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testPlannerUnaryOperations() throws IOException {
		Types.ExecMode oldPlatform = setExecMode(Types.ExecMode.SINGLE_NODE);
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";
			MatrixBlock matrix = MatrixBlock.randOperations(ROWS, COLS, 0.7, -2, 2, "uniform", 7);
			matrix.set(ROWS - 1, COLS - 1, -1);
			MatrixBlock vector = new MatrixBlock(ROWS, 1, false);
			for(int row = 0; row < ROWS; row++)
				vector.set(row, 0, row % 5 + 1);
			writeInput("X", matrix, ROWS, COLS);
			writeInput("V", vector, ROWS, 1);

			programArgs = arguments(true, "ooc");
			runTest(true, false, null, -1);
			Assert.assertTrue(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.UAKP));
			Assert.assertTrue(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.CONTAINS));
			Assert.assertTrue(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.REXPAND));
			Assert.assertTrue(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.UCUMKP));

			programArgs = arguments(false, "target");
			runTest(true, false, null, -1);
			compare("S", 1, 1, 1e-8);
			compare("C", 1, 1, 0);
			compare("E", ROWS, 5, 0);
			compare("U", ROWS, COLS, 1e-8);
		}
		finally {
			resetExecMode(oldPlatform);
		}
	}

	private String[] arguments(boolean ooc, String suffix) {
		String[] args = new String[ooc ? 9 : 8];
		int offset = 0;
		args[offset++] = "-stats";
		if(ooc)
			args[offset++] = "-ooc";
		args[offset++] = "-args";
		args[offset++] = input("X");
		args[offset++] = input("V");
		args[offset++] = output("S_" + suffix);
		args[offset++] = output("C_" + suffix);
		args[offset++] = output("E_" + suffix);
		args[offset] = output("U_" + suffix);
		return args;
	}

	private void compare(String name, int rows, int cols, double tolerance) throws IOException {
		MatrixBlock actual = DataConverter.readMatrixFromHDFS(output(name + "_ooc"), Types.FileFormat.BINARY, rows,
			cols, BLOCK_SIZE);
		MatrixBlock expected = DataConverter.readMatrixFromHDFS(output(name + "_target"), Types.FileFormat.BINARY, rows,
			cols, BLOCK_SIZE);
		TestUtils.compareMatrices(actual, expected, tolerance);
	}

	private void writeInput(String name, MatrixBlock value, int rows, int cols) throws IOException {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
		writer.writeMatrixToHDFS(value, input(name), rows, cols, BLOCK_SIZE, value.getNonZeros());
		HDFSTool.writeMetaDataFile(input(name + ".mtd"), Types.ValueType.FP64,
			new MatrixCharacteristics(rows, cols, BLOCK_SIZE, value.getNonZeros()), Types.FileFormat.BINARY);
	}
}
