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

public class SharedProducerBroadcastJoinTest extends AutomatedTestBase {
	private static final String TEST_NAME = "SharedProducerBroadcastJoin";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + SharedProducerBroadcastJoinTest.class.getSimpleName() + "/";
	private static final String INPUT_A = "A";
	private static final String INPUT_Y = "Y";
	private static final String INPUT_Z = "Z";
	private static final String OUTPUT = "res";
	private static final double EPS = 1e-6;
	private static final int ROWS_A = 1;
	private static final int COLS_A = 2500;
	private static final int ROWS_Y = 1800;
	private static final int BLEN = 1000;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testSharedProducerFeedsBroadcastAndJoin() {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";
			programArgs = new String[] {"-explain", "hops", "-stats", "-ooc", "-args",
				input(INPUT_A), input(INPUT_Y), input(INPUT_Z), output(OUTPUT)};

			double[][] a = getRandomMatrix(ROWS_A, COLS_A, 0, 1, 1.0, 7);
			double[][] y = getRandomMatrix(ROWS_Y, COLS_A, 0, 1, 0.35, 8);
			double[][] z = getRandomMatrix(ROWS_A, COLS_A, 0, 1, 1.0, 9);

			MatrixBlock aBlock = DataConverter.convertToMatrixBlock(a);
			MatrixBlock yBlock = DataConverter.convertToMatrixBlock(y);
			MatrixBlock zBlock = DataConverter.convertToMatrixBlock(z);
			writeInput(INPUT_A, aBlock, ROWS_A, COLS_A);
			writeInput(INPUT_Y, yBlock, ROWS_Y, COLS_A);
			writeInput(INPUT_Z, zBlock, ROWS_A, COLS_A);

			runTest(true, false, null, -1);

			MatrixBlock out = DataConverter.readMatrixFromHDFS(output(OUTPUT), Types.FileFormat.BINARY, 1, 1, BLEN);
			Assert.assertEquals(expected(a, y, z), out.get(0, 0), EPS);
			Assert.assertTrue("OOC tee was not used for the shared producer",
				heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.TEE.toString()));
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private void writeInput(String name, MatrixBlock block, long rows, long cols) throws IOException {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
		writer.writeMatrixToHDFS(block, input(name), rows, cols, BLEN, block.getNonZeros());
		HDFSTool.writeMetaDataFile(input(name + ".mtd"), Types.ValueType.FP64,
			new MatrixCharacteristics(rows, cols, BLEN, block.getNonZeros()), Types.FileFormat.BINARY);
	}

	private static double expected(double[][] a, double[][] y, double[][] z) {
		double ret = 0;
		for(int r = 0; r < ROWS_Y; r++)
			for(int c = 0; c < COLS_A; c++)
				ret += y[r][c] * (a[0][c] + 1);

		for(int c = 0; c < COLS_A; c++)
			ret += a[0][c] + 1 + z[0][c];

		return ret;
	}
}
