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

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

/**
 * Alternating least squares on a sparse rating matrix. The distinguishing property for OOC is that the two half-steps
 * traverse the same X in opposite orders, row-wise to solve for U and column-wise to solve for V, so a layout that
 * suits one half penalizes the other.
 */
public class ALSTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "ALS";
	private static final String TEST_CLASS_DIR = TEST_DIR + ALSTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String OUT_U_OOC = "U";
	private static final String OUT_V_OOC = "V";
	private static final String OUT_U_CP = "U_cp";
	private static final String OUT_V_CP = "V_cp";

	private static final int ROWS = 3000;
	private static final int COLS = 1200;
	private static final int RANK = 10;
	private static final int MAX_ITER = 5;
	private static final int BLOCK_SIZE = 1000;
	private static final int SEED = 19;
	private static final double SPARSITY = 0.05;
	private static final double REG = 1e-6;
	private static final double EPS = 1e-6;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testALSSparse() {
		runALSTest(SPARSITY);
	}

	@Test
	public void testALSDense() {
		runALSTest(1.0);
	}

	private void runALSTest(double sparsity) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			double[][] x = getRandomMatrix(ROWS, COLS, 1, 5, sparsity, SEED);
			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(x));

			runOOCAndCP(TEST_NAME, buildArgs(output(OUT_U_OOC), output(OUT_V_OOC)),
				buildArgs(output(OUT_U_CP), output(OUT_V_CP)));

			MatrixBlock uOOC = DataConverter.readMatrixFromHDFS(output(OUT_U_OOC), Types.FileFormat.BINARY, ROWS, RANK,
				BLOCK_SIZE);
			MatrixBlock uCP = DataConverter.readMatrixFromHDFS(output(OUT_U_CP), Types.FileFormat.BINARY, ROWS, RANK,
				BLOCK_SIZE);
			MatrixBlock vOOC = DataConverter.readMatrixFromHDFS(output(OUT_V_OOC), Types.FileFormat.BINARY, RANK, COLS,
				BLOCK_SIZE);
			MatrixBlock vCP = DataConverter.readMatrixFromHDFS(output(OUT_V_CP), Types.FileFormat.BINARY, RANK, COLS,
				BLOCK_SIZE);

			TestUtils.compareMatrices(uOOC, uCP, EPS);
			TestUtils.compareMatrices(vOOC, vCP, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private String[] buildArgs(String outU, String outV) {
		return new String[] {input(INPUT_X), Integer.toString(RANK), Double.toString(REG), Integer.toString(MAX_ITER),
			Integer.toString(SEED), outU, outV};
	}
}
