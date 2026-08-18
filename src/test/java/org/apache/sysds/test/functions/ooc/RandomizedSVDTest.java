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
 * Two-pass randomized SVD. Same task as the exact pca path but with a fixed, small number of passes over X, which makes
 * it the direct comparison point for how much of PCA's cost is the algorithm rather than the engine.
 */
public class RandomizedSVDTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "RandomizedSVD";
	private static final String TEST_CLASS_DIR = TEST_DIR + RandomizedSVDTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String OUT_S_OOC = "S";
	private static final String OUT_V_OOC = "V";
	private static final String OUT_S_CP = "S_cp";
	private static final String OUT_V_CP = "V_cp";

	private static final int ROWS = 10000;
	private static final int COLS = 200;
	private static final int RANK = 10;
	private static final int BLOCK_SIZE = 1000;
	private static final int SEED = 31;
	private static final double EPS = 1e-6;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testRandomizedSVDDense() {
		runRandomizedSVDTest(1.0);
	}

	@Test
	public void testRandomizedSVDSparse() {
		runRandomizedSVDTest(0.2);
	}

	private void runRandomizedSVDTest(double sparsity) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			double[][] x = getRandomMatrix(ROWS, COLS, -1, 1, sparsity, SEED);
			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(x));

			runOOCAndCP(TEST_NAME, buildArgs(output(OUT_S_OOC), output(OUT_V_OOC)),
				buildArgs(output(OUT_S_CP), output(OUT_V_CP)));

			MatrixBlock sOOC = DataConverter.readMatrixFromHDFS(output(OUT_S_OOC), Types.FileFormat.BINARY, RANK, RANK,
				BLOCK_SIZE);
			MatrixBlock sCP = DataConverter.readMatrixFromHDFS(output(OUT_S_CP), Types.FileFormat.BINARY, RANK, RANK,
				BLOCK_SIZE);
			MatrixBlock vOOC = DataConverter.readMatrixFromHDFS(output(OUT_V_OOC), Types.FileFormat.BINARY, COLS, RANK,
				BLOCK_SIZE);
			MatrixBlock vCP = DataConverter.readMatrixFromHDFS(output(OUT_V_CP), Types.FileFormat.BINARY, COLS, RANK,
				BLOCK_SIZE);

			TestUtils.compareMatrices(sOOC, sCP, EPS);
			TestUtils.compareMatrices(vOOC, vCP, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private String[] buildArgs(String outS, String outV) {
		return new String[] {input(INPUT_X), Integer.toString(RANK), Integer.toString(SEED), outS, outV};
	}
}
