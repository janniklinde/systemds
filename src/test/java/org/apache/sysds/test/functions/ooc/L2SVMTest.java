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
import java.util.Random;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

/**
 * L2-regularized SVM: the canonical iterative full-scan workload. Every outer iteration computes {@code t(X) %*% ...}
 * and the inner line search reuses {@code X %*% s}, so it exercises both repeated scans and intermediate reuse.
 */
public class L2SVMTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "L2SVM";
	private static final String TEST_CLASS_DIR = TEST_DIR + L2SVMTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String INPUT_Y = "Y";
	private static final String OUT_OOC = "model";
	private static final String OUT_CP = "model_cp";

	private static final int ROWS = 10000;
	private static final int COLS = 300;
	private static final int BLOCK_SIZE = 1000;
	private static final int MAX_ITER = 20;
	private static final int MAX_II = 10;
	private static final double EPSILON = 1e-8;
	private static final double REG = 1.0;
	private static final double EPS = 1e-6;
	private static final int SEED = 11;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testL2SVMDense() {
		runL2SVMTest(1.0);
	}

	@Test
	public void testL2SVMSparse() {
		runL2SVMTest(0.2);
	}

	private void runL2SVMTest(double sparsity) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			double[][] x = getRandomMatrix(ROWS, COLS, -1, 1, sparsity, SEED);
			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(x));

			// a linearly separable-ish labeling, so the solver actually converges
			Random rand = new Random(SEED);
			double[] plane = new double[COLS];
			for(int c = 0; c < COLS; c++)
				plane[c] = rand.nextGaussian();
			double[][] y = new double[ROWS][1];
			for(int r = 0; r < ROWS; r++) {
				double dot = 0;
				for(int c = 0; c < COLS; c++)
					dot += x[r][c] * plane[c];
				y[r][0] = dot >= 0 ? 1 : -1;
			}
			writeBinaryWithMTD(INPUT_Y, DataConverter.convertToMatrixBlock(y));

			runOOCAndCP(TEST_NAME, buildArgs(output(OUT_OOC)), buildArgs(output(OUT_CP)));

			MatrixBlock modelOOC = DataConverter.readMatrixFromHDFS(output(OUT_OOC), Types.FileFormat.BINARY, COLS, 1,
				BLOCK_SIZE);
			MatrixBlock modelCP = DataConverter.readMatrixFromHDFS(output(OUT_CP), Types.FileFormat.BINARY, COLS, 1,
				BLOCK_SIZE);
			TestUtils.compareMatrices(modelOOC, modelCP, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private String[] buildArgs(String out) {
		return new String[] {input(INPUT_X), input(INPUT_Y), Double.toString(EPSILON), Double.toString(REG),
			Integer.toString(MAX_ITER), Integer.toString(MAX_II), out};
	}
}
