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
import org.junit.Ignore;
import org.junit.Test;

/**
 * Gaussian mixture model, the soft-assignment counterpart to k-means. The M-step forms {@code t(resp) %*% X} and, for
 * the full-covariance model, a per-component {@code t(diff*resp) %*% diff}, so it stresses the same tsmm-shaped
 * products as PCA but once per component.
 */
public class GMMTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "GMM";
	private static final String TEST_CLASS_DIR = TEST_DIR + GMMTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String OUT_MU_OOC = "mu";
	private static final String OUT_W_OOC = "w";
	private static final String OUT_MU_CP = "mu_cp";
	private static final String OUT_W_CP = "w_cp";

	private static final int ROWS = 10000;
	private static final int COLS = 50;
	private static final int COMPONENTS = 3;
	private static final int MAX_ITER = 10;
	private static final int BLOCK_SIZE = 1000;
	private static final int SEED = 17;
	private static final double NOISE = 0.1;
	private static final double EPS = 1e-6;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testGMMDiagonal() {
		runGMMTest("VVI");
	}

	@Test
	public void testGMMFullCovariance() {
		runGMMTest("VVV");
	}

	private void runGMMTest(String model) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(generateClustered()));

			runOOCAndCP(TEST_NAME + "-" + model, buildArgs(model, output(OUT_MU_OOC), output(OUT_W_OOC)),
				buildArgs(model, output(OUT_MU_CP), output(OUT_W_CP)));

			MatrixBlock muOOC = DataConverter.readMatrixFromHDFS(output(OUT_MU_OOC), Types.FileFormat.BINARY,
				COMPONENTS, COLS, BLOCK_SIZE);
			MatrixBlock muCP = DataConverter.readMatrixFromHDFS(output(OUT_MU_CP), Types.FileFormat.BINARY, COMPONENTS,
				COLS, BLOCK_SIZE);
			MatrixBlock wOOC = DataConverter.readMatrixFromHDFS(output(OUT_W_OOC), Types.FileFormat.BINARY, 1,
				COMPONENTS, BLOCK_SIZE);
			MatrixBlock wCP = DataConverter.readMatrixFromHDFS(output(OUT_W_CP), Types.FileFormat.BINARY, 1, COMPONENTS,
				BLOCK_SIZE);

			TestUtils.compareMatrices(muOOC, muCP, EPS);
			TestUtils.compareMatrices(wOOC, wCP, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private String[] buildArgs(String model, String outMu, String outWeight) {
		return new String[] {input(INPUT_X), Integer.toString(COMPONENTS), model, Integer.toString(MAX_ITER),
			Integer.toString(SEED), outMu, outWeight};
	}

	private static double[][] generateClustered() {
		Random rand = new Random(SEED);
		double[][] centers = new double[COMPONENTS][COLS];
		for(int k = 0; k < COMPONENTS; k++)
			for(int c = 0; c < COLS; c++)
				centers[k][c] = rand.nextDouble() * 4;

		double[][] data = new double[ROWS][COLS];
		for(int r = 0; r < ROWS; r++) {
			int comp = r % COMPONENTS;
			for(int c = 0; c < COLS; c++)
				data[r][c] = centers[comp][c] + rand.nextGaussian() * NOISE;
		}
		return data;
	}
}
