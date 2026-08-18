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
 * Mini-batch SGD logistic regression. Stands in for the neural-network training loops: the data is consumed through
 * row-range indexing inside a nested loop rather than through whole-matrix scans, which is the access pattern none of
 * the batch solvers produce.
 */
public class MiniBatchSGDTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "MiniBatchSGD";
	private static final String TEST_CLASS_DIR = TEST_DIR + MiniBatchSGDTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String INPUT_Y = "y";
	private static final String OUT_OOC = "w";
	private static final String OUT_CP = "w_cp";

	private static final int ROWS = 8000;
	private static final int COLS = 100;
	private static final int EPOCHS = 2;
	private static final int BATCH = 1000;
	private static final int BLOCK_SIZE = 1000;
	private static final int SEED = 29;
	private static final double LEARNING_RATE = 0.1;
	private static final double EPS = 1e-8;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testMiniBatchAlignedBatches() {
		runSGDTest(BATCH);
	}

	@Test
	public void testMiniBatchUnalignedBatches() {
		// batches that straddle block boundaries, so a row range never matches one cached block
		runSGDTest(BATCH - 137);
	}

	private void runSGDTest(int batch) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			double[][] x = getRandomMatrix(ROWS, COLS, -1, 1, 1.0, SEED);
			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(x));

			Random rand = new Random(SEED);
			double[] plane = new double[COLS];
			for(int c = 0; c < COLS; c++)
				plane[c] = rand.nextGaussian();
			double[][] y = new double[ROWS][1];
			for(int r = 0; r < ROWS; r++) {
				double dot = 0;
				for(int c = 0; c < COLS; c++)
					dot += x[r][c] * plane[c];
				y[r][0] = dot >= 0 ? 1 : 0;
			}
			writeBinaryWithMTD(INPUT_Y, DataConverter.convertToMatrixBlock(y));

			runOOCAndCP(TEST_NAME, buildArgs(batch, output(OUT_OOC)), buildArgs(batch, output(OUT_CP)));

			MatrixBlock wOOC = DataConverter.readMatrixFromHDFS(output(OUT_OOC), Types.FileFormat.BINARY, COLS, 1,
				BLOCK_SIZE);
			MatrixBlock wCP = DataConverter.readMatrixFromHDFS(output(OUT_CP), Types.FileFormat.BINARY, COLS, 1,
				BLOCK_SIZE);
			TestUtils.compareMatrices(wOOC, wCP, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private String[] buildArgs(int batch, String out) {
		return new String[] {input(INPUT_X), input(INPUT_Y), Integer.toString(EPOCHS), Integer.toString(batch),
			Double.toString(LEARNING_RATE), out};
	}
}
