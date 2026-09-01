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
 * Multinomial naive Bayes: a single pass over X that reduces to per-class sums, i.e. {@code t(Y) %*% X}. The cheapest
 * of the classification workloads and the closest to a pure read-bound baseline.
 */
public class NaiveBayesTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "NaiveBayes";
	private static final String TEST_NAME_SS = "NaiveBayesSufficientStats";
	private static final String TEST_CLASS_DIR = TEST_DIR + NaiveBayesTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String INPUT_Y = "Y";
	private static final String OUT_PRIOR_OOC = "prior";
	private static final String OUT_COND_OOC = "cond";
	private static final String OUT_PRIOR_CP = "prior_cp";
	private static final String OUT_COND_CP = "cond_cp";

	private static final int ROWS = 10000;
	private static final int COLS = 300;
	private static final int CLASSES = 5;
	private static final int BLOCK_SIZE = 1000;
	private static final double LAPLACE = 1.0;
	private static final double EPS = 1e-9;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	/**
	 * The builtin reduces per class through {@code aggregate(target, groups)}, which the OOC groupedagg streams against
	 * the broadcast class vector.
	 */
	@Test
	public void testNaiveBayesBuiltinDense() {
		runNaiveBayesTest(TEST_NAME, 1.0);
	}

	@Test
	public void testNaiveBayesBuiltinSparse() {
		runNaiveBayesTest(TEST_NAME, 0.2);
	}

	@Test
	public void testNaiveBayesSufficientStatsDense() {
		runNaiveBayesTest(TEST_NAME_SS, 1.0);
	}

	@Test
	public void testNaiveBayesSufficientStatsSparse() {
		runNaiveBayesTest(TEST_NAME_SS, 0.2);
	}

	private void runNaiveBayesTest(String script, double sparsity) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + script + ".dml";

			// naive Bayes needs non-negative counts and labels in 1..CLASSES
			double[][] x = getRandomMatrix(ROWS, COLS, 0, 10, sparsity, 3);
			for(double[] row : x)
				for(int c = 0; c < COLS; c++)
					row[c] = Math.floor(row[c]);
			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(x));

			double[][] y = new double[ROWS][1];
			for(int r = 0; r < ROWS; r++)
				y[r][0] = r % CLASSES + 1;
			writeBinaryWithMTD(INPUT_Y, DataConverter.convertToMatrixBlock(y));

			runOOCAndCP(script,
				new String[] {input(INPUT_X), input(INPUT_Y), Double.toString(LAPLACE), output(OUT_PRIOR_OOC),
					output(OUT_COND_OOC)},
				new String[] {input(INPUT_X), input(INPUT_Y), Double.toString(LAPLACE), output(OUT_PRIOR_CP),
					output(OUT_COND_CP)});

			MatrixBlock priorOOC = DataConverter.readMatrixFromHDFS(output(OUT_PRIOR_OOC), Types.FileFormat.BINARY,
				CLASSES, 1, BLOCK_SIZE);
			MatrixBlock priorCP = DataConverter.readMatrixFromHDFS(output(OUT_PRIOR_CP), Types.FileFormat.BINARY,
				CLASSES, 1, BLOCK_SIZE);
			MatrixBlock condOOC = DataConverter.readMatrixFromHDFS(output(OUT_COND_OOC), Types.FileFormat.BINARY,
				CLASSES, COLS, BLOCK_SIZE);
			MatrixBlock condCP = DataConverter.readMatrixFromHDFS(output(OUT_COND_CP), Types.FileFormat.BINARY, CLASSES,
				COLS, BLOCK_SIZE);

			TestUtils.compareMatrices(priorOOC, priorCP, EPS);
			TestUtils.compareMatrices(condOOC, condCP, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}
}
