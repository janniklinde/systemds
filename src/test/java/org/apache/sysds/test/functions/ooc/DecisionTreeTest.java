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
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;

import org.junit.Test;

/**
 * Decision tree over recoded/binned features. The hardest of these workloads for a streaming engine: split finding is
 * data dependent, the node queue drives control flow from scalar results, and each level re-scans a subset of X.
 */
public class DecisionTreeTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "DecisionTree";
	private static final String TEST_CLASS_DIR = TEST_DIR + DecisionTreeTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String INPUT_Y = "y";
	private static final String OUT_OOC = "M";
	private static final String OUT_CP = "M_cp";

	private static final int ROWS = 6000;
	private static final int COLS = 20;
	private static final int BINS = 5;
	private static final int MAX_DEPTH = 5;
	private static final int MIN_LEAF = 20;
	private static final int MIN_SPLIT = 50;
	private static final int SEED = 23;
	private static final double EPS = 1e-9;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	/**
	 * Exercises the store lifetime rules hardest of all these workloads: the node queue is a list, so the same matrix
	 * objects leave and re-enter the symbol table across iterations while their materialized stores must stay alive.
	 */
	@Test
	public void testDecisionTree() {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			Random rand = new Random(SEED);
			double[][] x = new double[ROWS][COLS];
			double[][] y = new double[ROWS][1];
			for(int r = 0; r < ROWS; r++) {
				for(int c = 0; c < COLS; c++)
					x[r][c] = rand.nextInt(BINS) + 1;
				// a label that actually depends on the first two features, so splits are informative
				y[r][0] = (x[r][0] + x[r][1] > BINS ? 1 : 0) + 1;
			}
			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(x));
			writeBinaryWithMTD(INPUT_Y, DataConverter.convertToMatrixBlock(y));

			runOOCAndCP(TEST_NAME, buildArgs(output(OUT_OOC)), buildArgs(output(OUT_CP)));

			//OOC streams cannot be written as text, so the tree is exchanged in binary at its known linearized width
			int cols = 2 * ((1 << MAX_DEPTH) - 1);
			MatrixBlock ooc = DataConverter.readMatrixFromHDFS(output(OUT_OOC), Types.FileFormat.BINARY, 1, cols,
				ConfigurationManager.getBlocksize());
			MatrixBlock cp = DataConverter.readMatrixFromHDFS(output(OUT_CP), Types.FileFormat.BINARY, 1, cols,
				ConfigurationManager.getBlocksize());
			TestUtils.compareMatrices(ooc, cp, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private String[] buildArgs(String out) {
		return new String[] {input(INPUT_X), input(INPUT_Y), Integer.toString(MAX_DEPTH), Integer.toString(MIN_LEAF),
			Integer.toString(MIN_SPLIT), Integer.toString(SEED), out};
	}
}
