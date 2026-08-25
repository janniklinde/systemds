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
import org.apache.sysds.runtime.controlprogram.ParForProgramBlock;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;

import org.junit.Test;

/**
 * Random forest over recoded/binned features. Beyond what {@link DecisionTreeTest} covers, this trains the trees in a
 * parfor, so several workers stream the very same shared input concurrently. That is the case where each worker tees
 * the input matrix object: the tees have to agree on one reusable handle, because an input that only ever existed as
 * a stream has no file to fall back on.
 */
public class RandomForestTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "RandomForest";
	private static final String TEST_CLASS_DIR = TEST_DIR + RandomForestTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String INPUT_Y = "y";
	private static final String OUT_OOC = "M";
	private static final String OUT_CP = "M_cp";

	private static final int ROWS = 6000;
	private static final int COLS = 20;
	private static final int BINS = 5;
	//four trees, i.e. four concurrent workers over the shared input. Eight makes the CP reference itself fail
	//with an IndexOutOfBoundsException in a nested-parfor matmul, which is a separate defect from anything OOC.
	private static final int NUM_TREES = 4;
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

	/** Full data and features per tree, so the forest is deterministic and every tree sees the whole input. */
	@Test
	public void testRandomForestFullSample() {
		runRandomForest(1.0, 1.0);
	}

	/** Row and feature sampling, which adds a removeEmpty over the shared input on top of the concurrent tees. */
	@Test
	public void testRandomForestSubSample() {
		runRandomForest(0.5, 0.5);
	}

	private void runRandomForest(double sampleFrac, double featureFrac) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);
		//no retries: a worker that fails once and succeeds on the second attempt would otherwise hide a race
		//between the workers behind a correct result, and only show up as a slower run
		int retrysOld = ParForProgramBlock.MAX_RETRYS_ON_ERROR;
		ParForProgramBlock.MAX_RETRYS_ON_ERROR = 0;

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

			runOOCAndCP(TEST_NAME, buildArgs(sampleFrac, featureFrac, output(OUT_OOC)),
				buildArgs(sampleFrac, featureFrac, output(OUT_CP)));

			//OOC streams cannot be written as text, so the forest is exchanged in binary at its known width:
			//one feature-sampling indicator per input column, then the linearized tree per row
			int cols = COLS + 2 * ((1 << MAX_DEPTH) - 1);
			MatrixBlock ooc = DataConverter.readMatrixFromHDFS(output(OUT_OOC), Types.FileFormat.BINARY, NUM_TREES,
				cols, ConfigurationManager.getBlocksize());
			MatrixBlock cp = DataConverter.readMatrixFromHDFS(output(OUT_CP), Types.FileFormat.BINARY, NUM_TREES, cols,
				ConfigurationManager.getBlocksize());
			TestUtils.compareMatrices(ooc, cp, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			ParForProgramBlock.MAX_RETRYS_ON_ERROR = retrysOld;
			resetExecMode(platformOld);
		}
	}

	private String[] buildArgs(double sampleFrac, double featureFrac, String out) {
		return new String[] {input(INPUT_X), input(INPUT_Y), Integer.toString(NUM_TREES), Double.toString(sampleFrac),
			Double.toString(featureFrac), Integer.toString(MAX_DEPTH), Integer.toString(MIN_LEAF),
			Integer.toString(MIN_SPLIT), Integer.toString(SEED), out};
	}
}
