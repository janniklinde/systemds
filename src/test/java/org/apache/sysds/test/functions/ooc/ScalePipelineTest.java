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
 * Standardization, the preprocessing baseline. It is dominated by column aggregates and one elementwise rewrite of the
 * data, so the gap between its runtime and raw disk throughput is engine overhead rather than algorithmic cost.
 */
public class ScalePipelineTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "ScalePipeline";
	private static final String TEST_CLASS_DIR = TEST_DIR + ScalePipelineTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String OUT_MEAN_OOC = "cmean";
	private static final String OUT_SD_OOC = "csd";
	private static final String OUT_MEAN_CP = "cmean_cp";
	private static final String OUT_SD_CP = "csd_cp";

	private static final int ROWS = 20000;
	private static final int COLS = 400;
	private static final int BLOCK_SIZE = 1000;
	private static final int SEED = 37;
	private static final double EPS = 1e-9;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testScaleDense() {
		runScaleTest(1.0);
	}

	@Test
	public void testScaleSparse() {
		runScaleTest(0.2);
	}

	private void runScaleTest(double sparsity) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			double[][] x = getRandomMatrix(ROWS, COLS, -5, 5, sparsity, SEED);
			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(x));

			runOOCAndCP(TEST_NAME, new String[] {input(INPUT_X), output(OUT_MEAN_OOC), output(OUT_SD_OOC)},
				new String[] {input(INPUT_X), output(OUT_MEAN_CP), output(OUT_SD_CP)});

			MatrixBlock meanOOC = DataConverter.readMatrixFromHDFS(output(OUT_MEAN_OOC), Types.FileFormat.BINARY, 1,
				COLS, BLOCK_SIZE);
			MatrixBlock meanCP = DataConverter.readMatrixFromHDFS(output(OUT_MEAN_CP), Types.FileFormat.BINARY, 1, COLS,
				BLOCK_SIZE);
			MatrixBlock sdOOC = DataConverter.readMatrixFromHDFS(output(OUT_SD_OOC), Types.FileFormat.BINARY, 1, COLS,
				BLOCK_SIZE);
			MatrixBlock sdCP = DataConverter.readMatrixFromHDFS(output(OUT_SD_CP), Types.FileFormat.BINARY, 1, COLS,
				BLOCK_SIZE);

			TestUtils.compareMatrices(meanOOC, meanCP, EPS);
			TestUtils.compareMatrices(sdOOC, sdCP, EPS);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}
}
