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
import org.apache.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Correctness of the OOC reorg operators {@code rev} and {@code rdiag} against CP. The row counts are chosen so that
 * both the block-aligned reversal (pure block-row reindexing) and the unaligned one (every reversed block straddles two
 * output blocks) are covered.
 */
public class ReorgOpsTest extends OOCAlgorithmTestBase {
	private static final String TEST_NAME = "ReorgOps";
	private static final String TEST_CLASS_DIR = TEST_DIR + ReorgOpsTest.class.getSimpleName() + "/";

	private static final String INPUT_X = "X";
	private static final String INPUT_V = "v";
	private static final String OUT_REV_OOC = "R";
	private static final String OUT_DIAG_OOC = "d";
	private static final String OUT_SUM_OOC = "s";
	private static final String OUT_REV_CP = "R_cp";
	private static final String OUT_DIAG_CP = "d_cp";
	private static final String OUT_SUM_CP = "s_cp";

	private static final int COLS = 200;
	private static final int DIAG_ROWS = 2500;
	private static final int BLOCK_SIZE = 1000;
	private static final double EPS = 1e-10;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	/** nrow(X) is a multiple of the block size: rev only flips block row indexes. */
	@Test
	public void testReorgAlignedBlocks() {
		runReorgTest(4000, 1.0);
	}

	/** nrow(X) is not a multiple of the block size: every reversed block spans two output blocks. */
	@Test
	public void testReorgUnalignedBlocks() {
		runReorgTest(3500, 1.0);
	}

	@Test
	public void testReorgUnalignedBlocksSparse() {
		runReorgTest(3500, 0.2);
	}

	/** nrow(X) below the block size: single block, reversed in place. */
	@Test
	public void testReorgSingleBlock() {
		runReorgTest(600, 1.0);
	}

	private void runReorgTest(int rows, double sparsity) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			double[][] x = getRandomMatrix(rows, COLS, -1, 1, sparsity, 17);
			writeBinaryWithMTD(INPUT_X, DataConverter.convertToMatrixBlock(x));
			double[][] v = getRandomMatrix(DIAG_ROWS, 1, -1, 1, 1.0, 23);
			writeBinaryWithMTD(INPUT_V, DataConverter.convertToMatrixBlock(v));

			runOOCAndCP(TEST_NAME,
				new String[] {input(INPUT_X), input(INPUT_V), output(OUT_REV_OOC), output(OUT_DIAG_OOC),
					output(OUT_SUM_OOC)},
				new String[] {input(INPUT_X), input(INPUT_V), output(OUT_REV_CP), output(OUT_DIAG_CP),
					output(OUT_SUM_CP)});

			MatrixBlock revOOC = DataConverter.readMatrixFromHDFS(output(OUT_REV_OOC), Types.FileFormat.BINARY, rows,
				COLS, BLOCK_SIZE);
			MatrixBlock revCP = DataConverter.readMatrixFromHDFS(output(OUT_REV_CP), Types.FileFormat.BINARY, rows,
				COLS, BLOCK_SIZE);
			TestUtils.compareMatrices(revOOC, revCP, EPS);

			//the reversal must actually have happened, not just have been copied through
			for(int r = 0; r < rows; r++)
				Assert.assertEquals("rev mismatch in row " + r, x[rows - 1 - r][0], revOOC.get(r, 0), EPS);

			MatrixBlock diagOOC = DataConverter.readMatrixFromHDFS(output(OUT_DIAG_OOC), Types.FileFormat.BINARY,
				DIAG_ROWS, 1, BLOCK_SIZE);
			MatrixBlock diagCP = DataConverter.readMatrixFromHDFS(output(OUT_DIAG_CP), Types.FileFormat.BINARY,
				DIAG_ROWS, 1, BLOCK_SIZE);
			TestUtils.compareMatrices(diagOOC, diagCP, EPS);
			for(int r = 0; r < DIAG_ROWS; r++)
				Assert.assertEquals("diag round trip mismatch in row " + r, v[r][0], diagOOC.get(r, 0), EPS);

			TestUtils.compareScalars(readDMLScalarFromOutputDir(OUT_SUM_CP).get(new CellIndex(1, 1)),
				readDMLScalarFromOutputDir(OUT_SUM_OOC).get(new CellIndex(1, 1)), 1e-8);
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}
}
