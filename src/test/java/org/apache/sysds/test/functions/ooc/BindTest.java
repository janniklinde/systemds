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

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.io.MatrixWriter;
import org.apache.sysds.runtime.io.MatrixWriterFactory;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BindTest extends AutomatedTestBase {

	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BindTest.class.getSimpleName() + "/";
	private static final String[] TEST_NAMES = new String[] {"BindTest", "CBind3Test", "RBind3Test"};

	private final static double eps = 1e-8;
	private static final String OUTPUT_NAME = "res";
	private static final int BSIZE = 1000;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		for(String name : TEST_NAMES)
			addTestConfiguration(name, new TestConfiguration(TEST_CLASS_DIR + name + "/", name));
	}

	@Test
	public void testRBindAligned() {
		runBindTest(false, new int[][] {{1000, 300}, {1000, 300}});
	}

	@Test
	public void testRBindMisaligned() {
		runBindTest(false, new int[][] {{700, 300}, {1300, 300}});
	}

	@Test
	public void testRBindMisalignedMultipleColumnBlocks() {
		runBindTest(false, new int[][] {{300, 2300}, {700, 2300}});
	}

	@Test
	public void testRBindEmptyInput() {
		runBindTest(false, new int[][] {{0, 300}, {700, 300}});
	}

	@Test
	public void testNaryRBindMisaligned() {
		runBindTest(false, new int[][] {{700, 300}, {500, 300}, {400, 300}});
	}

	@Test
	public void testNaryRBindMultipleBlocks() {
		runBindTest(false, new int[][] {{1000, 1700}, {1000, 1700}, {1000, 1700}});
	}

	@Test
	public void testNaryRBindEmptyInput() {
		runBindTest(false, new int[][] {{0, 300}, {700, 300}, {500, 300}});
	}

	@Test
	public void testNaryCBindMisaligned() {
		runBindTest(true, new int[][] {{300, 700}, {300, 500}, {300, 400}});
	}

	@Test
	public void testNaryCBindMultipleBlocks() {
		runBindTest(true, new int[][] {{1700, 1000}, {1700, 1000}, {1700, 1000}});
	}

	@Test
	public void testNaryCBindEmptyInput() {
		runBindTest(true, new int[][] {{300, 0}, {300, 700}, {300, 500}});
	}

	private void runBindTest(boolean cbind, int[][] shapes) {
		Types.ExecMode platformOld = setExecMode(Types.ExecMode.SINGLE_NODE);

		try {
			String testName = shapes.length == 2 ? TEST_NAMES[0] : (cbind ? TEST_NAMES[1] : TEST_NAMES[2]);
			getAndLoadTestConfiguration(testName);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + testName + ".dml";

			String[] args = new String[shapes.length + 1];
			MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
			int expectedRows = 0;
			int expectedCols = 0;
			boolean degenerate = false;
			for(int i = 0; i < shapes.length; i++) {
				int rows = shapes[i][0];
				int cols = shapes[i][1];
				String name = "I" + i;
				double[][] data = TestUtils.floor(getRandomMatrix(rows, cols, -1, 1, 1.0, 7 + i));
				writer.writeMatrixToHDFS(DataConverter.convertToMatrixBlock(data), input(name), rows, cols, BSIZE,
					(long) rows * cols);
				HDFSTool.writeMetaDataFile(input(name + ".mtd"), Types.ValueType.FP64,
					new MatrixCharacteristics(rows, cols, BSIZE, (long) rows * cols), Types.FileFormat.BINARY);
				args[i] = input(name);
				expectedRows = cbind ? Math.max(expectedRows, rows) : expectedRows + rows;
				expectedCols = cbind ? expectedCols + cols : Math.max(expectedCols, cols);
				degenerate |= (cbind ? cols : rows) == 0;
			}

			args[shapes.length] = output(OUTPUT_NAME);
			programArgs = withFlags(args, true);
			runTest(true, false, null, -1);
			if(!degenerate)
				Assert.assertTrue("OOC wasn't used for bind",
					heavyHittersContainsString(Instruction.OOC_INST_PREFIX + (shapes.length == 2 ? Opcodes.APPEND
						.toString() : (cbind ? Opcodes.CBIND.toString() : Opcodes.RBIND.toString()))));

			// rerun without ooc flag
			args[shapes.length] = output(OUTPUT_NAME + "_target");
			programArgs = withFlags(args, false);
			runTest(true, false, null, -1);

			// compare results
			MatrixBlock ret1 = DataConverter.readMatrixFromHDFS(output(OUTPUT_NAME), Types.FileFormat.BINARY,
				expectedRows, expectedCols, BSIZE);
			MatrixBlock ret2 = DataConverter.readMatrixFromHDFS(output(OUTPUT_NAME + "_target"),
				Types.FileFormat.BINARY, expectedRows, expectedCols, BSIZE);
			TestUtils.compareMatrices(ret1, ret2, eps);
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private static String[] withFlags(String[] args, boolean ooc) {
		String[] flags = ooc ? new String[] {"-explain", "-stats", "-ooc", "-args"} : new String[] {"-explain",
			"-stats", "-args"};
		String[] all = new String[flags.length + args.length];
		System.arraycopy(flags, 0, all, 0, flags.length);
		System.arraycopy(args, 0, all, flags.length, args.length);
		return all;
	}
}
