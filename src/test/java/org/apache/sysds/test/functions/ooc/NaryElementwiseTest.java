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
import org.apache.sysds.common.Types.ExecMode;
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

import java.io.IOException;

public class NaryElementwiseTest extends AutomatedTestBase {
	private static final String TEST_NAME = "NaryElementwise";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + NaryElementwiseTest.class.getSimpleName() + "/";
	private static final String[] INPUT_NAMES = new String[] {"A", "B", "C"};
	private static final double eps = 1e-8;
	private static final int blen = 1000;
	private static final int rows = 2400;
	private static final int cols = 1400;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testNaryElementwiseDense() {
		runNaryElementwiseTest(1.0);
	}

	@Test
	public void testNaryElementwiseSparse() {
		runNaryElementwiseTest(0.05);
	}

	private void runNaryElementwiseTest(double sparsity) {
		ExecMode platformOld = setExecMode(ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
			for(int i = 0; i < INPUT_NAMES.length; i++) {
				MatrixBlock in = DataConverter.convertToMatrixBlock(getRandomMatrix(rows, cols, 1, 7, sparsity, 7 + i));
				writer.writeMatrixToHDFS(in, input(INPUT_NAMES[i]), rows, cols, blen, in.getNonZeros());
				HDFSTool.writeMetaDataFile(input(INPUT_NAMES[i] + ".mtd"), Types.ValueType.FP64,
					new MatrixCharacteristics(rows, cols, blen, in.getNonZeros()), Types.FileFormat.BINARY);
			}

			programArgs = new String[] {"-explain", "-stats", "-ooc", "-args", input(INPUT_NAMES[0]),
				input(INPUT_NAMES[1]), input(INPUT_NAMES[2]), output("R"), output("S")};
			runTest(true, false, null, -1);
			Assert.assertTrue("OOC wasn't used for n-ary multiply",
				heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.NM));
			Assert.assertTrue("OOC wasn't used for n-ary plus",
				heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.NP));

			programArgs = new String[] {"-explain", "-stats", "-args", input(INPUT_NAMES[0]), input(INPUT_NAMES[1]),
				input(INPUT_NAMES[2]), output("R_target"), output("S_target")};
			runTest(true, false, null, -1);

			for(String out : new String[] {"R", "S"}) {
				MatrixBlock actual = DataConverter.readMatrixFromHDFS(output(out), Types.FileFormat.BINARY, rows, cols,
					blen);
				MatrixBlock expected = DataConverter.readMatrixFromHDFS(output(out + "_target"),
					Types.FileFormat.BINARY, rows, cols, blen);
				TestUtils.compareMatrices(expected, actual, eps);
			}
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			resetExecMode(platformOld);
		}
	}
}
