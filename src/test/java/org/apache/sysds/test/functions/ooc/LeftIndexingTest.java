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

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.io.MatrixWriter;
import org.apache.sysds.runtime.io.MatrixWriterFactory;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LeftIndexingTest extends AutomatedTestBase {
	private static final String TEST_NAME = "LeftIndexing";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + LeftIndexingTest.class.getSimpleName() + "/";
	private static final int ROWS = 1200;
	private static final int COLS = 1200;
	private static final int BLOCKSIZE = 1000;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testMatrixAndScalarLeftIndexing() {
		Types.ExecMode platform = setExecMode(Types.ExecMode.SINGLE_NODE);
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";
			MatrixBlock left = MatrixBlock.randOperations(ROWS, COLS, 0.2, -1, 1, "uniform", 7);
			MatrixBlock right = MatrixBlock.randOperations(600, 700, 0.15, -2, 2, "uniform", 8);
			writeInput(left, "A");
			writeInput(right, "B");

			OOCCacheManager.getGlobalCache().updateLimits(4_000_000, 100_000);
			programArgs = new String[] {"-stats", "-ooc", "-args", input("A"), input("B"), output("result")};
			runTest(true, false, null, -1);
			Assert.assertTrue(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.LEFT_INDEX));
			OOCCacheManager.reset();

			programArgs = new String[] {"-stats", "-args", input("A"), input("B"), output("expected")};
			runTest(true, false, null, -1);
			MatrixBlock actual = DataConverter.readMatrixFromHDFS(output("result"), Types.FileFormat.BINARY, ROWS, COLS,
				BLOCKSIZE);
			MatrixBlock expected = DataConverter.readMatrixFromHDFS(output("expected"), Types.FileFormat.BINARY, ROWS,
				COLS, BLOCKSIZE);
			TestUtils.compareMatrices(expected, actual, 1e-10);
		}
		catch(IOException error) {
			throw new RuntimeException(error);
		}
		finally {
			OOCCacheManager.reset();
			resetExecMode(platform);
		}
	}

	private void writeInput(MatrixBlock block, String name) throws IOException {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
		writer.writeMatrixToHDFS(block, input(name), block.getNumRows(), block.getNumColumns(), BLOCKSIZE,
			block.getNonZeros());
		HDFSTool.writeMetaDataFile(input(name + ".mtd"), Types.ValueType.FP64,
			new MatrixCharacteristics(block.getNumRows(), block.getNumColumns(), BLOCKSIZE, block.getNonZeros()),
			Types.FileFormat.BINARY);
	}
}
