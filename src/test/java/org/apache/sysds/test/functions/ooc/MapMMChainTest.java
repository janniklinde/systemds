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
import java.util.Arrays;
import java.util.Collection;

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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@net.jcip.annotations.NotThreadSafe
public class MapMMChainTest extends AutomatedTestBase {
	private static final String TEST_NAME = "MapMMChain";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + MapMMChainTest.class.getSimpleName() + "/";
	private static final int ROWS = 1201;
	private static final int COLS = 1103;
	private static final int BLOCKSIZE = 1000;
	private final int _type;

	public MapMMChainTest(int type) {
		_type = type;
	}

	@Parameterized.Parameters(name = "type={0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {{1}, {2}, {3}});
	}

	@Before
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {"R"}));
	}

	@Test
	public void testMapMMChain() {
		Types.ExecMode platform = setExecMode(Types.ExecMode.SINGLE_NODE);
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";
			writeInput("X", getRandomMatrix(ROWS, COLS, 0, 1, 0.7, 7), ROWS, COLS);
			writeInput("v", getRandomMatrix(COLS, 1, 0, 1, 1, 13), COLS, 1);
			writeInput("w", getRandomMatrix(ROWS, 1, 0, 1, 1, 17), ROWS, 1);

			programArgs = arguments(true, output("R"));
			runTest(true, false, null, -1);
			Assert.assertTrue("OOC wasn't used for mapmmchain",
				heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.MAPMMCHAIN));

			programArgs = arguments(false, output("Expected"));
			runTest(true, false, null, -1);

			MatrixBlock actual = DataConverter.readMatrixFromHDFS(output("R"), Types.FileFormat.BINARY, COLS, 1,
				BLOCKSIZE);
			MatrixBlock expected = DataConverter.readMatrixFromHDFS(output("Expected"), Types.FileFormat.BINARY, COLS,
				1, BLOCKSIZE);
			TestUtils.compareMatrices(expected, actual, 1e-6);
		}
		catch(IOException error) {
			throw new RuntimeException(error);
		}
		finally {
			resetExecMode(platform);
		}
	}

	private String[] arguments(boolean ooc, String result) {
		String[] base = {"-stats", "-args", input("X"), input("v"), input("w"), result, Integer.toString(_type)};
		if(!ooc)
			return base;
		String[] args = new String[base.length + 1];
		args[0] = "-ooc";
		System.arraycopy(base, 0, args, 1, base.length);
		return args;
	}

	private void writeInput(String name, double[][] values, long rows, long cols) throws IOException {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
		writer.writeMatrixToHDFS(DataConverter.convertToMatrixBlock(values), input(name), rows, cols, BLOCKSIZE,
			rows * cols);
		HDFSTool.writeMetaDataFile(input(name + ".mtd"), Types.ValueType.FP64,
			new MatrixCharacteristics(rows, cols, BLOCKSIZE, rows * cols), Types.FileFormat.BINARY);
	}
}
