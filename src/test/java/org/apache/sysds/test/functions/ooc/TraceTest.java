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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.conf.DMLConfig;
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

public class TraceTest extends AutomatedTestBase {
	private static final String TEST_NAME = "Trace";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + TraceTest.class.getSimpleName() + "/";
	private static final int BLOCK_SIZE = 100;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testTraceAlignedBlocks() throws IOException {
		runTraceTest(400);
	}

	@Test
	public void testTraceTrailingPartialBlock() throws IOException {
		runTraceTest(437);
	}

	private void runTraceTest(int size) throws IOException {
		Types.ExecMode oldPlatform = setExecMode(Types.ExecMode.SINGLE_NODE);
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			setDefaultBlockSizeInConfig(BLOCK_SIZE);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";
			MatrixBlock matrix = MatrixBlock.randOperations(size, size, 0.7, -2, 2, "uniform", 7);
			writeInput("X", matrix, size);

			programArgs = arguments(true, "ooc");
			runTest(true, false, null, -1);
			Assert.assertTrue(heavyHittersContainsString(Instruction.OOC_INST_PREFIX + Opcodes.UAKTRACE));

			programArgs = arguments(false, "target");
			runTest(true, false, null, -1);
			compare("T");
			compare("D");
		}
		finally {
			resetExecMode(oldPlatform);
		}
	}

	private String[] arguments(boolean ooc, String suffix) {
		String[] args = new String[ooc ? 6 : 5];
		int offset = 0;
		args[offset++] = "-stats";
		if(ooc)
			args[offset++] = "-ooc";
		args[offset++] = "-args";
		args[offset++] = input("X");
		args[offset++] = output("T_" + suffix);
		args[offset] = output("D_" + suffix);
		return args;
	}

	private void compare(String name) throws IOException {
		MatrixBlock actual = DataConverter.readMatrixFromHDFS(output(name + "_ooc"), Types.FileFormat.BINARY, 1, 1,
			BLOCK_SIZE);
		MatrixBlock expected = DataConverter.readMatrixFromHDFS(output(name + "_target"), Types.FileFormat.BINARY, 1, 1,
			BLOCK_SIZE);
		TestUtils.compareMatrices(actual, expected, 1e-8);
	}

	private void setDefaultBlockSizeInConfig(int blockSize) throws IOException {
		DMLConfig config = new DMLConfig(getCurConfigFile().getPath());
		config.setTextValue(DMLConfig.DEFAULT_BLOCK_SIZE, String.valueOf(blockSize));
		Files.write(getCurConfigFile().toPath(), config.serializeDMLConfig().getBytes(StandardCharsets.UTF_8));
	}

	private void writeInput(String name, MatrixBlock value, int size) throws IOException {
		MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
		writer.writeMatrixToHDFS(value, input(name), size, size, BLOCK_SIZE, value.getNonZeros());
		HDFSTool.writeMetaDataFile(input(name + ".mtd"), Types.ValueType.FP64,
			new MatrixCharacteristics(size, size, BLOCK_SIZE, value.getNonZeros()), Types.FileFormat.BINARY);
	}
}
