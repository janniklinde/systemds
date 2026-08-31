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

import net.jcip.annotations.NotThreadSafe;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

@NotThreadSafe
public class CorrelatedRowFusionTest extends AutomatedTestBase {
	private static final String TEST_NAME = "CorrelatedRowFusion";
	private static final String TEST_DIR = "functions/ooc/";
	private static final String TEST_CLASS_DIR = TEST_DIR + CorrelatedRowFusionTest.class.getSimpleName() + "/";
	private static final int ROWS = 1201;
	private static final int COLS = 1103;
	private static final int RANK = 7;
	private static final int BLOCK_SIZE = 1000;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	@Test
	public void testKMeansAndGNMFDiamonds() {
		Types.ExecMode platform = setExecMode(Types.ExecMode.SINGLE_NODE);
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";

			programArgs = arguments(true, "ooc");
			runTest(true, false, null, -1);
			programArgs = arguments(false, "cp");
			runTest(true, false, null, -1);

			compare("C", RANK, COLS);
			compare("W", ROWS, RANK);
			compare("H", RANK, COLS);
		}
		catch(IOException error) {
			throw new RuntimeException(error);
		}
		finally {
			resetExecMode(platform);
		}
	}

	private String[] arguments(boolean ooc, String suffix) {
		String[] base = {"-stats", "-args", output("C_" + suffix), output("W_" + suffix), output("H_" + suffix)};
		if(!ooc)
			return base;
		String[] args = new String[base.length + 1];
		args[0] = "-ooc";
		System.arraycopy(base, 0, args, 1, base.length);
		return args;
	}

	private void compare(String name, long rows, long cols) throws IOException {
		MatrixBlock actual = DataConverter.readMatrixFromHDFS(output(name + "_ooc"), Types.FileFormat.BINARY, rows,
			cols, BLOCK_SIZE);
		MatrixBlock expected = DataConverter.readMatrixFromHDFS(output(name + "_cp"), Types.FileFormat.BINARY, rows,
			cols, BLOCK_SIZE);
		TestUtils.compareMatrices(expected, actual, 1e-8);
	}
}
