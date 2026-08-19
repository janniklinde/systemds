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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

public class UnsizedReadTest extends AutomatedTestBase {
	private final static String TEST_NAME = "UnsizedRead";
	private final static String TEST_DIR = "functions/ooc/";
	private final static String TEST_CLASS_DIR = TEST_DIR + UnsizedReadTest.class.getSimpleName() + "/";
	private final static long ULPS = 128;

	private final static String INPUT_NAME = "X";
	private final static String OUTPUT_NAME = "res";

	private final static int rows = 2500;
	private final static int cols = 40;
	private final static int blen = 1000;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME));
	}

	/**
	 * A csv without a metadata file has unknown dimensions until the reader sizes it. That size pass has to happen
	 * while the reblock instruction is wired, because the consumers read the dimensions when they wire their own
	 * plans, not when the data starts flowing.
	 */
	@Test
	public void testUnsizedCsvFeedsDimensionDependentOperators() {
		ExecMode platformOld = setExecMode(ExecMode.SINGLE_NODE);

		try {
			getAndLoadTestConfiguration(TEST_NAME);
			fullDMLScriptName = SCRIPT_DIR + TEST_DIR + TEST_NAME + ".dml";
			writeUnsizedCsv();

			programArgs = new String[] {"-stats", "-ooc", "-args", input(INPUT_NAME), output(OUTPUT_NAME)};
			runTest(true, false, null, -1);

			programArgs = new String[] {"-stats", "-args", input(INPUT_NAME), output(OUTPUT_NAME + "_target")};
			runTest(true, false, null, -1);

			MatrixBlock actual = DataConverter.readMatrixFromHDFS(output(OUTPUT_NAME), Types.FileFormat.BINARY, cols,
				cols, blen);
			MatrixBlock expected = DataConverter.readMatrixFromHDFS(output(OUTPUT_NAME + "_target"),
				Types.FileFormat.BINARY, cols, cols, blen);
			TestUtils.compareMatricesBitAvgDistance(expected, actual, ULPS, ULPS);
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		finally {
			resetExecMode(platformOld);
		}
	}

	private void writeUnsizedCsv() throws Exception {
		double[][] values = getRandomMatrix(rows, cols, 0, 1, 1, 7);
		//deliberately written without a metadata file
		File target = new File(input(INPUT_NAME));
		if(target.getParentFile() != null)
			target.getParentFile().mkdirs();
		try(BufferedWriter writer = new BufferedWriter(new FileWriter(target))) {
			StringBuilder line = new StringBuilder();
			for(double[] row : values) {
				line.setLength(0);
				for(int j = 0; j < cols; j++) {
					if(j > 0)
						line.append(',');
					line.append(row[j]);
				}
				writer.write(line.toString());
				writer.newLine();
			}
		}
	}
}
