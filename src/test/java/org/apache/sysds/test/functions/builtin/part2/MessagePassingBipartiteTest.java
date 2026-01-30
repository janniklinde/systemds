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

package org.apache.sysds.test.functions.builtin.part2;

import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.runtime.io.MatrixWriter;
import org.apache.sysds.runtime.io.MatrixWriterFactory;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.runtime.util.HDFSTool;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

import java.io.File;

public class MessagePassingBipartiteTest extends AutomatedTestBase {
	private static final String TEST_NAME = "message_passing_bipartite";
	private static final String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + MessagePassingBipartiteTest.class.getSimpleName() + "/";
	private static final int nV = 500000;
	private static final int nC = 700000;
	private static final int nE = 2500000;
	private static final int d = 32;
	private static final double sparsity = 1.0;

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{}));
	}

	@Override
	protected File getConfigTemplateFile() {
		return new File(CONFIG_DIR, "SystemDS-config-single.xml");
	}

	@Test
	public void testMessagePassingBipartite() throws Exception {
		ExecMode old = setExecMode(ExecMode.SINGLE_NODE);
		try {
			getAndLoadTestConfiguration(TEST_NAME);
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			System.out.println("FLOATING_POINT_PRECISION_CONFIG_FILE=" + new DMLConfig(getCurConfigFile().getPath())
				.getTextValue(DMLConfig.FLOATING_POINT_PRECISION));

			double[][] W_v_agg = getRandomMatrix(d, d, -1, 1, sparsity, 1);
			double[][] W_v_in = getRandomMatrix(d, d, -1, 1, sparsity, 2);
			double[][] W_c_agg = getRandomMatrix(d, d, -1, 1, sparsity, 3);
			double[][] W_c_in = getRandomMatrix(d, d, -1, 1, sparsity, 4);
			double[][] b_v = getRandomMatrix(1, d, -1, 1, sparsity, 3);
			double[][] b_c = getRandomMatrix(1, d, -1, 1, sparsity, 4);
			double[][] W_v_vccv = getRandomMatrix(d, 2*d, -1, 1, sparsity, 5);
			double[][] W_c_vccv = getRandomMatrix(d, 2*d, -1, 1, sparsity, 6);
			double[][] W_e_vccv = getRandomMatrix(1, 2*d, -1, 1, sparsity, 7);
			double[][] b_vccv = getRandomMatrix(1, 2*d, -1, 1, sparsity, 8);
			double[][] v = getRandomMatrix(nV, d, -1, 1, sparsity, 9);
			double[][] c = getRandomMatrix(nC, d, -1, 1, sparsity, 10);
			double[][] e = getRandomMatrix(nE, 1, -1, 1, sparsity, 11);
			double[][] Ex2 = new double[nE][2];
			int idx = 0;
			for(int ci = 1; ci <= nC && idx < nE; ci++) {
				for(int vi = 1; vi <= nV && idx < nE; vi++) {
					Ex2[idx][0] = ci;
					Ex2[idx][1] = vi;
					idx++;
				}
			}

			MatrixWriter writer = MatrixWriterFactory.createMatrixWriter(Types.FileFormat.BINARY);
			writeMatrix(writer, "W_v_agg", W_v_agg);
			W_v_agg = null;
			writeMatrix(writer, "W_v_in", W_v_in);
			W_v_in = null;
			writeMatrix(writer, "W_c_agg", W_c_agg);
			W_c_agg = null;
			writeMatrix(writer, "W_c_in", W_c_in);
			W_c_in = null;
			writeMatrix(writer, "b_v", b_v);
			b_v = null;
			writeMatrix(writer, "b_c", b_c);
			b_c = null;
			writeMatrix(writer, "W_v_vccv", W_v_vccv);
			W_v_vccv = null;
			writeMatrix(writer, "W_c_vccv", W_c_vccv);
			W_c_vccv = null;
			writeMatrix(writer, "W_e_vccv", W_e_vccv);
			W_e_vccv = null;
			writeMatrix(writer, "b_vccv", b_vccv);
			b_vccv = null;
			writeMatrix(writer, "v", v);
			v = null;
			writeMatrix(writer, "c", c);
			c = null;
			writeMatrix(writer, "e", e);
			e = null;
			writeMatrix(writer, "Ex2", Ex2);
			Ex2 = null;

			programArgs = new String[]{"-explain", "-exec", "singlenode", "-args",
				input("W_v_agg"), input("W_v_in"), input("W_c_agg"), input("W_c_in"), input("b_v"), input("b_c"),
				input("W_v_vccv"), input("W_c_vccv"), input("W_e_vccv"), input("b_vccv"),
				input("v"), input("c"), input("e"), input("Ex2")};
			runTest(true, true, org.apache.sysds.runtime.DMLRuntimeException.class, "expected FP32 dense block", -1);
		}
		finally {
			resetExecMode(old);
		}
	}

	private void writeMatrix(MatrixWriter writer, String name, double[][] data) throws Exception {
		MatrixBlock mb = DataConverter.convertToMatrixBlock(data);
		writer.writeMatrixToHDFS(mb, input(name), data.length, data[0].length, 1000, mb.getNonZeros());
		HDFSTool.writeMetaDataFile(input(name + ".mtd"), Types.ValueType.FP32,
			new MatrixCharacteristics(data.length, data[0].length, 1000, mb.getNonZeros()), Types.FileFormat.BINARY);
	}
}
