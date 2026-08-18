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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.utils.Statistics;

/**
 * Shared driver for the algorithm-level OOC tests. Every algorithm runs twice on the same input, once with {@code -ooc}
 * and once in plain CP mode, so that the comparison doubles as a correctness check and as a record of which
 * instructions the OOC backend actually covers. The per-run opcode breakdown is appended to a TSV report, which is what
 * turns a suite run into an operator-coverage overview.
 */
public abstract class OOCAlgorithmTestBase extends AutomatedTestBase {
	protected static final String TEST_DIR = "functions/ooc/";

	/** Report collecting the opcode breakdown of every OOC run, for the coverage overview. */
	private static final String REPORT_FILE = System.getProperty("ooc.coverage.report",
		"target/ooc-operator-coverage.tsv");

	/**
	 * Runs the script once out-of-core and once in CP. Both argument arrays are the plain {@code -args} payload; they
	 * differ only in the output paths so that the two results can be compared afterwards.
	 */
	protected void runOOCAndCP(String algorithm, String[] argsOOC, String[] argsCP) {
		programArgs = ArrayUtils.addAll(new String[] {"-stats", "-ooc", "-args"}, argsOOC);
		runTest(true, false, null, -1);
		recordCoverage(algorithm);

		programArgs = ArrayUtils.addAll(new String[] {"-stats", "-args"}, argsCP);
		runTest(true, false, null, -1);
	}

	/** Returns the opcodes of the last run that executed out-of-core, without the {@code ooc_} prefix. */
	protected static List<String> getOOCOpcodes() {
		List<String> ooc = new ArrayList<>();
		for(String opcode : Statistics.getCPHeavyHitterOpCodes())
			if(opcode.startsWith(Instruction.OOC_INST_PREFIX))
				ooc.add(opcode.substring(Instruction.OOC_INST_PREFIX.length()));
		return ooc;
	}

	private static synchronized void recordCoverage(String algorithm) {
		Map<String, Pair<Long, Double>> hitters = Statistics.getHeavyHittersHashMap();
		File report = new File(REPORT_FILE);
		File dir = report.getParentFile();
		if(dir != null && !dir.exists() && !dir.mkdirs())
			return;
		try(FileWriter out = new FileWriter(report, true)) {
			for(Map.Entry<String, Pair<Long, Double>> e : hitters.entrySet()) {
				String opcode = e.getKey();
				boolean ooc = opcode.startsWith(Instruction.OOC_INST_PREFIX);
				out.write(algorithm + '\t' + (ooc ? "OOC" : "CP") + '\t' +
					(ooc ? opcode.substring(Instruction.OOC_INST_PREFIX.length()) : opcode) + '\t' +
					e.getValue().getLeft() + '\t' + e.getValue().getRight() + '\n');
			}
		}
		catch(IOException ex) {
			// the report is diagnostic output, a failure to write it must not fail the test
			System.err.println("Failed to write OOC coverage report: " + ex.getMessage());
		}
	}
}
