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

package org.apache.sysds.performance.ooc;

import org.apache.sysds.api.DMLScript;

public class OOCMatrixMultiplication {
	private static final String CONFIG = "src/test/scripts/performance/ooc/SystemDS-config-ooc-performance.xml";

	public static void main(String[] args) throws Exception {
		int rows = args.length > 0 ? Integer.parseInt(args[0]) : 12000;
		int inner = args.length > 1 ? Integer.parseInt(args[1]) : 8000;
		int cols = args.length > 2 ? Integer.parseInt(args[2]) : 10000;
		double a = args.length > 3 ? Double.parseDouble(args[3]) : 2;
		double b = args.length > 4 ? Double.parseDouble(args[4]) : 3;
		String output = args.length > 5 ? args[5] : "target/OOCMatrixMultiplication";

		String script = "A = rand(rows=" + rows + ", cols=" + inner + ", min=" + a + ", max=" + (a + 1)
			+ ", sparsity=1, seed=7); B = rand(rows=" + inner + ", cols=" + cols + ", min=" + b + ", max=" + (b + 1)
			+ ", sparsity=1, seed=8); C = A %*% B; write(C, \"" + output + "\", format=\"binary\");";
		DMLScript.executeScript(
			new String[] {"-s", script, "-config", CONFIG, "-exec", "singlenode", "-ooc", "-oocStats", "-stats",
				"-explain"});
	}
}
