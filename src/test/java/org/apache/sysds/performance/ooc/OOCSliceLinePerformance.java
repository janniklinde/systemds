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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.junit.Assert;

public class OOCSliceLinePerformance {
	private static final long ROWS = 4_000_000;
	private static final int COLUMNS = 100;
	private static final int DOMAIN = 4;
	private static final int MAX_LEVEL = 1;
	private static final long MIN_SUPPORT = 4;
	private static final boolean SEL_FEAT = true;
	private static final int WARMUP_RUNS = 1;
	private static final boolean FORCE_EVICTION = false;
	private static final boolean COMPARE_WITH_CP = false;
	private static final long CACHE_HARD_LIMIT = 160_000_000L;
	private static final long CACHE_EVICTION_LIMIT = 1_000_000L;

	public static void main(String[] args) {
		try {
			for(int i = 0; i < WARMUP_RUNS; i++) {
				testSliceLineOOC(false);
			}
			testSliceLineOOC(true);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static void testSliceLineOOC(boolean log) throws Exception {
		Path dir = Files.createTempDirectory("sliceline-ooc");
		Path expected = dir.resolve("cp.txt");
		Path actual = dir.resolve("ooc.txt");
		try {
			if(COMPARE_WITH_CP)
				run(expected, false, log);
			OOCCacheManager.reset();
			if(FORCE_EVICTION)
				OOCCacheManager.getGlobalCache().updateLimits(CACHE_HARD_LIMIT, CACHE_EVICTION_LIMIT);
			try {
				run(actual, true, log);
			}
			finally {
				OOCCacheManager.reset();
			}
			if(COMPARE_WITH_CP)
				Assert.assertEquals("OOC top-k slices differ from CP execution", Files.readString(expected),
					Files.readString(actual));
		}
		finally {
			try(Stream<Path> files = Files.walk(dir)) {
				files.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
			}
		}
	}

	private static void run(Path output, boolean ooc, boolean log) throws Exception {
		String script = "X = round(rand(rows=" + ROWS + ", cols=" + COLUMNS + ", min=1, max=" + DOMAIN
			+ ", seed=7)); e = X[,1] == " + DOMAIN + "; [TK, TKC, D] = sliceLine(X=X, e=e, k=4, maxL=" + MAX_LEVEL
			+ ", minSup=" + MIN_SUPPORT + ", alpha=0.95, tpEval=FALSE, selFeat=" + (SEL_FEAT ? "TRUE" : "FALSE")
			+ ", verbose=FALSE);"
			+ " out = toString(TKC); write(out, \"" + output.toString().replace("\\", "\\\\") + "\");";
		Path file = Files.createTempFile("sliceline", ".dml");
		try {
			Files.writeString(file, script);
			List<String> args = new ArrayList<>(
				List.of("-f", file.toString(), "-config", "conf/SystemDS-config-defaults.xml", "-exec", "singlenode"));
			if(ooc) {
				args.add("-ooc");
				if(log)
					args.addAll(List.of("-oocStats", "5"));
			}
			if(log)
				args.addAll(List.of("-stats", "5"));
			Assert.assertTrue(DMLScript.executeScript(args.toArray(String[]::new)));
		}
		finally {
			Files.deleteIfExists(file);
		}
	}
}
