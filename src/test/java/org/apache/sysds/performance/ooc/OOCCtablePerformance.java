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
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;

public class OOCCtablePerformance {
	private static final String CONFIG = "src/test/scripts/performance/ooc/SystemDS-config-ooc-performance.xml";
	private static final long ROWS = 200_000_000;
	private static final int COLUMNS = 1000;
	private static final boolean FORCE_EVICTION = true;
	private static final long CACHE_HARD_LIMIT = 160_000_000L;
	private static final long CACHE_EVICTION_LIMIT = 1_000_000L;

	public static void main(String[] args) throws Exception {
		if(FORCE_EVICTION)
			OOCCacheManager.getGlobalCache().updateLimits(CACHE_HARD_LIMIT, CACHE_EVICTION_LIMIT);

		String script = "r = seq(1, " + ROWS + ", 1); " + "c = round(rand(rows=" + ROWS + ", cols=1, min=1, max="
			+ COLUMNS + ", seed=7)); " + "X = table(r, c, 1, " + ROWS + ", " + COLUMNS + ", FALSE); print(sum(X));";
		DMLScript.executeScript(new String[] {"-s", script, "-config", CONFIG, "-exec", "singlenode", "-ooc",
			"-oocStats", "50", "-stats", "50", "-explain"});
	}
}
