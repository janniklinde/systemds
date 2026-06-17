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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Locale;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.performance.TimingUtils;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.utils.Explain.ExplainType;

/**
 * Manual DML benchmark for the shared producer shape:
 *
 * <pre>
 * X = matrix(1, 1, n) + 1
 * prod(X) -> (Y / X, X / (Z + 1))
 * </pre>
 *
 * The default dimensions create a dense logical Y matrix of roughly 64 GiB. Constant folding and
 * algebraic simplification are disabled around each run to keep constant inputs without disabling
 * the broader rewrite pipeline that introduces shared-producer tee boundaries.
 * Override settings with system properties, for example:
 *
 * <pre>
 * -Dsysds.perf.ooc.sharedDml.targetGiB=16
 * -Dsysds.perf.ooc.sharedDml.cols=131072
 * -Dsysds.perf.ooc.sharedDml.measureRuns=3
 * </pre>
 */
public class SharedProducerBroadcastJoinDMLPerformance {
	private static final String PREFIX = "sysds.perf.ooc.sharedDml.";
	private static final double DEFAULT_TARGET_GIB = 64.0;
	private static final long DEFAULT_COLS = 262_144;
	private static final int DEFAULT_WARMUP_RUNS = 1;
	private static final int DEFAULT_MEASURE_RUNS = 1;
	private static final int DEFAULT_STATS_COUNT = 50;
	private static final int DEFAULT_OPT_LEVEL = 2;
	private static final int DEFAULT_BLOCK_SIZE = 250;
	private static final double DEFAULT_SPARSITY = 1.0;
	private static final boolean DEFAULT_DISABLE_CONSTANT_REWRITES = true;
	private static final long GIB = 1024L * 1024L * 1024L;

	public static void main(String[] args) throws Exception {
		Config config = Config.fromSystemProperties();
		DMLScriptState oldState = DMLScriptState.capture();
		Path script = writeScript(config);
		Path configFile = writeConfig(config);

		try {
			System.out.println(config);
			if(config.printScript)
				System.out.println(Files.readString(script, StandardCharsets.UTF_8));

			for(int i = 0; i < config.warmupRuns; i++)
				runScript(script, configFile, config);

			double[] millis = new double[config.measureRuns];
			System.out.println("run,time_ms,streamed_Y_GiB,streamed_Y_GiB_per_sec,rows,cols,blocksize");
			for(int i = 0; i < config.measureRuns; i++)
				millis[i] = report(runScript(script, configFile, config), i + 1, config);

			System.out.println("dml-shared-producer: " + TimingUtils.stats(millis));
		}
		finally {
			OOCCacheManager.reset();
			Files.deleteIfExists(script);
			Files.deleteIfExists(configFile);
			oldState.restore();
		}
	}

	private static double report(long elapsedNanos, int run, Config config) {
		double millis = elapsedNanos / 1_000_000.0;
		double seconds = elapsedNanos / 1_000_000_000.0;
		double gibPerSecond = config.streamedYGiB() / seconds;
		System.out.printf(Locale.US, "%d,%.3f,%.3f,%.3f,%d,%d,%d%n",
			run, millis, config.streamedYGiB(), gibPerSecond, config.rows, config.cols, config.blockSize);
		return millis;
	}

	private static long runScript(Path script, Path configFile, Config config) {
		OOCCacheManager.reset();
		OptimizerState oldOptimizerState = OptimizerState.capture();

		try {
			if(config.disableConstantRewrites) {
				OptimizerUtils.ALLOW_CONSTANT_FOLDING = false;
				OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = false;
			}
			ArrayList<String> args = new ArrayList<>();
			args.add("-f");
			args.add(script.toString());
			args.add("-config");
			args.add(configFile.toString());
			args.add("-exec");
			args.add("singlenode");
			args.add("-ooc");
			args.add("-stats");
			args.add(Integer.toString(config.statsCount));
			args.add("-oocStats");
			args.add(Integer.toString(config.statsCount));
			if(config.explain) {
				args.add("-explain");
				//args.add("hops");
			}

			long start = System.nanoTime();
			if(!DMLScript.executeScript(args.toArray(new String[0])))
				throw new IllegalStateException("DML benchmark script failed");
			return System.nanoTime() - start;
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
		finally {
			oldOptimizerState.restore();
			OOCCacheManager.reset();
		}
	}

	private static Path writeScript(Config config) throws IOException {
		Path script = Files.createTempFile("shared-producer-broadcast-join-", ".dml");
		Files.writeString(script, dml(config), StandardCharsets.UTF_8);
		return script;
	}

	private static Path writeConfig(Config config) throws IOException {
		Path configPath = Files.createTempFile("shared-producer-broadcast-join-", ".xml");
		Files.writeString(configPath,
			String.format(Locale.US,
				"<root>%n  <%s>%d</%s>%n  <%s>%d</%s>%n</root>%n",
				DMLConfig.OPTIMIZATION_LEVEL, config.optLevel, DMLConfig.OPTIMIZATION_LEVEL,
				DMLConfig.DEFAULT_BLOCK_SIZE, config.blockSize, DMLConfig.DEFAULT_BLOCK_SIZE),
			StandardCharsets.UTF_8);
		return configPath;
	}

	private static String dml(Config config) {
		String joinExpression = config.forceJoin ? "X / (Z + 1)" : "X + Z";
		return String.format(Locale.US,
			"A = matrix(1, rows=1, cols=%d);%n" +
				"Y = matrix(2, rows=%d, cols=%d);%n" +
				"Z = matrix(3, rows=1, cols=%d);%n" +
				"%n" +
				"X = A + 1;%n" +
				"%n" +
				"B = Y / X;%n" +
				"J = %s;%n" +
				"%n" +
				"res = as.matrix(sum(B) + sum(J));%n" +
				"print(as.scalar(res));%n",
			config.cols, config.rows, config.cols, config.cols, joinExpression);
	}

	private static final class Config {
		private final long rows;
		private final long cols;
		private final double targetGiB;
		private final double sparsity;
		private final int warmupRuns;
		private final int measureRuns;
		private final int statsCount;
		private final int optLevel;
		private final int blockSize;
		private final boolean explain;
		private final boolean printScript;
		private final boolean forceJoin;
		private final boolean disableConstantRewrites;

		private Config(long rows, long cols, double targetGiB, double sparsity, int warmupRuns,
			int measureRuns, int statsCount, int optLevel, int blockSize, boolean explain, boolean printScript,
			boolean forceJoin, boolean disableConstantRewrites) {
			this.rows = rows;
			this.cols = cols;
			this.targetGiB = targetGiB;
			this.sparsity = sparsity;
			this.warmupRuns = warmupRuns;
			this.measureRuns = measureRuns;
			this.statsCount = statsCount;
			this.optLevel = optLevel;
			this.blockSize = blockSize;
			this.explain = explain;
			this.printScript = printScript;
			this.forceJoin = forceJoin;
			this.disableConstantRewrites = disableConstantRewrites;
		}

		private static Config fromSystemProperties() {
			double targetGiB = getDouble(PREFIX + "targetGiB", DEFAULT_TARGET_GIB);
			long cols = Long.getLong(PREFIX + "cols", DEFAULT_COLS);
			long rows = getLong(PREFIX + "rows", deriveRows(targetGiB, cols));
			double sparsity = getDouble(PREFIX + "sparsity", DEFAULT_SPARSITY);
			int warmupRuns = Integer.getInteger(PREFIX + "warmupRuns", DEFAULT_WARMUP_RUNS);
			int measureRuns = Integer.getInteger(PREFIX + "measureRuns", DEFAULT_MEASURE_RUNS);
			int statsCount = Integer.getInteger(PREFIX + "statsCount", DEFAULT_STATS_COUNT);
			int optLevel = Integer.getInteger(PREFIX + "optLevel", DEFAULT_OPT_LEVEL);
			int blockSize = Integer.getInteger(PREFIX + "blockSize", DEFAULT_BLOCK_SIZE);
			boolean explain = getBoolean(PREFIX + "explain", true);
			boolean printScript = Boolean.getBoolean(PREFIX + "printScript");
			boolean forceJoin = getBoolean(PREFIX + "forceJoin", true);
			boolean disableConstantRewrites = getBoolean(PREFIX + "disableConstantRewrites",
				DEFAULT_DISABLE_CONSTANT_REWRITES);

			if(rows <= 0 || cols <= 0 || targetGiB <= 0 || warmupRuns < 0 || measureRuns <= 0 ||
				statsCount <= 0)
				throw new IllegalArgumentException("Dimensions, targetGiB, run counts, and statsCount must be positive.");
			if(sparsity <= 0 || sparsity > 1)
				throw new IllegalArgumentException("sparsity must be in (0, 1].");
			if(optLevel < 0 || optLevel > 7)
				throw new IllegalArgumentException("optLevel must be in [0, 7].");
			if(blockSize <= 0)
				throw new IllegalArgumentException("blockSize must be positive.");
			return new Config(rows, cols, targetGiB, sparsity, warmupRuns, measureRuns, statsCount,
				optLevel, blockSize, explain, printScript, forceJoin, disableConstantRewrites);
		}

		private static long deriveRows(double targetGiB, long cols) {
			if(targetGiB <= 0 || cols <= 0)
				throw new IllegalArgumentException("targetGiB and cols must be positive.");
			return Math.max(1, (long) Math.ceil(targetGiB * GIB / (cols * (double) Double.BYTES)));
		}

		private double streamedYGiB() {
			return rows * (double) cols * Double.BYTES / GIB;
		}

		@Override
		public String toString() {
			return String.format(Locale.US,
				"Shared producer DML performance: rows=%d, cols=%d, targetGiB=%.3f, " +
					"streamedYGiB=%.3f, sparsity=%.4f, warmups=%d, runs=%d, optLevel=%d, blockSize=%d, forceJoin=%s, " +
					"disableConstantRewrites=%s",
				rows, cols, targetGiB, streamedYGiB(), sparsity, warmupRuns, measureRuns, optLevel,
				blockSize, forceJoin, disableConstantRewrites);
		}
	}

	private static final class OptimizerState {
		private final boolean allowConstantFolding;
		private final boolean allowAlgebraicSimplification;

		private OptimizerState(boolean allowConstantFolding, boolean allowAlgebraicSimplification) {
			this.allowConstantFolding = allowConstantFolding;
			this.allowAlgebraicSimplification = allowAlgebraicSimplification;
		}

		private static OptimizerState capture() {
			return new OptimizerState(OptimizerUtils.ALLOW_CONSTANT_FOLDING,
				OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION);
		}

		private void restore() {
			OptimizerUtils.ALLOW_CONSTANT_FOLDING = allowConstantFolding;
			OptimizerUtils.ALLOW_ALGEBRAIC_SIMPLIFICATION = allowAlgebraicSimplification;
		}
	}

	private static double getDouble(String key, double defaultValue) {
		String value = System.getProperty(key);
		return value == null ? defaultValue : Double.parseDouble(value);
	}

	private static long getLong(String key, long defaultValue) {
		String value = System.getProperty(key);
		return value == null ? defaultValue : Long.parseLong(value);
	}

	private static boolean getBoolean(String key, boolean defaultValue) {
		String value = System.getProperty(key);
		return value == null ? defaultValue : Boolean.parseBoolean(value);
	}

	private static final class DMLScriptState {
		private final boolean statistics;
		private final int statisticsCount;
		private final boolean useOOC;
		private final boolean oocStatistics;
		private final int oocStatisticsCount;
		private final ExplainType explain;

		private DMLScriptState(boolean statistics, int statisticsCount, boolean useOOC,
			boolean oocStatistics, int oocStatisticsCount, ExplainType explain) {
			this.statistics = statistics;
			this.statisticsCount = statisticsCount;
			this.useOOC = useOOC;
			this.oocStatistics = oocStatistics;
			this.oocStatisticsCount = oocStatisticsCount;
			this.explain = explain;
		}

		private static DMLScriptState capture() {
			return new DMLScriptState(DMLScript.STATISTICS, DMLScript.STATISTICS_COUNT,
				DMLScript.USE_OOC, DMLScript.OOC_STATISTICS, DMLScript.OOC_STATISTICS_COUNT,
				DMLScript.EXPLAIN);
		}

		private void restore() {
			DMLScript.STATISTICS = statistics;
			DMLScript.STATISTICS_COUNT = statisticsCount;
			DMLScript.USE_OOC = useOOC;
			DMLScript.OOC_STATISTICS = oocStatistics;
			DMLScript.OOC_STATISTICS_COUNT = oocStatisticsCount;
			DMLScript.EXPLAIN = explain;
		}
	}
}
