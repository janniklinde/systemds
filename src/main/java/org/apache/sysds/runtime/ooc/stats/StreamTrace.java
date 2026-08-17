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

package org.apache.sysds.runtime.ooc.stats;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class StreamTrace {
	public static final boolean ENABLED = Boolean.getBoolean("sysds.ooc.trace");

	private static final Map<Long, Stream> STREAMS = new ConcurrentHashMap<>();

	public static void register(long streamId) {
		if(!ENABLED)
			return;
		STREAMS.computeIfAbsent(streamId, id -> new Stream(owner()));
	}

	public static void put(long streamId, long bytes) {
		if(ENABLED)
			record(streamId).put(bytes);
	}

	public static void evictWrite(long streamId, long bytes) {
		if(ENABLED)
			record(streamId).evictWrite(bytes);
	}

	public static void dropWarm(long streamId) {
		if(ENABLED)
			record(streamId).drops.increment();
	}

	public static void spillRead(long streamId, long bytes) {
		if(ENABLED)
			record(streamId).spillRead(bytes);
	}

	public static void sourceRead(long streamId, long bytes) {
		if(ENABLED)
			record(streamId).sourceRead(bytes);
	}

	public static void dump() {
		if(!ENABLED || STREAMS.isEmpty())
			return;
		StringBuilder sb = new StringBuilder("OOC stream trace (spill attribution):\n");
		sb.append(String.format("%-8s %9s %9s %9s %9s %9s %9s %9s %9s  %s%n", "stream", "puts", "putGB", "writes",
			"writeGB", "spillRd", "spillGB", "srcRd", "srcGB", "first inserted by"));
		STREAMS.entrySet().stream()
			.sorted(Comparator.comparingLong((Map.Entry<Long, Stream> e) -> -e.getValue().writeBytes.sum()))
			.forEach(e -> {
				Stream s = e.getValue();
				sb.append(String.format("%-8d %9d %9.3f %9d %9.3f %9d %9.3f %9d %9.3f  %s%n", e.getKey(), s.puts.sum(),
					gb(s.putBytes), s.writes.sum(), gb(s.writeBytes), s.spillReads.sum(), gb(s.spillReadBytes),
					s.sourceReads.sum(), gb(s.sourceReadBytes), s.owner));
			});
		System.out.println(sb);
	}

	public static void reset() {
		STREAMS.clear();
	}

	private static Stream record(long streamId) {
		return STREAMS.computeIfAbsent(streamId, id -> new Stream(owner()));
	}

	private static double gb(LongAdder bytes) {
		return bytes.sum() / 1e9;
	}

	private static String owner() {
		StackTraceElement[] stack = Thread.currentThread().getStackTrace();
		StringBuilder sb = new StringBuilder();
		for(StackTraceElement frame : stack) {
			String cls = frame.getClassName();
			if(!cls.startsWith("org.apache.sysds") || cls.startsWith("org.apache.sysds.runtime.ooc.stats") ||
				cls.startsWith("org.apache.sysds.runtime.ooc.cache"))
				continue;
			if(sb.length() > 0)
				sb.append(" <- ");
			sb.append(cls.substring(cls.lastIndexOf('.') + 1)).append('.').append(frame.getMethodName());
			if(sb.length() > 160)
				break;
		}
		return sb.toString();
	}

	private static final class Stream {
		private final String owner;
		private final LongAdder puts = new LongAdder();
		private final LongAdder putBytes = new LongAdder();
		private final LongAdder writes = new LongAdder();
		private final LongAdder writeBytes = new LongAdder();
		private final LongAdder spillReads = new LongAdder();
		private final LongAdder spillReadBytes = new LongAdder();
		private final LongAdder sourceReads = new LongAdder();
		private final LongAdder sourceReadBytes = new LongAdder();
		private final LongAdder drops = new LongAdder();

		private Stream(String owner) {
			this.owner = owner;
		}

		private void put(long bytes) {
			puts.increment();
			putBytes.add(bytes);
		}

		private void evictWrite(long bytes) {
			writes.increment();
			writeBytes.add(bytes);
		}

		private void spillRead(long bytes) {
			spillReads.increment();
			spillReadBytes.add(bytes);
		}

		private void sourceRead(long bytes) {
			sourceReads.increment();
			sourceReadBytes.add(bytes);
		}
	}
}
