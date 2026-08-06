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

package org.apache.sysds.runtime.ooc.cache.io;

final class BackingLocation {
	static final long NONE = 0L;

	private static final int KIND_SHIFT = 62;
	private static final long KIND_MASK = 3L << KIND_SHIFT;
	private static final long KIND_SPILL = 1L << KIND_SHIFT;

	private static final int PARTITION_SHIFT = 32;
	private static final long PARTITION_MASK = (1L << 30) - 1;
	private static final long OFFSET_MASK = 0xFFFFFFFFL;

	private BackingLocation() {
	}

	static long spill(int partitionId, long offset) {
		if(partitionId < 0 || partitionId > PARTITION_MASK)
			throw new IllegalArgumentException("Spill partition id out of range: " + partitionId);
		if(offset < 0 || offset > OFFSET_MASK)
			throw new IllegalArgumentException("Spill offset out of range: " + offset);
		return KIND_SPILL | ((long) partitionId << PARTITION_SHIFT) | offset;
	}

	static boolean isSpill(long location) {
		return (location & KIND_MASK) == KIND_SPILL;
	}

	static int spillPartition(long location) {
		return (int) ((location >>> PARTITION_SHIFT) & PARTITION_MASK);
	}

	static long spillOffset(long location) {
		return location & OFFSET_MASK;
	}
}
