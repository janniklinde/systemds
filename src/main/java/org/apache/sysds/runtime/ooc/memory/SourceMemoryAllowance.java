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

package org.apache.sysds.runtime.ooc.memory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SourceMemoryAllowance extends SyncMemoryAllowance {
	private static final long DEFAULT_BUDGET_BYTES = 128L * 1024 * 1024;
	private static final Set<SourceMemoryAllowance> LIVE = ConcurrentHashMap.newKeySet();
	private static final long BUDGET_BYTES = Long.getLong("sysds.ooc.source.budget.bytes",
		Math.min(DEFAULT_BUDGET_BYTES, GlobalMemoryBroker.get().getAllowedMemory() / 3));

	public SourceMemoryAllowance(MemoryBroker broker, long consumptionLimit) {
		super(broker, consumptionLimit);
		LIVE.add(this);
	}

	public static long getSourceShare() {
		return BUDGET_BYTES / Math.max(1, LIVE.size());
	}

	public static String describeState() {
		long used = 0;
		for(SourceMemoryAllowance source : LIVE)
			used += source.getUsedMemory();
		return "sourceBudget[budget=" + BUDGET_BYTES + " sources=" + LIVE.size() + " share=" + getSourceShare()
			+ " used=" + used + ']';
	}

	@Override
	public boolean tryReserve(long bytes) {
		if(bytes > 0 && getUsedMemory() > 0 && getUsedMemory() >= getSourceShare())
			return false;
		return super.tryReserve(bytes);
	}

	@Override
	public void shutdown() {
		super.shutdown();
		if(LIVE.remove(this))
			wakeSources();
	}

	private static void wakeSources() {
		for(SourceMemoryAllowance source : LIVE)
			source.onBrokerMemoryAvailable();
	}
}
