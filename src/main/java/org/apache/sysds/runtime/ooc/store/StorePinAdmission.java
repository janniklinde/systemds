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

package org.apache.sysds.runtime.ooc.store;

import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.util.function.BooleanSupplier;

final class StorePinAdmission {
	private StorePinAdmission() {
	}

	static OOCFuture<BlockEntry> pinAdmitted(OOCCache cache, long streamId, long sequenceNumber,
		MemoryAllowance allowance, BooleanSupplier cancelled) {
		if(cancelled.getAsBoolean())
			return OOCFuture.completed(null);
		if(allowance.isShutdown())
			return OOCFuture.failed(
				new IllegalStateException("Allowance was shut down while a pin admission was pending."));

		OOCFuture<BlockEntry> admitted;
		try {
			admitted = cache.pinAdmitted(streamId, sequenceNumber, allowance);
		}
		catch(RuntimeException ex) {
			return OOCFuture.failed(ex);
		}
		OOCFuture<BlockEntry> result = new OOCFuture<>();
		admitted.whenComplete((entry, error) -> {
			if(error != null) {
				result.completeExceptionally(error);
				return;
			}
			if(entry != null && cancelled.getAsBoolean()) {
				cache.unpin(entry, allowance);
				result.complete(null);
				return;
			}
			result.complete(entry);
		});
		return result;
	}
}
