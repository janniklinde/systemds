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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Asynchronous retry of cache pins whose allowance admission failed. Retries are scheduled with a small
 * bounded backoff on a shared daemon worker instead of polling on the caller thread. Callers must
 * guarantee that the requested entry exists and stays referenced until the pin resolves; a pin that
 * fails because the entry is missing is indistinguishable from failed admission and would retry until
 * cancelled.
 */
final class StorePinRetry {
	private static final long RETRY_BASE_NANOS = TimeUnit.MICROSECONDS.toNanos(100);
	private static final long RETRY_MAX_NANOS = TimeUnit.MILLISECONDS.toNanos(8);
	private static final ScheduledExecutorService RETRY_EXECUTOR =
		Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "ooc-store-pin-retry");
			t.setDaemon(true);
			return t;
		});

	private StorePinRetry() {
	}

	/**
	 * Pins the entry, retrying failed admissions until success or cancellation. The result future
	 * completes with the pinned entry, with null if cancelled, or exceptionally on pin failure.
	 */
	static void pinWithRetry(OOCCache cache, long streamId, long sequenceNumber, MemoryAllowance allowance,
		BooleanSupplier cancelled, OOCFuture<BlockEntry> result) {
		attempt(cache, streamId, sequenceNumber, allowance, cancelled, result, 0);
	}

	private static void attempt(OOCCache cache, long streamId, long sequenceNumber, MemoryAllowance allowance,
		BooleanSupplier cancelled, OOCFuture<BlockEntry> result, int attempt) {
		if(cancelled.getAsBoolean()) {
			result.complete(null);
			return;
		}
		if(allowance.isShutdown()) {
			//a shut-down allowance fails every reservation; retrying would never terminate
			result.completeExceptionally(
				new IllegalStateException("Allowance was shut down while a pin retry was pending."));
			return;
		}
		OOCFuture<BlockEntry> pin;
		try {
			pin = cache.pin(streamId, sequenceNumber, allowance);
		}
		catch(RuntimeException ex) {
			//e.g. cache shutdown between retries; the result future must still complete
			result.completeExceptionally(ex);
			return;
		}
		pin.whenComplete((entry, error) -> {
			if(error != null) {
				result.completeExceptionally(error);
				return;
			}
			if(entry != null) {
				if(cancelled.getAsBoolean()) {
					cache.unpin(entry, allowance);
					result.complete(null);
					return;
				}
				result.complete(entry);
				return;
			}
			if(cancelled.getAsBoolean()) {
				result.complete(null);
				return;
			}
			long delayNanos = Math.min(RETRY_MAX_NANOS, RETRY_BASE_NANOS << Math.min(attempt, 8));
			try {
				RETRY_EXECUTOR.schedule(
					() -> attempt(cache, streamId, sequenceNumber, allowance, cancelled, result, attempt + 1),
					delayNanos, TimeUnit.NANOSECONDS);
			}
			catch(RuntimeException ex) {
				result.completeExceptionally(ex);
			}
		});
	}
}
