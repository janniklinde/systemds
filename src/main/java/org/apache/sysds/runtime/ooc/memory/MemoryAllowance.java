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

import org.apache.sysds.runtime.ooc.cache.OOCFuture;

public interface MemoryAllowance {
	boolean tryReserve(long bytes);

	default boolean tryReserveTask(long bytes) {
		return tryReserve(bytes);
	}

	void reserveBlocking(long bytes);

	OOCFuture<Void> reserveAsync(long bytes);

	default OOCFuture<Void> reserveTaskAsync(long bytes) {
		return reserveAsync(bytes);
	}

	void release(long bytes);

	long getUsedMemory();

	default void addPassiveMemory(long bytes) {
	}

	default void removePassiveMemory(long bytes) {
	}

	default long getPassiveMemory() {
		return 0;
	}

	default long getActiveMemory() {
		return Math.max(0, getUsedMemory() - getPassiveMemory());
	}

	long getGrantedMemory();

	long getTargetMemory();

	void setTargetMemory(long targetMemory);

	void shutdown();

	boolean isShutdown();

	/**
	 * Allowances that are exempt from the broker admission policy may reserve memory whenever the broker has free
	 * bytes, regardless of strict-mode fair shares. This is reserved for liveness-critical allowances such as the
	 * revive allowance used to fetch parked callbacks back into memory.
	 */
	default boolean isAdmissionExempt() {
		return false;
	}

	default void destroy() {
		shutdown();
	}

	default long getFreeMemory() {
		return Math.max(0, getGrantedMemory() - getUsedMemory());
	}

	default boolean isUnderPressure() {
		return getGrantedMemory() > getTargetMemory();
	}

	default long reclaimUnused() {
		return 0;
	}

	default String debugState() {
		return getClass().getSimpleName();
	}
}
