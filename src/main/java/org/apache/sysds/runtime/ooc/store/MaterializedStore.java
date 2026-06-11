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

import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

public interface MaterializedStore<T extends SpillableObject> extends AutoCloseable {
	/**
	 * Publishes a logical index whose resident bytes are already owned by the supplied allowance. Calls may be
	 * concurrent and out of order. Completed publication must contain every index in [0, size) exactly once.
	 */
	void publishPinned(int index, T value, long bytes, MemoryAllowance allowance);

	/**
	 * Publishes an already grouped sealed pack. The supplied bytes are owned by the allowance as one physical unit,
	 * while every item remains addressable through its logical index.
	 */
	void publishPackPinned(int[] indices, T[] values, long[] bytes, int off, int len, MemoryAllowance allowance);

	default void publishPackPinned(int[] indices, T[] values, long[] bytes, MemoryAllowance allowance) {
		publishPackPinned(indices, values, bytes, 0, indices.length, allowance);
	}

	/**
	 * Completes publication after all publisher tasks have joined.
	 */
	void complete();

	Reader<T> openReader(AccessPattern pattern, MemoryAllowance allowance, int maxPrefetch);

	PackReader<T> openOpportunisticReader(AccessPattern pattern, MemoryAllowance allowance, int maxPrefetch);

	/**
	 * Freezes the reader set. Offline access and reclamation start only after this call.
	 */
	void sealReaders();

	int size();

	@Override
	void close();

	interface AccessPattern {
		boolean hasNext();

		int next();

		/**
		 * Returns whether this reader may still access the index. False must never become true again.
		 */
		boolean needs(int index);

		void consumed(int index);
	}

	interface Reader<T extends SpillableObject> extends AutoCloseable {
		boolean hasNext();

		/**
		 * Returns the next ordered item, blocking while cache loading or allowance admission cannot progress.
		 */
		Lease<T> next() throws InterruptedException;

		@Override
		void close();
	}

	interface PackReader<T extends SpillableObject> extends AutoCloseable {
		boolean hasNext();

		/**
		 * Returns the next completed physical pack, independent of request order.
		 */
		PackLease<T> nextPack() throws InterruptedException;

		@Override
		void close();
	}

	interface PackLease<T extends SpillableObject> extends AutoCloseable {
		int size();

		int index(int slot);

		T value(int slot);

		@Override
		void close();
	}

	interface Lease<T extends SpillableObject> extends AutoCloseable {
		int index();

		T value();

		Lease<T> retain();

		@Override
		void close();
	}
}
