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
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

public interface MaterializedStore<T extends SpillableObject> extends AutoCloseable {
	/**
	 * Publishes a logical index whose resident bytes are already owned by the supplied allowance. Calls may be
	 * concurrent and out of order. Completed publication must contain every index in [0, size) exactly once.
	 */
	void publishPinned(int index, T value, long bytes, MemoryAllowance allowance);

	/**
	 * Publishes a logical index from a managed payload, transferring its detached reservation into the cache
	 * ownership protocol. This is the preferred publication path for memory-managed inputs because settling
	 * the payload makes a double release structurally impossible; the raw overload remains for inputs that
	 * reserve their bytes directly on the materialization allowance.
	 */
	default void publishPinned(int index, ManagedPayload<T> payload) {
		payload.transfer();
		try {
			publishPinned(index, payload.value(), payload.bytes(), payload.owner());
		}
		catch(RuntimeException ex) {
			//the payload was already marked transferred; return the bytes to the producer directly
			if(payload.bytes() > 0)
				payload.owner().release(payload.bytes());
			throw ex;
		}
	}

	/**
	 * Publishes a logical index and keeps the canonical entry pinned for production-time fan-out. The
	 * returned lease serves concurrent live consumers through {@link Lease#retain()} aliases; the LAST
	 * alias close unpins, transferring resident ownership to the cache (possibly deferred). While live,
	 * {@link LiveLease#entry()} exposes the still-pinned canonical entry so a consumer can park a
	 * logical reference to it (e.g. {@code OperatorStateTable.installReference}).
	 */
	LiveLease<T> publishPinnedLive(int index, T value, long bytes, MemoryAllowance allowance);

	default LiveLease<T> publishPinnedLive(int index, ManagedPayload<T> payload) {
		payload.transfer();
		try {
			return publishPinnedLive(index, payload.value(), payload.bytes(), payload.owner());
		}
		catch(RuntimeException ex) {
			//the payload was already marked transferred; return the bytes to the producer directly
			if(payload.bytes() > 0)
				payload.owner().release(payload.bytes());
			throw ex;
		}
	}

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
	 * Opens a demand-driven reader for targeted index access. Unlike the ordered and opportunistic
	 * readers, the index sequence is not known up front but dictated by the caller (e.g. by the arrival
	 * order of a partner stream in broadcasts and indexed-lookup joins). The reader participates in
	 * forgetting through the supplied liveness.
	 */
	IndexedReader<T> openIndexedReader(Liveness liveness, MemoryAllowance allowance);

	/**
	 * Pins an already published item without registering a logical reader consumption. This is for
	 * streamable live replay: the streamable owns logical lifetime, and rmvar-style sealing decides
	 * when forgetting can begin.
	 */
	OOCFuture<Lease<T>> requestPublished(int index, MemoryAllowance allowance);

	/**
	 * Freezes the reader set against new readers and starts/sweeps forgetting. Readers may consume
	 * before sealing; sealing represents rmvar/deletion closing the logical streamable.
	 */
	void sealReaders();

	int size();

	@Override
	void close();

	/**
	 * The liveness part of a reader's access contract: which indices the reader may still access.
	 * Replay order is a separate concern ({@link AccessPattern}); demand-driven readers carry only
	 * liveness.
	 */
	interface Liveness {
		/**
		 * Returns whether this reader may still access the index. False must never become true again.
		 */
		boolean needs(int index);

		void consumed(int index);

		/**
		 * Reserves one future consumption for a demand-driven request. Returning false means no remaining
		 * use can be reserved and the request is a caller error. Reservations gate request admission only;
		 * {@link #needs(int)} must stay true until the matching consumption, so an index with in-flight
		 * requests cannot be forgotten. The default delegates to {@code needs} for iteration-driven
		 * patterns that control their own request rate.
		 */
		default boolean reserve(int index) {
			return needs(index);
		}

		/**
		 * Returns a reservation taken by {@link #reserve(int)} that will not result in a consumption.
		 */
		default void unreserve(int index) {
		}
	}

	interface AccessPattern extends Liveness {
		boolean hasNext();

		int next();
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

	interface IndexedReader<T extends SpillableObject> extends AutoCloseable {
		/**
		 * Requests a targeted index. The returned future completes with a lease once the item is loaded
		 * and admitted under the reader allowance; admission failures are retried asynchronously, never
		 * by blocking or polling on the caller thread. The future completes with null only if the reader
		 * is closed before admission. Requesting an index whose liveness is exhausted is an error.
		 */
		OOCFuture<Lease<T>> request(int index);

		/**
		 * Returns a lease if the index is resident and admissible right now; null otherwise. Never blocks
		 * and never schedules I/O.
		 */
		Lease<T> requestIfLive(int index);

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

	interface LiveLease<T extends SpillableObject> extends Lease<T> {
		/**
		 * The still-pinned canonical cache entry of a live publication. Valid only while this lease is
		 * open; the pin it relies on is dropped when the last alias closes.
		 */
		BlockEntry entry();

		@Override
		LiveLease<T> retain();
	}
}
