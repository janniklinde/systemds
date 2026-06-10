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

package org.apache.sysds.runtime.ooc.cache;

import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.util.concurrent.CompletableFuture;

public interface OOCCache {
	default OOCFuture<BlockEntry> pin(BlockKey key, MemoryAllowance allowance) {
		return pin(key.getStreamId(), key.getSequenceNumber(), allowance);
	}

	/**
	 * Adds a new resident entry whose bytes are already owned by the supplied allowance. This creates cache metadata and
	 * a physical pin, but does not transfer ownership to the cache. Ownership can later move only via pin/unpin.
	 */
	default BlockEntry putPinned(BlockKey key, Object data, long size, MemoryAllowance allowance) {
		return putPinned(key.getStreamId(), key.getSequenceNumber(), data, size, allowance);
	}

	/**
	 * Adds a new resident entry whose bytes are already owned by the supplied allowance. This creates cache metadata and
	 * a physical pin, but does not transfer ownership to the cache. Ownership can later move only via pin/unpin.
	 */
	BlockEntry putPinned(long sId, long tId, Object data, long size, MemoryAllowance allowance);

	/**
	 * Pins an item backed by an allowance. A successful pin transfers resident-memory ownership from the cache to the
	 * owner of the allowance and guarantees data availability through the returned entry. While pinned, the entry remains
	 * visible to cache metadata but its bytes are not counted as cache-owned memory.
	 *
	 * Implementations must reserve the required bytes from the allowance before making data available. The returned
	 * future itself must not be null; if reservation or loading fails in a non-exceptional way, the future completes with
	 * null.
	 *
	 * @param sId
	 * @param tId
	 * @param allowance
	 * @return a non-null future of the pinned block entry; the future result is null if the required memory could not be reserved
	 */
	OOCFuture<BlockEntry> pin(long sId, long tId, MemoryAllowance allowance);

	/**
	 * Pins an item backed by an allowance if it is already live in cache. A successful pin transfers resident-memory
	 * ownership from the cache to the owner of the allowance and guarantees data availability through the returned entry.
	 * While pinned, the entry remains visible to cache metadata but its bytes are not counted as cache-owned memory.
	 *
	 * Implementations must reserve the required bytes from the allowance before returning the entry.
	 *
	 * @param sId
	 * @param tId
	 * @param allowance
	 * @return the pinned block entry if available; null if the required memory could not be reserved or the block is not live
	 */
	BlockEntry pinIfLive(long sId, long tId, MemoryAllowance allowance);

	/**
	 * Unpins an item that is still backed by the given allowance. Unpinning tries to transfer resident-memory ownership
	 * back to the cache. An ownership transfer may commit immediately only if this does not cause the cache to exceed its
	 * hard limit. Otherwise, the transfer is deferred and the allowance remains charged until the returned handle commits,
	 * is reclaimed, or is superseded by a later pin that transfers ownership to another allowance.
	 *
	 * @param entry
	 * @param allowance
	 * @return a handle describing the ownership transfer from allowance-owned memory back to cache-owned memory
	 */
	UnpinHandle unpin(BlockEntry entry, MemoryAllowance allowance);

	/**
	 * References a specific entry, guaranteeing metadata persistence until dereferenced. Referencing does not affect
	 * resident-memory ownership; ownership remains with the current owner, either the cache or an allowance.
	 *
	 * @param entry
	 * @return
	 */
	int reference(BlockEntry entry);

	default int referencePinned(BlockEntry entry) {
		return reference(entry);
	}

	/**
	 * Dereferences a specific entry. This causes the item metadata and backing storage to become removable if no further
	 * reference exists. Removal may happen later if the item is still pinned or has a deferred unpin transfer.
	 *
	 * @param entry
	 * @return
	 */
	int dereference(BlockEntry entry);

	/**
	 * Dereferences the logical entry identified by the key without requiring callers to retain a BlockEntry.
	 */
	int dereference(BlockKey key);

	/**
	 * Updates the cache limits.
	 */
	void updateLimits(long hardLimit, long evictionLimit);

	/**
	 * Returns the current cache-owned resident size in bytes. This excludes bytes currently owned by operator
	 * allowances through pinned entries.
	 */
	long getOwnedCacheSize();

	default long getCacheSize() {
		return getOwnedCacheSize();
	}

	/**
	 * Shuts down the cache scheduler.
	 */
	void shutdown();

	interface UnpinHandle {
		BlockEntry getEntry();
		MemoryAllowance getAllowance();
		long getBytes();
		boolean isCommitted();
		CompletableFuture<Boolean> getCompletionFuture();

		/**
		 * Cancels a not-yet-committed transfer and returns the entry to the allowance owner. Returns null if ownership
		 * already transferred to the cache.
		 */
		BlockEntry reclaim();
	}
}
