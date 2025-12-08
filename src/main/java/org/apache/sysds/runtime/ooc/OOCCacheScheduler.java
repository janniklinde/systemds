package org.apache.sysds.runtime.ooc;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface OOCCacheScheduler {
	static OOCCacheScheduler _scheduler = null;

	static OOCCacheScheduler get() {
		return null;
	}

	/**
	 * Requests a single block from the cache.
	 * @param key the requested key associated to the block
	 * @return the available BlockEntry
	 */
	CompletableFuture<BlockEntry> request(BlockKey key);

	/**
	 * Requests a list of blocks from the cache that must be available at the same time.
	 * @param keys the requested keys associated to the block
	 * @return the list of available BlockEntries
	 */
	CompletableFuture<List<BlockEntry>> request(List<BlockKey> keys);

	/**
	 * Places a new block in the cache. Note that objects are immutable and cannot be overwritten.
	 * @param key the associated key of the block
	 * @param data the block data
	 * @param size the size of the data
	 */
	void put(BlockKey key, Object data, long size);

	/**
	 * Forgets a block from the cache.
	 * @param key the associated key of the block
	 */
	void forget(BlockKey key);

	/**
	 * Pins a BlockEntry in cache to prevent eviction.
	 * @param entry the entry to be pinned
	 */
	void pin(BlockEntry entry);

	/**
	 * Unpins a pinned block.
	 * @param entry the entry to be unpinned
	 */
	void unpin(BlockEntry entry);
}
