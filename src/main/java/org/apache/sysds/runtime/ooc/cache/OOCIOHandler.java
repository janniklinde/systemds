package org.apache.sysds.runtime.ooc.cache;

import java.util.concurrent.CompletableFuture;

public interface OOCIOHandler {
	void shutdown();

	CompletableFuture<Void> scheduleEviction(BlockEntry block);

	CompletableFuture<BlockEntry> scheduleRead(BlockEntry block);

	CompletableFuture<Boolean> scheduleDeletion(BlockEntry block);
}
