package org.apache.sysds.runtime.ooc;

import java.util.concurrent.CompletableFuture;

public interface IOHandlerOOC {
	CompletableFuture<Void> scheduleEviction(BlockEntry block);

	CompletableFuture<BlockEntry> scheduleRead(BlockEntry block);

	CompletableFuture<Boolean> scheduleDeletion(BlockEntry block);
}
