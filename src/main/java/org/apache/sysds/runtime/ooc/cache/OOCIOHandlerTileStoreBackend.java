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

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class OOCIOHandlerTileStoreBackend implements TileStoreBackend {
	private final ConcurrentHashMap<BlockKey, IndexedMatrixValue> _pendingReadables = new ConcurrentHashMap<>();

	@Override
	public CompletableFuture<Void> spill(BlockKey key, IndexedMatrixValue imv) {
		// Patch to avoid collisions with the OOCCacheScheduler
		BlockKey patched = new BlockKey(Long.MAX_VALUE - key.getStreamId(), key.getSequenceNumber());
		BlockEntry entry = new BlockEntry(patched, ((MatrixBlock)imv.getValue()).getExactSerializedSize(), imv);
		OOCIOHandler.SpillFuture future = OOCCacheManager.getIOHandler().scheduleEviction(entry);
		var idx = imv.getIndexes();
		var val = imv.getValue();
		return future.serializedFuture().thenRun(() -> {
			if(future.readFuture().isDone())
				return;
			_pendingReadables.put(key, new IndexedMatrixValue(idx, val));
			future.readFuture().thenRun(() -> discard(_pendingReadables.remove(key)));
		});
	}

	@Override
	public CompletableFuture<IndexedMatrixValue> read(BlockKey key) {
		var imv = _pendingReadables.get(key);
		if(imv != null)
			return CompletableFuture.completedFuture(imv);
		// Patch to avoid collisions with the OOCCacheScheduler
		BlockKey patched = new BlockKey(Long.MAX_VALUE - key.getStreamId(), key.getSequenceNumber());
		BlockEntry entry = new BlockEntry(patched);
		return OOCCacheManager.getIOHandler().scheduleRead(entry).thenApply(e -> (IndexedMatrixValue)e.getDataUnsafe());
	}

	@Override
	public void delete(BlockKey key) {
		discard(_pendingReadables.remove(key));
		// Patch to avoid collisions with the OOCCacheScheduler
		BlockKey patched = new BlockKey(Long.MAX_VALUE - key.getStreamId(), key.getSequenceNumber());
		OOCCacheManager.getIOHandler().scheduleDeletion(new BlockEntry(patched));
	}

	private void discard(IndexedMatrixValue imv) {
		if(imv != null)
			imv.discard();
	}
}
