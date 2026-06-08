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

public class OOCIOHandlerTileStoreBackend implements TileStoreBackend {
	@Override
	public CompletableFuture<Void> spill(BlockKey key, IndexedMatrixValue imv) {
		// Patch to avoid collisions with the OOCCacheScheduler
		BlockKey patched = new BlockKey(Long.MAX_VALUE - key.getStreamId(), key.getSequenceNumber());
		BlockEntry entry = new BlockEntry(patched, ((MatrixBlock)imv.getValue()).getExactSerializedSize(), imv);
		return OOCCacheManager.getIOHandler().scheduleEviction(entry);
	}

	@Override
	public CompletableFuture<IndexedMatrixValue> read(BlockKey key) {
		// Patch to avoid collisions with the OOCCacheScheduler
		BlockKey patched = new BlockKey(Long.MAX_VALUE - key.getStreamId(), key.getSequenceNumber());
		BlockEntry entry = new BlockEntry(patched);
		CompletableFuture<IndexedMatrixValue> result = new CompletableFuture<>();
		OOCCacheManager.getIOHandler().scheduleRead(entry).whenComplete((read, error) -> {
			try {
				if(error != null)
					result.completeExceptionally(error);
				else if(read == null)
					result.complete(null);
				else
					result.complete((IndexedMatrixValue)read.getDataUnsafe());
			}
			catch(Throwable t) {
				result.completeExceptionally(t);
			}
		});
		return result;
	}

	@Override
	public void delete(BlockKey key) {
		// Patch to avoid collisions with the OOCCacheScheduler
		BlockKey patched = new BlockKey(Long.MAX_VALUE - key.getStreamId(), key.getSequenceNumber());
		OOCCacheManager.getIOHandler().scheduleDeletion(new BlockEntry(patched));
	}
}
