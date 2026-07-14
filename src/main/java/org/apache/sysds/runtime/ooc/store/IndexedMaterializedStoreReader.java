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
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

public final class IndexedMaterializedStoreReader<T extends SpillableObject>
	implements MaterializedStore.IndexedReader<T>, StoreRegisteredReader {
	private final OOCCache cache;
	private final long streamId;
	private final IntSupplier completedSize;
	private final MaterializedStore.Liveness liveness;
	private final Runnable afterClose;
	private final IntConsumer afterRelease;
	private volatile boolean closed;

	IndexedMaterializedStoreReader(OOCCache cache, long streamId, IntSupplier completedSize,
		MaterializedStore.Liveness liveness, Runnable afterClose, IntConsumer afterRelease) {
		this.cache = cache;
		this.streamId = streamId;
		this.completedSize = completedSize;
		this.liveness = liveness;
		this.afterClose = afterClose;
		this.afterRelease = afterRelease;
	}

	@Override
	public MaterializedStore.Liveness liveness() {
		return liveness;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public OOCFuture<MaterializedStore.Lease<T>> request(int index, MemoryAllowance requestAllowance) {
		checkReady(index);
		reserve(index);
		OOCFuture<BlockEntry> pinned = StorePinAdmission.pinAdmitted(cache, streamId, index, requestAllowance,
			() -> closed);
		OOCFuture<MaterializedStore.Lease<T>> result = new OOCFuture<>();
		pinned.whenComplete((entry, error) -> {
			if(error != null) {
				liveness.unreserve(index);
				result.completeExceptionally(error);
			}
			else if(entry == null) {
				liveness.unreserve(index);
				result.complete(null);
			}
			else
				result.complete(
					new StoreLease<>(lease -> release(lease.index(), lease.entryUnsafe(), requestAllowance), index, entry));
		});
		return result;
	}

	@Override
	public MaterializedStore.Lease<T> requestIfLive(int index, MemoryAllowance requestAllowance) {
		checkReady(index);
		reserve(index);
		BlockEntry entry = cache.pinIfLive(streamId, index, requestAllowance);
		if(entry == null) {
			liveness.unreserve(index);
			return null;
		}
		return new StoreLease<>(lease -> release(lease.index(), lease.entryUnsafe(), requestAllowance), index, entry);
	}

	@Override
	public void close() {
		if(closed)
			return;
		closed = true;
		afterClose.run();
	}

	public void release(int index, BlockEntry entry, MemoryAllowance allowance) {
		cache.unpin(entry, allowance);
		liveness.consumed(index);
		afterRelease.accept(index);
	}

	private void reserve(int index) {
		if(!liveness.reserve(index))
			throw new IllegalStateException("Index is no longer live for this reader: " + index);
	}

	private void checkReady(int index) {
		if(closed)
			throw new IllegalStateException("Reader is closed");
		if(index < 0 || index >= completedSize.getAsInt())
			throw new IndexOutOfBoundsException("Invalid requested index: " + index);
	}
}
