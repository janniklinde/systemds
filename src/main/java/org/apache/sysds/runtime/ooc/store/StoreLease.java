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
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;

import java.util.concurrent.atomic.AtomicInteger;

final class StoreLease<T extends SpillableObject> implements MaterializedStore.Lease<T> {
	private final StoreLeaseReleaser releaser;
	private final int index;
	private final BlockEntry entry;
	private boolean open;
	private SharedState shared;

	StoreLease(StoreLeaseReleaser releaser, int index, BlockEntry entry) {
		this.releaser = releaser;
		this.index = index;
		this.entry = entry;
		open = true;
	}

	private StoreLease(SharedState shared) {
		releaser = shared.releaser;
		index = shared.index;
		entry = shared.entry;
		this.shared = shared;
		open = true;
	}

	@Override
	public int index() {
		return index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T value() {
		if(!open)
			throw new IllegalStateException("Lease is closed");
		return (T)entry.getData();
	}

	@Override
	public MaterializedStore.Lease<T> retain() {
		if(!open)
			throw new IllegalStateException("Lease is closed");
		if(shared == null)
			shared = new SharedState(releaser, index, entry);
		else
			shared.retain();
		return new StoreLease<>(shared);
	}

	@Override
	public void close() {
		if(!open)
			return;
		open = false;
		if(shared == null)
			releaser.release(index, entry);
		else
			shared.release();
	}

	private static final class SharedState {
		private final StoreLeaseReleaser releaser;
		private final int index;
		private final BlockEntry entry;
		private final AtomicInteger references;

		private SharedState(StoreLeaseReleaser releaser, int index, BlockEntry entry) {
			this.releaser = releaser;
			this.index = index;
			this.entry = entry;
			references = new AtomicInteger(2);
		}

		private void retain() {
			references.incrementAndGet();
		}

		private void release() {
			if(references.decrementAndGet() == 0)
				releaser.release(index, entry);
		}
	}
}
