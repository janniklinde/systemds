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
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

public final class StoreLiveLease<T extends SpillableObject> implements MaterializedStore.Lease<T> {
	private final State state;
	private boolean open;

	StoreLiveLease(OOCCache cache, int index, BlockEntry entry, MemoryAllowance allowance,
		IntConsumer afterRelease) {
		state = new State(cache, index, entry, allowance, afterRelease);
		open = true;
	}

	private StoreLiveLease(State state) {
		this.state = state;
		open = true;
	}

	@Override
	public int index() {
		return state.index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T value() {
		if(!open)
			throw new IllegalStateException("Lease is closed");
		return (T)state.entry.getData();
	}

	public BlockEntry entry() {
		if(!open)
			throw new IllegalStateException("Lease is closed");
		return state.entry;
	}

	@Override
	public StoreLiveLease<T> retain() {
		if(!open)
			throw new IllegalStateException("Lease is closed");
		state.retain();
		return new StoreLiveLease<>(state);
	}

	@Override
	public void close() {
		if(!open)
			return;
		open = false;
		state.release();
	}

	private static final class State {
		private final OOCCache cache;
		private final int index;
		private final BlockEntry entry;
		private final MemoryAllowance allowance;
		private final IntConsumer afterRelease;
		private final AtomicInteger references;

		private State(OOCCache cache, int index, BlockEntry entry, MemoryAllowance allowance,
			IntConsumer afterRelease) {
			this.cache = cache;
			this.index = index;
			this.entry = entry;
			this.allowance = allowance;
			this.afterRelease = afterRelease;
			references = new AtomicInteger(1);
		}

		private void retain() {
			int before = references.getAndIncrement();
			if(before <= 0)
				throw new IllegalStateException("Live lease is already fully closed");
		}

		private void release() {
			if(references.decrementAndGet() == 0) {
				cache.unpin(entry, allowance);
				afterRelease.accept(index);
			}
		}
	}
}
