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
import java.util.function.Consumer;

public final class StoreLease<T extends SpillableObject> implements MaterializedStore.Lease<T> {
	private final Consumer<StoreLease<T>> _releaser;
	private final int _index;
	private final BlockEntry _entry;
	private boolean _open;
	private AtomicInteger _shared;

	StoreLease(Consumer<StoreLease<T>> releaser, int index, BlockEntry entry) {
		this(releaser, index, entry, null);
	}

	StoreLease(Consumer<StoreLease<T>> releaser, int index, BlockEntry entry, AtomicInteger shared) {
		_releaser = releaser;
		_index = index;
		_entry = entry;
		_open = true;
		_shared = shared;
	}

	@Override
	public int index() {
		return _index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T value() {
		if(!_open)
			throw new IllegalStateException("Lease is closed");
		return (T) _entry.getData();
	}

	BlockEntry entry() {
		if(!_open)
			throw new IllegalStateException("Lease is closed");
		return _entry;
	}

	BlockEntry entryUnsafe() {
		return _entry;
	}

	@Override
	public StoreLease<T> retain() {
		if(!_open)
			throw new IllegalStateException("Lease is closed");
		if(_shared == null)
			_shared = new AtomicInteger(2);
		else
			_shared.incrementAndGet();
		return new StoreLease<>(_releaser, _index, _entry, _shared);
	}

	@Override
	public void close() {
		if(!_open)
			return;
		_open = false;
		if(_shared == null || _shared.decrementAndGet() == 0)
			_releaser.accept(this);
	}
}
