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

public final class StoreLease<T extends SpillableObject> implements MaterializedStore.Lease<T>, StateLease<T> {
	private final Consumer<StoreLease<T>> _releaser;
	private final int _index;
	private final T _value;
	private final BlockEntry _entry;
	private final long _bytes;
	private boolean _open;
	private final AtomicInteger _shared;

	StoreLease(Consumer<StoreLease<T>> releaser, int index, BlockEntry entry) {
		this(releaser, index, null, entry, entry.getSize(), new AtomicInteger(1));
	}

	StoreLease(BlockEntry entry, Runnable releaser) {
		this(ignored -> releaser.run(), -1, null, entry, entry.getSize(), new AtomicInteger(1));
	}

	StoreLease(T value, long bytes, Runnable releaser) {
		this(ignored -> releaser.run(), -1, value, null, bytes, new AtomicInteger(1));
	}

	private StoreLease(Consumer<StoreLease<T>> releaser, int index, T value, BlockEntry entry, long bytes,
		AtomicInteger shared) {
		_releaser = releaser;
		_index = index;
		_value = value;
		_entry = entry;
		_bytes = bytes;
		_open = true;
		_shared = shared;
	}

	@Override
	public int index() {
		return _index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized T value() {
		if(!_open)
			throw new IllegalStateException("Lease is closed");
		return _entry == null ? _value : (T) _entry.getData();
	}

	@Override
	public long bytes() {
		return _bytes;
	}

	synchronized BlockEntry entry() {
		if(!_open)
			throw new IllegalStateException("Lease is closed");
		return _entry;
	}

	BlockEntry entryUnsafe() {
		return _entry;
	}

	@Override
	public synchronized StoreLease<T> retain() {
		if(!_open)
			throw new IllegalStateException("Lease is closed");
		_shared.incrementAndGet();
		return new StoreLease<>(_releaser, _index, _value, _entry, _bytes, _shared);
	}

	@Override
	public synchronized void close() {
		if(!_open)
			return;
		_open = false;
		if(_shared.decrementAndGet() == 0)
			_releaser.accept(this);
	}
}
