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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public final class MaterializedCallback<T extends SpillableObject> implements OOCStream.QueueCallback<T> {
	private static final MemoryAllowance REVIVE_ALLOWANCE = new SyncMemoryAllowance(GlobalMemoryBroker.getSource());

	private StoreLease<T> _lease;
	private final AtomicReference<DMLRuntimeException> _failure;
	private final int _index;
	private final MaterializedStore<T> _store;
	private boolean _closed;
	private boolean _parked;

	public MaterializedCallback(StoreLease<T> lease) {
		this(lease, new AtomicReference<>(), -1, null);
	}

	public MaterializedCallback(StoreLease<T> lease, int index) {
		this(lease, new AtomicReference<>(), index, null);
	}

	public MaterializedCallback(StoreLease<T> lease, int index, MaterializedStore<T> store) {
		this(lease, new AtomicReference<>(), index, store);
	}

	private MaterializedCallback(StoreLease<T> lease, AtomicReference<DMLRuntimeException> failure, int index,
		MaterializedStore<T> store) {
		_lease = lease;
		_failure = failure;
		_index = index;
		_store = store;
	}

	public synchronized long tryPark() {
		if(_closed || _parked || _store == null || _index < 0 || _lease == null)
			return 0;
		if(!_lease.isSole() || _failure.get() != null)
			return 0;
		BlockEntry entry = _lease.entry();
		long bytes = entry != null ? entry.getSize() : 0;
		if(bytes <= 0)
			return 0;
		_store.cache().reference(entry);
		_parked = true;
		StoreLease<T> lease = _lease;
		_lease = null;
		lease.close();
		return bytes;
	}

	private synchronized void revive() {
		if(!_parked)
			return;
		BlockEntry entry;
		try {
			entry = OOCUtils.pinAdmitted(_store.cache(), _store.streamId(), _index, REVIVE_ALLOWANCE, () -> false).get();
		}
		catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new DMLRuntimeException(e);
		}
		catch(ExecutionException e) {
			throw DMLRuntimeException.of(e.getCause());
		}
		if(entry == null)
			throw new DMLRuntimeException("Parked block " + _index + " vanished before it was revived.");
		_lease = StoreLease.createAsync(entry, () -> _store.cache().unpin(entry, REVIVE_ALLOWANCE).getCompletionFuture());
		_store.cache().dereference(new BlockKey(_store.streamId(), _index));
		_parked = false;
	}

	public int publishedIndex() {
		return _index;
	}

	public synchronized BlockEntry pinnedEntry() {
		return _lease != null ? _lease.entry() : null;
	}

	@Override
	public synchronized T get() {
		DMLRuntimeException failure = _failure.get();
		if(failure != null)
			throw failure;
		revive();
		return _lease.value();
	}

	@Override
	public synchronized OOCStream.QueueCallback<T> keepOpen() {
		if(_closed)
			throw new IllegalStateException("Cannot keep open a closed callback");
		revive();
		return new MaterializedCallback<>(_lease.retain(), _failure, _index, _store);
	}

	@Override
	public synchronized void close() {
		if(_closed)
			return;
		_closed = true;
		if(_lease != null)
			_lease.close();
	}

	@Override
	public void fail(DMLRuntimeException failure) {
		_failure.set(failure);
	}

	@Override
	public boolean isEos() {
		return false;
	}

	@Override
	public boolean isFailure() {
		return _failure.get() != null;
	}
}
