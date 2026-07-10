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
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

public final class MaterializedCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
	private final StoreLease<IndexedMatrixValue> _lease;
	private DMLRuntimeException _failure;
	private boolean _closed;

	public MaterializedCallback(StoreLease<IndexedMatrixValue> lease) {
		_lease = lease;
	}

	public BlockEntry pinnedEntry() {
		return _lease.entry();
	}

	@Override
	public IndexedMatrixValue get() {
		if(_failure != null)
			throw _failure;
		return _lease.value();
	}

	@Override
	public synchronized OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
		if(_closed)
			throw new IllegalStateException("Cannot keep open a closed callback");
		return new MaterializedCallback(_lease.retain());
	}

	@Override
	public synchronized OOCStream.QueueCallback<IndexedMatrixValue> transferOwnershipBlocking(
		MemoryAllowance allowance) {
		if(_closed)
			throw new IllegalStateException("Cannot transfer ownership of a closed callback");
		_lease.transferOwnershipBlocking(allowance);
		return this;
	}

	@Override
	public synchronized void close() {
		if(_closed)
			return;
		_closed = true;
		_lease.close();
	}

	@Override
	public void fail(DMLRuntimeException failure) {
		_failure = failure;
	}

	@Override
	public boolean isEos() {
		return false;
	}

	@Override
	public boolean isFailure() {
		return _failure != null;
	}
}
