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

import java.util.List;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;

final class SourceBackedGroupCallback implements OOCStream.GroupQueueCallback<IndexedMatrixValue> {
	private final List<IndexedMatrixValue> _values;
	private final OOCIOHandler.SourceBlockDescriptor _descriptor;
	private ReservationBudget _ownership;
	private boolean _transferred;

	SourceBackedGroupCallback(List<IndexedMatrixValue> values, OOCIOHandler.SourceBlockDescriptor descriptor,
		ReservationBudget ownership) {
		if(values == null || values.isEmpty())
			throw new IllegalArgumentException("Source-backed callback requires at least one value.");
		_values = List.copyOf(values);
		_descriptor = descriptor;
		_ownership = ownership;
	}

	SourceGroup take() {
		if(_transferred || _ownership == null)
			throw new IllegalStateException("Source-backed callback ownership was already transferred.");
		_transferred = true;
		ReservationBudget ownership = _ownership;
		_ownership = null;
		return new SourceGroup(_values, _descriptor, ownership);
	}

	@Override
	public int size() {
		return _values.size();
	}

	@Override
	public OOCStream.QueueCallback<IndexedMatrixValue> getCallback(int index) {
		throw new UnsupportedOperationException("Source-backed groups can only be consumed by materialization.");
	}

	@Override
	public IndexedMatrixValue get() {
		throw new UnsupportedOperationException("Source-backed groups can only be consumed by materialization.");
	}

	@Override
	public OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
		throw new UnsupportedOperationException("Source-backed groups cannot escape their materialization boundary.");
	}

	@Override
	public void close() {
		ReservationBudget ownership = _ownership;
		_ownership = null;
		if(ownership != null)
			ownership.close();
	}

	@Override
	public void fail(DMLRuntimeException failure) {
		close();
	}

	@Override
	public boolean isEos() {
		return false;
	}

	@Override
	public boolean isFailure() {
		return false;
	}

	record SourceGroup(List<IndexedMatrixValue> values, OOCIOHandler.SourceBlockDescriptor descriptor,
		ReservationBudget ownership) {
	}
}
