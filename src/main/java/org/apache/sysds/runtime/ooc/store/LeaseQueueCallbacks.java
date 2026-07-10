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

/**
 * Common adapters from OOC store/table leases to stream callbacks. Store leases retain one physical
 * lease per alias; state-table leases share one taken lease and consume it when the last alias closes.
 */
public final class LeaseQueueCallbacks {
	private LeaseQueueCallbacks() {
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> store(
		MaterializedStore.Lease<IndexedMatrixValue> lease) {
		if(lease instanceof StoreLease<?> storeLease)
			return new MaterializedCallback(castStoreLease(storeLease));
		return new StoreLeaseBackedCallback(lease);
	}

	@SuppressWarnings("unchecked")
	private static StoreLease<IndexedMatrixValue> castStoreLease(StoreLease<?> lease) {
		return (StoreLease<IndexedMatrixValue>) lease;
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> state(
		StateLease<IndexedMatrixValue> lease) {
		return new StateLeaseBackedCallback(lease);
	}

	public static MaterializedCallback pinned(StoreLease<IndexedMatrixValue> lease) {
		return new MaterializedCallback(lease);
	}

	private static final class StoreLeaseBackedCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final MaterializedStore.Lease<IndexedMatrixValue> _lease;
		private DMLRuntimeException _failure;
		private boolean _closed;

		private StoreLeaseBackedCallback(MaterializedStore.Lease<IndexedMatrixValue> lease) {
			_lease = lease;
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
			return new StoreLeaseBackedCallback(_lease.retain());
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

	private static final class StateLeaseBackedCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final StateLease<IndexedMatrixValue> _lease;
		private DMLRuntimeException _failure;
		private int _references = 1;
		private boolean _closed;

		private StateLeaseBackedCallback(StateLease<IndexedMatrixValue> lease) {
			_lease = lease;
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
			_references++;
			return this;
		}

		@Override
		public synchronized void close() {
			if(_closed || --_references > 0)
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
}
