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
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

/**
 * Rendezvous driver over an {@link OperatorStateTable}: atomic install-or-take per linearized slot,
 * routed by callback kind. Exclusive in-memory callbacks transfer their detached reservation
 * ({@code installOrTake}); shared pinned-lease callbacks from materialized boundaries park a logical
 * reference to the canonical entry ({@code installReferenceOrTake}) — with the pin held INSIDE this
 * helper until the rendezvous future resolves, because the chained INSTALLING-wait path makes that
 * contract easy to violate in primitive code; unmanaged callbacks are measured and reserved on the
 * supplied allowance.
 *
 * Ownership contract: the helper takes ownership of the supplied callback — the caller must hold no
 * aliases and must not touch it afterwards. The future completes with null when the tile was
 * INSTALLED (everything the helper held is settled; the partner side will take it later), or with a
 * {@link Match} when the partner was already installed: {@code own()} carries this tile's value and
 * {@code partner()} the taken partner lease, both as callbacks the caller closes exactly once after
 * the join compute.
 */
public final class TableRendezvous {

	private TableRendezvous() {
	}

	public static OOCFuture<Match> installOrTake(OperatorStateTable<IndexedMatrixValue> table, int slot,
		OOCStream.QueueCallback<IndexedMatrixValue> tile, MemoryAllowance allowance, long fallbackBytes) {
		if(tile instanceof MaterializationSink.PinnedLeaseCallback pinned)
			return installReferenceOrTake(table, slot, pinned);
		ManagedPayload<IndexedMatrixValue> payload;
		if(tile instanceof InMemoryQueueCallback managed) {
			payload = managed.extractManagedPayload();
			managed.close();
		}
		else {
			IndexedMatrixValue value = tile.get();
			long bytes = fallbackBytes > 0 ? fallbackBytes :
				((MatrixBlock) value.getValue()).getExactSerializedSize();
			allowance.reserveBlocking(bytes);
			payload = new ManagedPayload<>(value, bytes, allowance);
			tile.close();
		}
		OOCFuture<Match> result = new OOCFuture<>();
		table.installOrTake(slot, payload).whenComplete((lease, error) -> {
			if(error != null) {
				payload.release();
				result.completeExceptionally(error);
			}
			else if(lease == null)
				result.complete(null); //installed: the reservation transferred into the table
			else
				result.complete(new Match(new PayloadCallback(payload), new StateLeaseCallback(lease)));
		});
		return result;
	}

	/**
	 * Reference rendezvous for a tile of a shared materialized boundary: the canonical entry stays
	 * where it is, the table parks (or the partner takes) a counted logical reference. The supplied
	 * alias is the pin that keeps the entry alive; it is closed here exactly when the rendezvous
	 * resolved (install: the table holds its own reference now; take: the alias moves into the match
	 * as the own-side value).
	 */
	private static OOCFuture<Match> installReferenceOrTake(OperatorStateTable<IndexedMatrixValue> table,
		int slot, MaterializationSink.PinnedLeaseCallback pinned) {
		OOCFuture<Match> result = new OOCFuture<>();
		table.installReferenceOrTake(slot, pinned.pinnedEntry()).whenComplete((lease, error) -> {
			if(error != null) {
				pinned.close();
				result.completeExceptionally(error);
			}
			else if(lease == null) {
				pinned.close();
				result.complete(null);
			}
			else
				result.complete(new Match(pinned, new StateLeaseCallback(lease)));
		});
		return result;
	}

	/**
	 * A resolved rendezvous: the arriving tile ({@code own}) paired with the previously installed
	 * partner ({@code partner}). Close both exactly once after the compute (try-with-resources on the
	 * pair); closing releases the own-side reservation/pin and consumes the partner lease.
	 */
	public static final class Match {
		private final OOCStream.QueueCallback<IndexedMatrixValue> _own;
		private final OOCStream.QueueCallback<IndexedMatrixValue> _partner;

		private Match(OOCStream.QueueCallback<IndexedMatrixValue> own,
			OOCStream.QueueCallback<IndexedMatrixValue> partner) {
			_own = own;
			_partner = partner;
		}

		public OOCStream.QueueCallback<IndexedMatrixValue> own() {
			return _own;
		}

		public OOCStream.QueueCallback<IndexedMatrixValue> partner() {
			return _partner;
		}
	}

	/**
	 * Wraps an unsettled {@link ManagedPayload} as a callback: the last close across all
	 * {@code keepOpen} aliases releases the detached reservation.
	 */
	private static final class PayloadCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final ManagedPayload<IndexedMatrixValue> _payload;
		private DMLRuntimeException _failure;
		private int _references = 1;
		private boolean _closed;

		private PayloadCallback(ManagedPayload<IndexedMatrixValue> payload) {
			_payload = payload;
		}

		@Override
		public IndexedMatrixValue get() {
			if(_failure != null)
				throw _failure;
			return _payload.value();
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
			_payload.release();
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

	/**
	 * Wraps a taken {@link OperatorStateTable.StateLease} as a callback: the last close across all
	 * {@code keepOpen} aliases is the exactly-once consumption of the taken value.
	 */
	private static final class StateLeaseCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final OperatorStateTable.StateLease<IndexedMatrixValue> _lease;
		private DMLRuntimeException _failure;
		private int _references = 1;
		private boolean _closed;

		private StateLeaseCallback(OperatorStateTable.StateLease<IndexedMatrixValue> lease) {
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
			try {
				_lease.close();
			}
			catch(RuntimeException ex) {
				throw ex;
			}
			catch(Exception ex) {
				throw new DMLRuntimeException(ex);
			}
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
