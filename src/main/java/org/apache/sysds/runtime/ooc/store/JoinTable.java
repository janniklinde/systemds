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

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

public final class JoinTable {

	public static OOCFuture<Match> putIfAbsent(StateTable<IndexedMatrixValue> table, int slot,
		OOCStream.QueueCallback<IndexedMatrixValue> tile, MemoryAllowance allowance) {
		if(tile instanceof MaterializedCallback pinned && pinned.pinnedEntry() != null)
			return installReferenceOrTake(table, slot, pinned, allowance);
		ManagedPayload<IndexedMatrixValue> payload;
		if(tile instanceof InMemoryQueueCallback managed && managed.getManagedBytes() > 0) {
			payload = managed.extractManagedPayload();
			managed.close();
		}
		else {
			IndexedMatrixValue value = tile.get();
			long bytes = ((MatrixBlock) value.getValue()).getExactSerializedSize();
			allowance.reserveBlocking(bytes);
			payload = new ManagedPayload<>(value, bytes, allowance);
			tile.close();
		}
		OOCFuture<Match> result = new OOCFuture<>();
		OOCFuture<StoreLease<IndexedMatrixValue>> matched;
		try {
			matched = table.putOrTake(slot, payload, allowance);
		}
		catch(RuntimeException ex) {
			payload.release();
			return OOCFuture.failed(ex);
		}
		matched.whenComplete((lease, error) -> {
			if(error != null) {
				payload.release();
				result.completeExceptionally(error);
			}
			else if(lease == null)
				result.complete(null); //installed: the reservation transferred into the table
			else
				result.complete(new Match(
					new MaterializedCallback(new StoreLease<>(payload.value(), payload.bytes(), payload::release)),
					new MaterializedCallback(lease)));
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
	private static OOCFuture<Match> installReferenceOrTake(StateTable<IndexedMatrixValue> table,
		int slot, MaterializedCallback pinned, MemoryAllowance allowance) {
		OOCFuture<Match> result = new OOCFuture<>();
		OOCFuture<StoreLease<IndexedMatrixValue>> matched;
		try {
			matched = table.putReferenceOrTake(slot, pinned.pinnedEntry(), allowance);
		}
		catch(RuntimeException ex) {
			pinned.close();
			return OOCFuture.failed(ex);
		}
		matched.whenComplete((lease, error) -> {
			if(error != null) {
				pinned.close();
				result.completeExceptionally(error);
			}
			else if(lease == null) {
				pinned.close();
				result.complete(null);
			}
			else
				result.complete(new Match(pinned, new MaterializedCallback(lease)));
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

}
