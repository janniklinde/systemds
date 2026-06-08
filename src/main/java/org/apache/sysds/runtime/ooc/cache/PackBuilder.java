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

package org.apache.sysds.runtime.ooc.cache;

import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class PackBuilder {
	final int streamSlot;
	final MemoryAllowance allowance;
	final long packTargetBytes;
	final List<PackUnpinHandle> deferredUnpins = new ArrayList<>();
	long[] streamIds = new long[16];
	long[] tileIds = new long[16];
	private Object[] values = new Object[16];
	long[] sizes = new long[16];
	long bytes;
	int count;
	int activePins;
	boolean sealed;
	boolean sealScheduled;
	private boolean producerTransferred;
	PackedPinState state;

	PackBuilder(int streamSlot, MemoryAllowance allowance, long packTargetBytes) {
		this.streamSlot = streamSlot;
		this.allowance = allowance;
		this.packTargetBytes = packTargetBytes;
	}

	int append(long streamId, long tileId, Object value, long size) {
		ensureCapacity(count + 1);
		int slot = count++;
		streamIds[slot] = streamId;
		tileIds[slot] = tileId;
		values[slot] = value;
		sizes[slot] = size;
		bytes += size;
		activePins++;
		return slot;
	}

	long getBytes() {
		return bytes;
	}

	PackedBlock createBlock() {
		return new PackedBlock(Arrays.copyOf(values, count), Arrays.copyOf(sizes, count), bytes);
	}

	PackUnpinHandle unpinProducer(BlockEntry entry, int slot, MemoryAllowance owner) {
		activePins--;
		PackUnpinHandle handle = new PackUnpinHandle(entry, owner, sizes[slot]);
		deferredUnpins.add(handle);
		return handle;
	}

	void transferProducerOwnership(OOCCacheImpl physical) {
		if(state == null || physical == null || producerTransferred)
			return;
		producerTransferred = true;
		OOCCache.UnpinHandle physicalUnpin = physical.unpin(state.physicalEntry, allowance);
		if(physicalUnpin.isCommitted()) {
			completeDeferredUnpins(true);
			return;
		}
		physicalUnpin.getCompletionFuture().whenComplete((committed, ex) ->
			completeDeferredUnpins(ex == null && committed));
	}

	private void ensureCapacity(int minSize) {
		if(minSize <= values.length)
			return;
		int len = values.length;
		while(minSize > len)
			len <<= 1;
		streamIds = Arrays.copyOf(streamIds, len);
		tileIds = Arrays.copyOf(tileIds, len);
		values = Arrays.copyOf(values, len);
		sizes = Arrays.copyOf(sizes, len);
	}

	private void completeDeferredUnpins(boolean committed) {
		for(PackUnpinHandle handle : deferredUnpins)
			handle.complete(committed);
		deferredUnpins.clear();
	}
}
