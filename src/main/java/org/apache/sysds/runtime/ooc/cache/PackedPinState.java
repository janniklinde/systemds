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
import java.util.concurrent.TimeUnit;

final class PackedPinState {
	final BlockEntry physicalEntry;
	private MemoryAllowance[] allowances;
	private int[] counts;
	private OOCFuture<BlockEntry>[] futures;
	private long[] releaseDueNanos;
	private DelayedPackedUnpinHandle[] releaseHandles;
	private int size;
	private boolean releaseQueued;

	@SuppressWarnings("unchecked")
	PackedPinState(BlockEntry physicalEntry) {
		this.physicalEntry = physicalEntry;
		allowances = new MemoryAllowance[2];
		counts = new int[2];
		futures = new OOCFuture[2];
		releaseDueNanos = new long[2];
		releaseHandles = new DelayedPackedUnpinHandle[2];
	}

	synchronized OOCFuture<BlockEntry> pin(OOCCacheImpl physical, MemoryAllowance allowance, boolean liveOnly) {
		int ix = indexOf(allowance);
		if(ix >= 0) {
			cancelRelease(ix);
			counts[ix]++;
			return futures[ix];
		}
		OOCFuture<BlockEntry> future = liveOnly ?
			OOCFuture.completed(physical.pinIfLive(physicalEntry.getKey().getStreamId(),
				physicalEntry.getKey().getSequenceNumber(), allowance)) :
			physical.pin(physicalEntry.getKey(), allowance);
		addAllowance(allowance, future);
		future.whenComplete((entry, ex) -> {
			if(entry == null || ex != null)
				removeFailedAllowance(allowance, future);
		});
		return future;
	}

	BlockEntry pinIfLive(OOCCacheImpl physical, MemoryAllowance allowance) {
		try {
			return pin(physical, allowance, true).getNow(null);
		}
		catch(RuntimeException ex) {
			return null;
		}
	}

	synchronized OOCCache.UnpinHandle unpin(OOCPackedCache owner, long releaseDelayMs,
		MemoryAllowance allowance) {
		int ix = indexOf(allowance);
		if(ix < 0)
			return ImmediatePackedUnpinHandle.committed(physicalEntry, allowance, physicalEntry.getSize());
		counts[ix]--;
		if(counts[ix] > 0)
			return ImmediatePackedUnpinHandle.committed(physicalEntry, allowance, physicalEntry.getSize());
		DelayedPackedUnpinHandle handle = new DelayedPackedUnpinHandle(physicalEntry, allowance);
		releaseHandles[ix] = handle;
		releaseDueNanos[ix] = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, releaseDelayMs));
		owner.enqueueRelease(this);
		return handle;
	}

	long releaseDuePins(OOCCacheImpl physical, long nowNanos) {
		ArrayList<PackedRelease> due = null;
		long nextDueNanos = Long.MAX_VALUE;
		synchronized(this) {
			for(int i = 0; i < size;) {
				DelayedPackedUnpinHandle handle = releaseHandles[i];
				if(handle == null || counts[i] > 0) {
					i++;
					continue;
				}
				long dueNanos = releaseDueNanos[i];
				if(dueNanos > nowNanos) {
					nextDueNanos = Math.min(nextDueNanos, dueNanos);
					i++;
					continue;
				}
				if(due == null)
					due = new ArrayList<>();
				due.add(new PackedRelease(allowances[i], handle));
				removeAt(i);
			}
		}
		if(due != null)
			for(PackedRelease release : due)
				releasePhysicalPin(physical, release.allowance, release.handle);
		return nextDueNanos;
	}

	synchronized boolean markReleaseQueued() {
		if(releaseQueued)
			return false;
		releaseQueued = true;
		return true;
	}

	synchronized void clearReleaseQueued() {
		releaseQueued = false;
	}

	private int indexOf(MemoryAllowance allowance) {
		for(int i = 0; i < size; i++)
			if(allowances[i] == allowance)
				return i;
		return -1;
	}

	private synchronized void addAllowance(MemoryAllowance allowance, OOCFuture<BlockEntry> future) {
		if(size == allowances.length)
			grow();
		allowances[size] = allowance;
		counts[size] = 1;
		futures[size] = future;
		size++;
	}

	private void grow() {
		int nextSize = size * 2;
		MemoryAllowance[] biggerAllowances = new MemoryAllowance[nextSize];
		int[] biggerCounts = new int[nextSize];
		@SuppressWarnings("unchecked")
		OOCFuture<BlockEntry>[] biggerFutures = new OOCFuture[nextSize];
		long[] biggerReleaseDueNanos = new long[nextSize];
		DelayedPackedUnpinHandle[] biggerReleaseHandles = new DelayedPackedUnpinHandle[nextSize];
		System.arraycopy(allowances, 0, biggerAllowances, 0, size);
		System.arraycopy(counts, 0, biggerCounts, 0, size);
		System.arraycopy(futures, 0, biggerFutures, 0, size);
		System.arraycopy(releaseDueNanos, 0, biggerReleaseDueNanos, 0, size);
		System.arraycopy(releaseHandles, 0, biggerReleaseHandles, 0, size);
		allowances = biggerAllowances;
		counts = biggerCounts;
		futures = biggerFutures;
		releaseDueNanos = biggerReleaseDueNanos;
		releaseHandles = biggerReleaseHandles;
	}

	private void cancelRelease(int ix) {
		releaseDueNanos[ix] = 0;
		DelayedPackedUnpinHandle handle = releaseHandles[ix];
		if(handle != null) {
			releaseHandles[ix] = null;
			handle.complete(false);
		}
	}

	private void releasePhysicalPin(OOCCacheImpl physical, MemoryAllowance allowance,
		DelayedPackedUnpinHandle handle) {
		OOCCache.UnpinHandle physicalHandle = physical.unpin(physicalEntry, allowance);
		if(physicalHandle.isCommitted()) {
			handle.complete(true);
			return;
		}
		physicalHandle.getCompletionFuture().whenComplete((committed, ex) ->
			handle.complete(ex == null && Boolean.TRUE.equals(committed)));
	}

	private synchronized void removeFailedAllowance(MemoryAllowance allowance, OOCFuture<BlockEntry> future) {
		int ix = indexOf(allowance);
		if(ix >= 0 && futures[ix] == future)
			removeAt(ix);
	}

	private void removeAt(int ix) {
		int last = --size;
		allowances[ix] = allowances[last];
		counts[ix] = counts[last];
		futures[ix] = futures[last];
		releaseDueNanos[ix] = releaseDueNanos[last];
		releaseHandles[ix] = releaseHandles[last];
		allowances[last] = null;
		counts[last] = 0;
		futures[last] = null;
		releaseDueNanos[last] = 0;
		releaseHandles[last] = null;
	}

	private record PackedRelease(MemoryAllowance allowance, DelayedPackedUnpinHandle handle) {
	}
}
