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

package org.apache.sysds.runtime.ooc.memory;

import org.apache.sysds.runtime.ooc.OOCDebug;
import org.apache.sysds.utils.Statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GlobalMemoryBroker implements MemoryBroker {
	private static final long RECLAIM_RETRY_DELAY_MS = 5;
	private static final double RECLAIM_PRESSURE = 0.85;
	private static final ScheduledThreadPoolExecutor RECLAIM_EXECUTOR = createReclaimExecutor();

	private enum BrokerMode {
		RELAXED, STRICT
	}

	private static final GlobalMemoryBroker BROKER = new GlobalMemoryBroker(Runtime.getRuntime().maxMemory() / 3);

	public static GlobalMemoryBroker get() {
		return BROKER;
	}

	public static long defaultAllowanceLimit() {
		return Math.max(Runtime.getRuntime().maxMemory() / 10, 200_000_000L);
	}

	private final long _allowedBytes;
	private final CopyOnWriteArrayList<MemoryAllowance> _allowances;
	private final AtomicBoolean _reclaimRunning;
	private long _usedBytes;
	private BrokerMode _brokerMode;

	private record TargetUpdate(MemoryAllowance _allowance, long _target) {}

	private static ScheduledThreadPoolExecutor createReclaimExecutor() {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> {
			Thread t = new Thread(r, "ooc-memory-broker-reclaimer");
			t.setDaemon(true);
			return t;
		});
		executor.setRemoveOnCancelPolicy(true);
		return executor;
	}

	public GlobalMemoryBroker(long allowedBytes) {
		_allowedBytes = allowedBytes;
		_usedBytes = 0;
		_allowances = new CopyOnWriteArrayList<>();
		_reclaimRunning = new AtomicBoolean(false);
	}

	@Override
	public long requestMemory(MemoryAllowance allowance, long minSize, long maxSize) {
		List<TargetUpdate> updates = null;
		long allow = 0;
		long usedBefore;
		int allowanceCount;
		synchronized(this) {
			usedBefore = _usedBytes;
			if(minSize < 0 || maxSize < minSize)
				throw new IllegalArgumentException();
			long free = _allowedBytes - _usedBytes;
			if(free >= minSize) {
				allow = Math.min(free, maxSize);
				_usedBytes += allow;
				updates = rebalance(false);
			}
			allowanceCount = _allowances.size();
		}
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[BROKER-REQUEST] allowance=" + dbgId(allowance) + " min=" + minSize + " max=" + maxSize
				+ " granted=" + allow + " used=" + usedBefore + "->" + _usedBytes
				+ " allowances=" + allowanceCount);
		if(updates != null)
			applyTargetUpdates(updates);
		return allow;
	}

	private void runReclaim() {
		Statistics.incrementOOCMemoryReclaimRun();
		long nanos = System.nanoTime();
		try {
			long reclaimed = 0;
			for(MemoryAllowance allowance : _allowances)
				if(!allowance.isShutdown())
					reclaimed += allowance.reclaimUnused();
			Statistics.accumulateOOCMemoryReclaimBytes(reclaimed);
			if(reclaimed == 0)
				return;

			List<TargetUpdate> updates;
			long usedBefore;
			synchronized(this) {
				usedBefore = _usedBytes;
				_usedBytes = Math.max(0, _usedBytes - reclaimed);
				updates = rebalanceAfterFree();
			}
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[BROKER-RECLAIM] reclaimed=" + reclaimed + " used=" + usedBefore + "->" + _usedBytes);
			if(updates != null)
				applyTargetUpdates(updates);
			notifyReservationWaiters();
		}
		finally {
			Statistics.accumulateOOCMemoryReclaimTime(System.nanoTime() - nanos);
			if(shouldRetryReclaim())
				RECLAIM_EXECUTOR.schedule(this::runReclaim, RECLAIM_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
			else {
				_reclaimRunning.set(false);
				if(shouldRetryReclaim() && _reclaimRunning.compareAndSet(false, true))
					RECLAIM_EXECUTOR.execute(this::runReclaim);
			}
		}
	}

	private boolean shouldRetryReclaim() {
		if(!hasReclaimPressure())
			return false;
		for(MemoryAllowance allowance : _allowances) {
			if(allowance instanceof SyncMemoryAllowance sync && sync.hasReservationWaiters())
				return true;
		}
		return false;
	}

	private synchronized boolean hasReclaimPressure() {
		return _usedBytes >= _allowedBytes * RECLAIM_PRESSURE;
	}

	@Override
	public void freeMemory(MemoryAllowance allowance, long freedMemory) {
		List<TargetUpdate> updates = null;
		boolean notifyWaiters;
		long usedBefore;
		int allowanceCount;
		synchronized(this) {
			if(freedMemory < 0)
				throw new IllegalArgumentException();
			usedBefore = _usedBytes;
			_usedBytes -= freedMemory;
			updates = rebalanceAfterFree();
			notifyWaiters = freedMemory > 0;
			allowanceCount = _allowances.size();
		}
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[BROKER-FREE] allowance=" + dbgId(allowance) + " freed=" + freedMemory
				+ " used=" + usedBefore + "->" + _usedBytes + " allowances=" + allowanceCount);
		if(updates != null)
			applyTargetUpdates(updates);
		if(notifyWaiters)
			notifyReservationWaiters();
	}

	@Override
	public void shutdownAllowance(MemoryAllowance allowance) {
		List<TargetUpdate> updates;
		synchronized(this) {
			updates = rebalance(true);
		}
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[BROKER-SHUTDOWN] allowance=" + dbgId(allowance) + " used=" + _usedBytes
				+ " allowances=" + _allowances.size());
		applyTargetUpdates(updates);
		notifyReservationWaiters();
	}

	@Override
	public void destroyAllowance(MemoryAllowance allowance, long freedMemory) {
		List<TargetUpdate> updates;
		long usedBefore;
		int allowanceCount;
		synchronized(this) {
			if(freedMemory < 0)
				throw new IllegalArgumentException();
			usedBefore = _usedBytes;
			_allowances.remove(allowance);
			_usedBytes -= freedMemory;
			updates = rebalance(true);
			allowanceCount = _allowances.size();
		}
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[BROKER-DESTROY] allowance=" + dbgId(allowance) + " freed=" + freedMemory
				+ " used=" + usedBefore + "->" + _usedBytes + " allowances=" + allowanceCount);
		applyTargetUpdates(updates);
		notifyReservationWaiters();
	}

	@Override
	public synchronized void attachAllowance(MemoryAllowance allowance) {
		_allowances.add(allowance);
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[BROKER-ATTACH] allowance=" + dbgId(allowance) + " allowances=" + _allowances.size()
				+ " used=" + _usedBytes + " allowed=" + _allowedBytes);
		allowance.setTargetMemory(_allowedBytes);
	}

	@Override
	public void reservationBlocked(MemoryAllowance allowance, long bytes) {
		if(_reclaimRunning.compareAndSet(false, true))
			RECLAIM_EXECUTOR.execute(this::runReclaim);
	}

	public void shutdownAllAllowances() {
		List<MemoryAllowance> snapshot;
		synchronized(this) {
			snapshot = new ArrayList<>(_allowances);
		}
		for(MemoryAllowance allowance : snapshot)
			allowance.shutdown();
		warnOutstandingShutdownAllowances(snapshot);
	}

	private static void warnOutstandingShutdownAllowances(List<MemoryAllowance> allowances) {
		int count = 0;
		long used = 0;
		long granted = 0;
		StringBuilder details = new StringBuilder();
		for(MemoryAllowance allowance : allowances) {
			long allowanceUsed = allowance.getUsedMemory();
			long allowanceGranted = allowance.getGrantedMemory();
			if(allowanceUsed == 0 && allowanceGranted == 0)
				continue;

			count++;
			used += allowanceUsed;
			granted += allowanceGranted;
			details.append("[WARN]   ")
				.append(dbgId(allowance))
				.append(" owners=").append(debugOwners(allowance))
				.append(" used=").append(allowanceUsed)
				.append(" granted=").append(allowanceGranted)
				.append(" target=").append(allowance.getTargetMemory())
				.append(" minimum=").append(allowance.getMinimumOperatingMemory())
				.append(" shutdown=").append(allowance.isShutdown())
				.append('\n');
		}
		if(count == 0)
			return;

		System.out.println("[WARN] OOC memory allowance shutdown left live memory: count=" + count
			+ ", used=" + used + ", granted=" + granted);
		System.out.print(details);
	}

	public synchronized boolean hasActiveAllowances() {
		for(MemoryAllowance allowance : _allowances)
			if(!allowance.isShutdown())
				return true;
		return false;
	}

	private List<TargetUpdate> rebalance(boolean force) {
		long free = _allowedBytes - _usedBytes;
		if(force)
			_brokerMode = null;
		if(free > _allowedBytes / 5)
			return switchBrokerMode(BrokerMode.RELAXED);
		else
			return switchBrokerMode(BrokerMode.STRICT);
	}

	private List<TargetUpdate> rebalanceAfterFree() {
		long free = _allowedBytes - _usedBytes;
		if(_brokerMode == BrokerMode.RELAXED && free > _allowedBytes / 5)
			return rebalanceToRelaxed();
		return rebalance(false);
	}

	private List<TargetUpdate> switchBrokerMode(BrokerMode newMode) {
		if(newMode == _brokerMode)
			return null;
		List<TargetUpdate> updates = switch(newMode) {
			case STRICT -> rebalanceToStrict();
			case RELAXED -> rebalanceToRelaxed();
			default -> throw new IllegalStateException("Unsupported broker mode " + newMode);
		};
		_brokerMode = newMode;
		return updates;
	}

	private List<TargetUpdate> rebalanceToStrict() {
		List<TargetUpdate> updates = new ArrayList<>();
		long share = getEqualShare();
		for(MemoryAllowance allowance : _allowances) {
			if(allowance.isShutdown())
				continue;
			if(allowance.getUsedMemory() > share) {
				updates.add(new TargetUpdate(allowance,
					Math.min(allowance.getTargetMemory(), share + (long) ((allowance.getUsedMemory() - share) * 0.9))));
			}
		}
		return updates;
	}

	private List<TargetUpdate> rebalanceToRelaxed() {
		List<TargetUpdate> updates = new ArrayList<>();
		long free = _allowedBytes - _usedBytes;
		for(MemoryAllowance allowance : _allowances) {
			if(allowance.isShutdown())
				continue;
			updates.add(new TargetUpdate(allowance, allowance.getGrantedMemory() + free));
		}
		return updates;
	}

	private long getEqualShare() {
		int active = getActiveAllowanceCount();
		return active == 0 ? _allowedBytes : _allowedBytes / active;
	}

	private int getActiveAllowanceCount() {
		int active = 0;
		for(MemoryAllowance allowance : _allowances)
			if(!allowance.isShutdown())
				active++;
		return active;
	}

	private static void applyTargetUpdates(List<TargetUpdate> updates) {
		for(TargetUpdate update : updates)
			update._allowance.setTargetMemory(update._target);
	}

	private void notifyReservationWaiters() {
		for(MemoryAllowance allowance : _allowances)
			if(allowance instanceof SyncMemoryAllowance sync)
				sync.onBrokerMemoryAvailable();
	}

	private static String dbgId(MemoryAllowance allowance) {
		return allowance.getClass().getSimpleName() + "@" + System.identityHashCode(allowance);
	}

	private static String debugOwners(MemoryAllowance allowance) {
		return allowance instanceof SyncMemoryAllowance sync ? sync.getDebugOwners() : "unregistered";
	}

	public synchronized boolean hasOutstandingUsage() {
		if(_usedBytes != 0)
			return true;
		for(MemoryAllowance allowance : _allowances) {
			if(allowance.getUsedMemory() != 0 || allowance.getGrantedMemory() != 0)
				return true;
		}
		return false;
	}
}
