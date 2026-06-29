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

	private final long _allowedBytes;
	private final List<MemoryAllowance> _allowances;
	private final AtomicBoolean _reclaimRunning;
	private long _usedBytes;
	private BrokerMode _brokerMode;
	private final AtomicBoolean _forceReclaimRequested;

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
		_allowances = new ArrayList<>();
		_reclaimRunning = new AtomicBoolean(false);
		_forceReclaimRequested = new AtomicBoolean(false);
	}

	@Override
	public long requestMemory(MemoryAllowance allowance, long minSize, long maxSize) {
		List<TargetUpdate> updates = null;
		long allow = 0;
		long usedBefore;
		int allowanceCount;
		boolean requestReclaim;
		synchronized(this) {
			usedBefore = _usedBytes;
			if(minSize < 0 || maxSize < minSize)
				throw new IllegalArgumentException();
			long share = getEqualShare();
			long free = _allowedBytes - _usedBytes;
			requestReclaim = free < minSize || hasReclaimPressureLocked();
			if(free < minSize) {
				if(allowance.getGrantedMemory() > share && allowance.getTargetMemory() > allowance.getGrantedMemory())
					updates = List.of(new TargetUpdate(allowance, allowance.getUsedMemory()));
				else {
					MemoryAllowance largestConsumer = findLargestShrinkCandidate(share);
					if(largestConsumer != null) {
						long newTarget = (long) (largestConsumer.getGrantedMemory() * 0.8);
						if(newTarget <= share)
							newTarget = share;
						updates = List.of(new TargetUpdate(largestConsumer, newTarget));
					}
				}
			}
			else {
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
		if(requestReclaim)
			scheduleReclaimIfNeeded(allow < minSize);
		return allow;
	}

	private void scheduleReclaimIfNeeded(boolean force) {
		if(force)
			_forceReclaimRequested.set(true);
		if(!force && !hasReclaimPressure())
			return;
		if(!_reclaimRunning.compareAndSet(false, true))
			return;
		RECLAIM_EXECUTOR.execute(this::runReclaim);
	}

	private synchronized boolean hasReclaimPressure() {
		return hasReclaimPressureLocked();
	}

	private boolean hasReclaimPressureLocked() {
		return _usedBytes >= (_allowedBytes * RECLAIM_PRESSURE);
	}

	private void runReclaim() {
		Statistics.incrementOOCMemoryReclaimRun();

		long nanos = System.nanoTime();
		boolean reschedule = false;
		try {
			boolean force = _forceReclaimRequested.getAndSet(false);
			Statistics.accumulateOOCMemoryReclaimBytes(reclaimUnusedGrantedMemory(force));
			reschedule = hasReclaimPressure();
		}
		catch(Throwable t) {
			if(OOCDebug.TRACE_HOT_PATH)
				System.out.println("[BROKER-RECLAIM-ERROR] " + t.getMessage());
		}
		finally {
			if(reschedule) {
				RECLAIM_EXECUTOR.schedule(this::runReclaim, RECLAIM_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
			}
			else {
				_reclaimRunning.set(false);
				if(_forceReclaimRequested.get() || hasReclaimPressure())
					scheduleReclaimIfNeeded(_forceReclaimRequested.get());
			}
		}
		Statistics.accumulateOOCMemoryReclaimTime(System.nanoTime() - nanos);
	}

	private long reclaimUnusedGrantedMemory(boolean force) {
		List<MemoryAllowance> snapshot;
		synchronized(this) {
			if(!force && !hasReclaimPressureLocked())
				return 0;
			snapshot = new ArrayList<>(_allowances);
		}

		long reclaimed = 0;
		for(MemoryAllowance allowance : snapshot) {
			if(allowance.isShutdown())
				continue;
			reclaimed += allowance.reclaimUnused();
		}
		if(reclaimed <= 0)
			return 0;

		List<TargetUpdate> updates;
		long usedBefore;
		synchronized(this) {
			usedBefore = _usedBytes;
			_usedBytes = Math.max(0, _usedBytes - reclaimed);
			updates = rebalance(false);
		}
		if(OOCDebug.TRACE_HOT_PATH)
			System.out.println("[BROKER-RECLAIM] reclaimed=" + reclaimed + " used=" + usedBefore + "->" + _usedBytes);
		if(updates != null)
			applyTargetUpdates(updates);
		notifyReservationWaiters();
		return reclaimed;
	}

	private MemoryAllowance findLargestShrinkCandidate(long share) {
		long largest = Long.MIN_VALUE;
		MemoryAllowance allowance = null;
		for(MemoryAllowance candidate : _allowances) {
			if(candidate.isShutdown())
				continue;
			long granted = candidate.getGrantedMemory();
			if(granted > share && granted > largest) {
				largest = granted;
				allowance = candidate;
			}
		}
		return allowance;
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
			if(allowance.isShutdown())
				updates = rebalance(false);
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

	public void shutdownAllAllowances() {
		List<MemoryAllowance> snapshot;
		synchronized(this) {
			snapshot = new ArrayList<>(_allowances);
		}
		for(MemoryAllowance allowance : snapshot)
			allowance.shutdown();
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
		List<MemoryAllowance> snapshot;
		synchronized(this) {
			snapshot = new ArrayList<>(_allowances);
		}
		for(MemoryAllowance allowance : snapshot)
			if(allowance instanceof SyncMemoryAllowance sync)
				sync.onBrokerMemoryAvailable();
	}

	private static String dbgId(MemoryAllowance allowance) {
		return allowance.getClass().getSimpleName() + "@" + System.identityHashCode(allowance);
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
