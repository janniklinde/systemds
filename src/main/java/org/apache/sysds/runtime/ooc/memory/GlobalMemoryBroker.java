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

import org.apache.sysds.utils.Statistics;

import java.util.ArrayList;
import java.util.Comparator;
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
	private static final GlobalMemoryBroker SOURCE_BROKER = new GlobalMemoryBroker(
		Math.min(100L*1024*1024, Runtime.getRuntime().maxMemory() / 10));

	public static GlobalMemoryBroker get() {
		return BROKER;
	}

	public static GlobalMemoryBroker getSource() {
		return SOURCE_BROKER;
	}

	public long getAllowedMemory() {
		return _allowedBytes;
	}

	public synchronized long getUsedMemory() {
		return _usedBytes;
	}

	/**
	 * Snapshot of every allowance holding memory, largest grant first. Answers the question the per-primitive dump
	 * cannot: which allowances the broker granted its memory to, including those of primitives no longer live.
	 */
	public String describeAllowances() {
		List<MemoryAllowance> holders = new ArrayList<>();
		for(MemoryAllowance allowance : _allowances)
			if(allowance.getGrantedMemory() > 0)
				holders.add(allowance);
		holders.sort(Comparator.comparingLong(MemoryAllowance::getGrantedMemory).reversed());
		StringBuilder sb = new StringBuilder(holders.size() + " allowance(s) holding, of " + _allowances.size()
			+ " attached; reclaimerArmed=" + _reclaimRunning.get() + ':');
		for(MemoryAllowance allowance : holders)
			sb.append("\n   ").append(allowance.getClass().getSimpleName()).append('@')
				.append(System.identityHashCode(allowance)).append(" used=").append(allowance.getUsedMemory())
				.append(" granted=").append(allowance.getGrantedMemory()).append(" target=")
				.append(allowance.getTargetMemory()).append(" shutdown=").append(allowance.isShutdown());
		return sb.toString();
	}

	private final long _allowedBytes;
	private final CopyOnWriteArrayList<MemoryAllowance> _allowances;
	private final AtomicBoolean _reclaimRunning;
	private long _usedBytes;
	private BrokerMode _brokerMode;

	private static ScheduledThreadPoolExecutor createReclaimExecutor() {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
			Thread thread = new Thread(runnable, "ooc-memory-broker-reclaimer");
			thread.setDaemon(true);
			return thread;
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
		long allow = 0;
		boolean modeChanged;
		synchronized(this) {
			if(minSize < 0 || maxSize < minSize)
				throw new IllegalArgumentException();
			long free = _allowedBytes - _usedBytes;
			if(free >= minSize && (_brokerMode != BrokerMode.STRICT || allowance.getUsedMemory() < getEqualShare())) {
				long ceiling = Math.max(allowance.getTargetMemory(), allowance.getUsedMemory() + minSize);
				long grantHeadroom = Math.max(0, ceiling - allowance.getGrantedMemory());
				allow = Math.min(Math.min(free, maxSize), grantHeadroom);
				_usedBytes += allow;
			}
			modeChanged = updateMode();
		}
		if(modeChanged)
			notifyReservationWaiters();
		return allow;
	}

	@Override
	public void freeMemory(MemoryAllowance allowance, long freedMemory) {
		boolean modeChanged;
		synchronized(this) {
			if(freedMemory < 0)
				throw new IllegalArgumentException();
			_usedBytes -= freedMemory;
			modeChanged = updateMode();
		}
		if(freedMemory > 0 || modeChanged)
			notifyReservationWaiters();
	}

	@Override
	public void shutdownAllowance(MemoryAllowance allowance) {
		notifyReservationWaiters();
	}

	@Override
	public void destroyAllowance(MemoryAllowance allowance, long freedMemory) {
		synchronized(this) {
			if(freedMemory < 0)
				throw new IllegalArgumentException();
			_allowances.remove(allowance);
			_usedBytes -= freedMemory;
			updateMode();
		}
		notifyReservationWaiters();
	}

	@Override
	public synchronized void attachAllowance(MemoryAllowance allowance) {
		_allowances.add(allowance);
		allowance.setTargetMemory(_allowedBytes);
	}

	@Override
	public void reservationBlocked(MemoryAllowance allowance, long bytes) {
		if(_reclaimRunning.compareAndSet(false, true))
			RECLAIM_EXECUTOR.execute(this::runReclaim);
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

			synchronized(this) {
				_usedBytes = Math.max(0, _usedBytes - reclaimed);
				updateMode();
			}
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

	private boolean updateMode() {
		long free = _allowedBytes - _usedBytes;
		BrokerMode newMode = free > _allowedBytes / 5 ? BrokerMode.RELAXED : BrokerMode.STRICT;
		if(newMode == _brokerMode)
			return false;
		_brokerMode = newMode;
		return true;
	}

	@Override
	public synchronized boolean isStrictMode() {
		return _brokerMode == BrokerMode.STRICT;
	}

	@Override
	public synchronized long getFairShare() {
		return getEqualShare();
	}

	private long getEqualShare() {
		int active = 0;
		for(MemoryAllowance allowance : _allowances)
			if(!allowance.isShutdown())
				active++;
		return active == 0 ? _allowedBytes : _allowedBytes / active;
	}

	private void notifyReservationWaiters() {
		for(MemoryAllowance allowance : _allowances) {
			if(allowance instanceof SyncMemoryAllowance sync)
				sync.onBrokerMemoryAvailable();
		}
	}
}
