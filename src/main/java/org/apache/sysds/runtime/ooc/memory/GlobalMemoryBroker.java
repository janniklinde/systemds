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

import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.utils.Statistics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GlobalMemoryBroker implements MemoryBroker {
	private static final long RECLAIM_RETRY_DELAY_MS = 2;
	private static final double RECLAIM_PRESSURE = 0.85;
	/**
	 * Fraction of the broker budget above which buffered callbacks are force-parked into the cache. Matches the
	 * onset of {@link BrokerMode#STRICT} (see {@link #updateMode()}): strict mode is the engine's own definition of
	 * "memory is tight", and it is where admission starts refusing, so the valve has to be armed by then.
	 */
	private static final double PURGE_PRESSURE = Double
		.parseDouble(System.getProperty("sysds.ooc.purge.pressure", "0.80"));
	private static final ScheduledThreadPoolExecutor RECLAIM_EXECUTOR = createReclaimExecutor();

	private enum BrokerMode {
		RELAXED, STRICT
	}

	private static final long MAX_BROKER_BYTES = 3L << 30; // 3 GB
	private static final GlobalMemoryBroker BROKER = new GlobalMemoryBroker(
		Math.min(MAX_BROKER_BYTES, Runtime.getRuntime().maxMemory() / 3));
	private static final GlobalMemoryBroker SOURCE_BROKER = new GlobalMemoryBroker(
		Math.min(200L*1024*1024, Runtime.getRuntime().maxMemory() / 12));

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
	private final AtomicBoolean _purgeRunning;
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
		_purgeRunning = new AtomicBoolean(false);
	}

	@Override
	public long requestMemory(MemoryAllowance allowance, long minSize, long maxSize) {
		long allow = 0;
		boolean modeChanged;
		boolean purge;
		synchronized(this) {
			if(minSize < 0 || maxSize < minSize)
				throw new IllegalArgumentException();
			long free = _allowedBytes - _usedBytes;
			if(free >= minSize && (_brokerMode != BrokerMode.STRICT || allowance.isAdmissionExempt()
				|| allowance.getUsedMemory() < getFairShareFloored(minSize))) {
				long ceiling = Math.max(allowance.getTargetMemory(), allowance.getUsedMemory() + minSize);
				long grantHeadroom = Math.max(0, ceiling - allowance.getGrantedMemory());
				allow = Math.min(Math.min(free, maxSize), grantHeadroom);
				_usedBytes += allow;
			}
			modeChanged = updateMode();
			purge = hasPurgePressure();
		}
		if(modeChanged)
			notifyReservationWaiters();
		if(purge)
			schedulePurge();
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
		if(hasPurgePressure())
			schedulePurge();
	}

	private synchronized boolean hasPurgePressure() {
		return _usedBytes >= (long) (_allowedBytes * PURGE_PRESSURE);
	}

	/**
	 * Last resort against a hard stall: force-park queue-buffered callbacks into the cache so their bytes return to
	 * the broker. Runs off the caller thread because parking releases memory, which re-enters this broker.
	 */
	private void schedulePurge() {
		boolean storeBacked = this == SOURCE_BROKER;
		if((this != BROKER && !storeBacked) || !_purgeRunning.compareAndSet(false, true))
			return;
		RECLAIM_EXECUTOR.execute(() -> {
			try {
				if(storeBacked)
					SubscribableTaskQueue.purgeBufferedStore();
				else
					SubscribableTaskQueue.purgeBuffered();
			}
			finally {
				_purgeRunning.set(false);
			}
		});
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

	@Override
	public synchronized long getFairShare(long taskBytes) {
		return getFairShareFloored(taskBytes);
	}

	/**
	 * The fair share is a cap on how far one allowance may run ahead of the others, and admission lets a request
	 * cross it (the test is {@code used < share}, not {@code used + bytes <= share}). That grace only means something
	 * while the share is at least a task: below that, the very first block already puts an allowance over its share
	 * and freezes it for good. Hold the share at one task in plus one out so every allowance can always work.
	 */
	private long getFairShareFloored(long taskBytes) {
		long floor = taskBytes > 0 && taskBytes <= Long.MAX_VALUE / 2 ? 2 * taskBytes : taskBytes;
		return Math.max(getEqualShare(), floor);
	}

	/**
	 * The strict-mode fair share, computed over the allowances that actually hold memory rather than every attached
	 * one. Most attached allowances hold nothing - a plan with 1058 allowances of which 212 hold would otherwise put
	 * the share at ~991KB and refuse every holder of a single block while the broker still has free bytes.
	 */
	private long getEqualShare() {
		int holders = 0;
		for(MemoryAllowance allowance : _allowances)
			if(!allowance.isShutdown() && allowance.getGrantedMemory() > 0)
				holders++;
		return holders == 0 ? _allowedBytes : _allowedBytes / holders;
	}

	private void notifyReservationWaiters() {
		for(MemoryAllowance allowance : _allowances) {
			if(allowance instanceof SyncMemoryAllowance sync)
				sync.onBrokerMemoryAvailable();
		}
	}
}
