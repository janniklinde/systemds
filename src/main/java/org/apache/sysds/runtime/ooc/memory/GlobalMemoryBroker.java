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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class GlobalMemoryBroker implements MemoryBroker {
	private enum BrokerMode {
		RELAXED, STRICT
	}

	private static final GlobalMemoryBroker BROKER = new GlobalMemoryBroker(Runtime.getRuntime().maxMemory() / 3);

	public static GlobalMemoryBroker get() {
		return BROKER;
	}

	private final long _allowedBytes;
	private final List<MemoryAllowance> _allowances;
	private final LinkedList<MemoryAllowance> _overconsumers;
	private long _usedBytes;
	private BrokerMode _brokerMode;

	private record TargetUpdate(MemoryAllowance _allowance, long _target) {}

	public GlobalMemoryBroker(long allowedBytes) {
		_allowedBytes = allowedBytes;
		_usedBytes = 0;
		_allowances = new ArrayList<>();
		_overconsumers = new LinkedList<>();
	}

	@Override
	public long requestMemory(MemoryAllowance allowance, long minSize, long maxSize) {
		List<TargetUpdate> updates = null;
		long allow = 0;
		long usedBefore;
		synchronized(this) {
			usedBefore = _usedBytes;
			if(minSize < 0 || maxSize < minSize)
				throw new IllegalArgumentException();
			long share = getEqualShare();
			long free = _allowedBytes - _usedBytes;
			if(free < minSize) {
				if(allowance.getGrantedMemory() > share && allowance.getTargetMemory() > allowance.getGrantedMemory())
					updates = List.of(new TargetUpdate(allowance, allowance.getUsedMemory()));
				else {
					MemoryAllowance largestConsumer = findAndRemoveLargestConsumer();
					if(largestConsumer != null) {
						long newTarget = (long) (largestConsumer.getGrantedMemory() * 0.8);
						if(newTarget <= share)
							newTarget = share;
						else
							addOverconsumer(largestConsumer);
						updates = List.of(new TargetUpdate(largestConsumer, newTarget));
					}
				}
			}
			else {
				allow = Math.min(free, maxSize);
				_usedBytes += allow;
				updates = rebalance(false);
				if(allowance.getGrantedMemory() <= share && allowance.getGrantedMemory() + allow > share)
					addOverconsumer(allowance);
			}
		}
		System.out.println("[BROKER-REQUEST] allowance=" + dbgId(allowance) + " min=" + minSize + " max=" + maxSize
			+ " granted=" + allow + " used=" + usedBefore + "->" + _usedBytes
			+ " allowances=" + _allowances.size() + " overconsumers=" + _overconsumers.size());
		if(updates != null)
			applyTargetUpdates(updates);
		return allow;
	}

	private MemoryAllowance findAndRemoveLargestConsumer() {
		long largest = Long.MIN_VALUE;
		MemoryAllowance allowance = null;
		for(MemoryAllowance largestConsumer : _overconsumers) {
			if(largestConsumer.getGrantedMemory() > largest) {
				largest = largestConsumer.getGrantedMemory();
				allowance = largestConsumer;
			}
		}
		_overconsumers.remove(allowance);
		return allowance;
	}

	@Override
	public void freeMemory(MemoryAllowance allowance, long freedMemory) {
		List<TargetUpdate> updates = null;
		long usedBefore;
		synchronized(this) {
			if(freedMemory < 0)
				throw new IllegalArgumentException();
			usedBefore = _usedBytes;
			_usedBytes -= freedMemory;
			if(allowance.isShutdown())
				updates = rebalance(false);
			long share = getEqualShare();
			if(allowance.getGrantedMemory() <= share && allowance.getGrantedMemory() + freedMemory > share)
				_overconsumers.remove(allowance);
			else if(allowance.getGrantedMemory() <= allowance.getTargetMemory() && allowance.getGrantedMemory() > share)
				addOverconsumer(allowance);
		}
		System.out.println("[BROKER-FREE] allowance=" + dbgId(allowance) + " freed=" + freedMemory
			+ " used=" + usedBefore + "->" + _usedBytes + " allowances=" + _allowances.size()
			+ " overconsumers=" + _overconsumers.size());
		if(updates != null)
			applyTargetUpdates(updates);
	}

	@Override
	public void shutdownAllowance(MemoryAllowance allowance) {
		List<TargetUpdate> updates;
		synchronized(this) {
			_overconsumers.remove(allowance);
			updates = rebalance(true);
		}
		System.out.println("[BROKER-SHUTDOWN] allowance=" + dbgId(allowance) + " used=" + _usedBytes
			+ " allowances=" + _allowances.size() + " overconsumers=" + _overconsumers.size());
		applyTargetUpdates(updates);
	}

	@Override
	public void destroyAllowance(MemoryAllowance allowance, long freedMemory) {
		List<TargetUpdate> updates;
		long usedBefore;
		synchronized(this) {
			if(freedMemory < 0)
				throw new IllegalArgumentException();
			usedBefore = _usedBytes;
			_allowances.remove(allowance);
			_overconsumers.remove(allowance);
			_usedBytes -= freedMemory;
			updates = rebalance(true);
		}
		System.out.println("[BROKER-DESTROY] allowance=" + dbgId(allowance) + " freed=" + freedMemory
			+ " used=" + usedBefore + "->" + _usedBytes + " allowances=" + _allowances.size()
			+ " overconsumers=" + _overconsumers.size());
		applyTargetUpdates(updates);
	}

	@Override
	public synchronized void attachAllowance(MemoryAllowance allowance) {
		_allowances.add(allowance);
		System.out.println("[BROKER-ATTACH] allowance=" + dbgId(allowance) + " allowances=" + _allowances.size()
			+ " used=" + _usedBytes + " allowed=" + _allowedBytes);
		allowance.setTargetMemory(_allowedBytes);
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
		refreshOverconsumers(updates);
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
		refreshOverconsumers(updates);
		return updates;
	}

	private long getEqualShare() {
		return _allowances.isEmpty() ? _allowedBytes : _allowedBytes / _allowances.size();
	}

	private void addOverconsumer(MemoryAllowance allowance) {
		if(!_overconsumers.contains(allowance))
			_overconsumers.add(allowance);
	}

	private void refreshOverconsumers(List<TargetUpdate> updates) {
		_overconsumers.clear();
		long share = getEqualShare();
		for(MemoryAllowance allowance : _allowances) {
			if(allowance.isShutdown())
				continue;
			long target = allowance.getTargetMemory();
			for(TargetUpdate update : updates) {
				if(update._allowance == allowance) {
					target = update._target;
					break;
				}
			}
			if(allowance.getGrantedMemory() > share && allowance.getGrantedMemory() <= target)
				_overconsumers.add(allowance);
		}
	}

	private static void applyTargetUpdates(List<TargetUpdate> updates) {
		for(TargetUpdate update : updates)
			update._allowance.setTargetMemory(update._target);
	}

	private static String dbgId(MemoryAllowance allowance) {
		return allowance.getClass().getSimpleName() + "@" + System.identityHashCode(allowance);
	}

	public synchronized String dumpOutstandingAllowances() {
		StringBuilder sb = new StringBuilder();
		sb.append("GlobalMemoryBroker used=").append(_usedBytes)
			.append(" allowed=").append(_allowedBytes)
			.append(" allowances=").append(_allowances.size())
			.append(" overconsumers=").append(_overconsumers.size())
			.append('\n');
		for(MemoryAllowance allowance : _allowances) {
			long used = allowance.getUsedMemory();
			long granted = allowance.getGrantedMemory();
			long target = allowance.getTargetMemory();
			if(used == 0 && granted == 0)
				continue;
			sb.append("  allowance=").append(dbgId(allowance))
				.append(" used=").append(used)
				.append(" granted=").append(granted)
				.append(" target=").append(target)
				.append(" shutdown=").append(allowance.isShutdown())
				.append('\n');
		}
		return sb.toString();
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
