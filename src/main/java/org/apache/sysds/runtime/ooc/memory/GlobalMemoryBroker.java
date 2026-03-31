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
import java.util.PriorityQueue;

public class GlobalMemoryBroker implements MemoryBroker {
	private enum BrokerMode {
		RELAXED, STRICT;
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

	public GlobalMemoryBroker(long allowedBytes) {
		_allowedBytes = allowedBytes;
		_usedBytes = 0;
		_allowances = new ArrayList<>();
		_overconsumers = new LinkedList<>();
	}

	@Override
	public synchronized long requestMemory(MemoryAllowance allowance, long minSize, long maxSize) {
		if(minSize < 0 || maxSize < minSize)
			throw new IllegalArgumentException();
		long free = _allowedBytes - _usedBytes;
		if(free < minSize) {
			if(allowance.getGrantedMemory() > _allowedBytes / _allowances.size() && allowance.getTargetMemory() > allowance.getGrantedMemory())
				allowance.setTargetMemory(allowance.getUsedMemory());
			else {
				// Not overconsuming --> try to free memory from overconsumers
				MemoryAllowance largestConsumer = findAndRemoveLargestConsumer();
				if(largestConsumer != null) {
					long newTarget = (long)(largestConsumer.getGrantedMemory() * 0.8);
					if(newTarget > _allowedBytes / _allowances.size()) {
						_overconsumers.add(largestConsumer);
					}
					else {
						newTarget = _allowedBytes / _allowances.size();
					}
					largestConsumer.setTargetMemory(newTarget);
				}
			}
			return 0;
		}
		long allow = Math.min(free, maxSize);
		_usedBytes += allow;
		rebalance(false);
		if(allowance.getGrantedMemory() <= _allowedBytes / _allowances.size()
			&& allowance.getGrantedMemory() + allow > _allowedBytes / _allowances.size()) {
			_overconsumers.add(allowance);
		}
		return allow;
	}

	private MemoryAllowance findAndRemoveLargestConsumer() {
		long largest = Long.MIN_VALUE;
		MemoryAllowance allowance = null;
		for(MemoryAllowance largestConsumer : _overconsumers) {
			if(largestConsumer.getGrantedMemory() > largest)
				allowance = largestConsumer;
		}
		_overconsumers.remove(allowance);
		return allowance;
	}

	@Override
	public synchronized void freeMemory(MemoryAllowance allowance, long freedMemory) {
		if(freedMemory < 0)
			throw new IllegalArgumentException();
		_usedBytes -= freedMemory;
		if(allowance.getGrantedMemory() <= _allowedBytes / _allowances.size()
			&& allowance.getGrantedMemory() + freedMemory > _allowedBytes / _allowances.size()) {
			_overconsumers.remove(allowance);
		}
		else if(allowance.getGrantedMemory() <= allowance.getTargetMemory()
			&& allowance.getGrantedMemory() > _allowedBytes / _allowances.size()) {
			_overconsumers.add(allowance);
		}
	}

	@Override
	public synchronized MemoryAllowance createAllowance(long initialGrant) {
		long free =  _allowedBytes - _usedBytes;
		long grant = Math.min(initialGrant, free);
		MemoryAllowance allowance = new SyncMemoryAllowance(this, 0, grant, free);
		_allowances.add(allowance);
		rebalance(true);
		return allowance;
	}

	private synchronized void rebalance(boolean force) {
		long free = _allowedBytes - _usedBytes;
		if(force)
			_brokerMode = null;
		if(free > _allowedBytes / 5)
			switchBrokerMode(BrokerMode.RELAXED);
		else
			switchBrokerMode(BrokerMode.STRICT);
	}

	private synchronized void switchBrokerMode(BrokerMode newMode) {
		if(newMode == _brokerMode)
			return;
		switch(newMode) {
			case STRICT:
				rebalanceToStrict();
				break;
			case RELAXED:
				rebalanceToRelaxed();
				break;
		}
		_brokerMode = newMode;
	}

	private synchronized void rebalanceToStrict() {
		// Current heuristic: Disallow allocation for operators using more than equal share of memory chunks
		// Distribute remaining free allowance between under-utilized operators
		long share = _allowedBytes / _allowances.size();
		for(MemoryAllowance allowance : _allowances) {
			if(allowance.getUsedMemory() > share) {
				allowance.setTargetMemory(Math.min(allowance.getTargetMemory(), share + (long)((allowance.getUsedMemory() - share) * 0.9)));
			}
		}
	}

	private synchronized void rebalanceToRelaxed() {
		long free = _allowedBytes - _usedBytes;
		for(MemoryAllowance allowance : _allowances)
			allowance.setTargetMemory(free);
	}
}
