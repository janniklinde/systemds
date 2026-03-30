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

public class SyncMemoryAllowance implements MemoryAllowance {
	protected final MemoryBroker _broker;
	protected long _usedBytes;
	protected long _grantedBytes;
	protected long _targetBytes;

	protected SyncMemoryAllowance(MemoryBroker broker, long used, long granted, long target) {
		_broker = broker;
		_usedBytes = used;
		_grantedBytes = granted;
		_targetBytes = target;
	}

	@Override
	public synchronized boolean tryReserve(long bytes) {
		if(_usedBytes + bytes > _targetBytes)
			return false;
		if(_usedBytes + bytes <= _grantedBytes) {
			_usedBytes += bytes;
			return true;
		}
		_grantedBytes += _broker.requestMemory(this, _usedBytes + bytes - _grantedBytes, Math.min(_grantedBytes, bytes) * 2);
		if(_usedBytes + bytes <= _grantedBytes) {
			_usedBytes += bytes;
			return true;
		}
		return false;
	}

	@Override
	public synchronized void release(long bytes) {
		_usedBytes -= bytes;
		if(_grantedBytes < _targetBytes) {
			long oldGrantedBytes = _grantedBytes;
			_grantedBytes = Math.max(_usedBytes, _targetBytes);
			_broker.freeMemory(this, oldGrantedBytes - _grantedBytes);
		}
	}

	@Override
	public long getUsedMemory() {
		return _usedBytes;
	}

	@Override
	public long getGrantedMemory() {
		return _grantedBytes;
	}

	@Override
	public long getTargetMemory() {
		return _targetBytes;
	}

	@Override
	public void setTargetMemory(long targetMemory) {
		_targetBytes = targetMemory;
		if(_grantedBytes < _targetBytes)
			_grantedBytes = _broker.requestMemory(this, 0, _targetBytes - _grantedBytes);
	}
}
