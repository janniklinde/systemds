package org.apache.sysds.runtime.ooc.memory;

import java.util.ArrayList;
import java.util.List;

public class GlobalMemoryBroker implements MemoryBroker {
	private final long _allowedBytes;
	private long _usedBytes;
	private List<MemoryAllowance> _allowances;

	public GlobalMemoryBroker(long allowedBytes) {
		_allowedBytes = allowedBytes;
		_usedBytes = 0;
		_allowances = new ArrayList<>();
	}

	@Override
	public synchronized long requestMemory(MemoryAllowance allowance, long minSize, long maxSize) {
		long free = _allowedBytes - _usedBytes;
		if(free < minSize)
			return 0;
		long allow = Math.min(free, maxSize);
		_usedBytes += allow;
		return allow;
	}

	@Override
	public synchronized void freeMemory(MemoryAllowance allowance, long freedMemory) {
		_usedBytes -= freedMemory;
	}

	@Override
	public synchronized MemoryAllowance createAllowance(long initialGrant) {
		long grant = Math.min(initialGrant, _allowedBytes - _usedBytes);
		return new SyncMemoryAllowance(this, 0, grant, );
	}
}
