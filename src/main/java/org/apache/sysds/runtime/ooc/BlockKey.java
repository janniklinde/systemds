package org.apache.sysds.runtime.ooc;

import org.jetbrains.annotations.NotNull;

public class BlockKey implements Comparable<BlockKey> {
	private final long _streamId;
	private final long _sequenceNumber;

	public BlockKey(long streamId, long sequenceNumber) {
		this._streamId = streamId;
		this._sequenceNumber = sequenceNumber;
	}

	@Override
	public int compareTo(@NotNull BlockKey blockKey) {
		int cmp = Long.compare(_streamId, blockKey._streamId);
		if (cmp != 0)
			return cmp;
		return Long.compare(_sequenceNumber, blockKey._sequenceNumber);
	}

	@Override
	public int hashCode() {
		return 31 * Long.hashCode(_streamId) + Long.hashCode(_sequenceNumber);
	}

	@Override
	public String toString() {
		return "BlockEntry(" + _streamId + ", " + _sequenceNumber + ")";
	}
}
