package org.apache.sysds.runtime.ooc.stats;

import java.util.Map;

public class OOCEvent {
	private final long startTimestamp;
	private final long endTimestamp;
	private final int callerId;
	private final long threadId;

	public OOCEvent(int callerId, long startTimestamp, long endTimestamp) {
		this.startTimestamp = startTimestamp;
		this.endTimestamp = endTimestamp;
		this.callerId = callerId;
		this.threadId = Thread.currentThread().getId();
	}

	public void appendCSV(StringBuilder sb, Map<Integer, String> callerDict) {
		sb.append(threadId);
		sb.append(",");
		sb.append(callerDict.get(callerId));
		sb.append(",");
		sb.append(startTimestamp);
		sb.append(",");
		sb.append(endTimestamp);
		sb.append("\n");
	}
}
