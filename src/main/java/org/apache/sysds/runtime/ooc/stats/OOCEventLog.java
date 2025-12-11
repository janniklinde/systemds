package org.apache.sysds.runtime.ooc.stats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class OOCEventLog {
	public static final boolean USE_OOC_EVENT_LOG = true;

	private static AtomicInteger _callerCtr = new AtomicInteger(0);
	private static ConcurrentHashMap<Integer, String> _callerNames = new ConcurrentHashMap<>();
	private static ConcurrentLinkedQueue<OOCEvent> _eventLog = new ConcurrentLinkedQueue<>();

	public static int registerCaller(String callerName) {
		int callerId = _callerCtr.incrementAndGet();
		_callerNames.put(callerId, callerName);
		return callerId;
	}

	public static void onComputeEvent(int callerId, long startTimestamp, long endTimestamp) {
		_eventLog.add(new OOCEvent(callerId, startTimestamp, endTimestamp));
	}

	public static String toCSV() {
		StringBuilder sb = new StringBuilder();
		sb.append("ThreadID,CallerID,StartNanos,EndNanos\n");
		_eventLog.forEach(e -> e.appendCSV(sb, _callerNames));
		return sb.toString();
	}

	public static void clear() {
		_callerCtr.set(0);
		_callerNames.clear();
		_eventLog.clear();
	}
}
