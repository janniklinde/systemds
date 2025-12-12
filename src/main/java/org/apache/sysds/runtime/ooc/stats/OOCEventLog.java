package org.apache.sysds.runtime.ooc.stats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OOCEventLog {
	public static final boolean USE_OOC_EVENT_LOG = true;
	private static final int MAX_NUM_EVENTS = 100000;

	private static AtomicInteger _callerCtr = new AtomicInteger(0);
	private static ConcurrentHashMap<Integer, String> _callerNames = new ConcurrentHashMap<>();

	private static AtomicInteger _logCtr = new AtomicInteger(0);
	private static final EventType[] _eventTypes = USE_OOC_EVENT_LOG ? new  EventType[MAX_NUM_EVENTS] : null;
	private static final long[] _startTimestamps = USE_OOC_EVENT_LOG ? new long[MAX_NUM_EVENTS] : null;
	private static final long[] _endTimestamps = USE_OOC_EVENT_LOG ? new long[MAX_NUM_EVENTS] : null;
	private static final int[] _callerIds = USE_OOC_EVENT_LOG ? new int[MAX_NUM_EVENTS] : null;
	private static final long[] _threadIds =  USE_OOC_EVENT_LOG ? new long[MAX_NUM_EVENTS] : null;
	private static final long[] _data = USE_OOC_EVENT_LOG ? new long[MAX_NUM_EVENTS] : null;

	public static int registerCaller(String callerName) {
		int callerId = _callerCtr.incrementAndGet();
		_callerNames.put(callerId, callerName);
		return callerId;
	}

	public static void onComputeEvent(int callerId, long startTimestamp, long endTimestamp) {
		int idx = _logCtr.getAndIncrement();
		_eventTypes[idx] = EventType.COMPUTE;
		_startTimestamps[idx] = startTimestamp;
		_endTimestamps[idx] = endTimestamp;
		_callerIds[idx] = callerId;
		_threadIds[idx] = Thread.currentThread().getId();
	}

	public static void onDiskWriteEvent(int callerId, long startTimestamp, long endTimestamp, long size) {
		int idx = _logCtr.getAndIncrement();
		_eventTypes[idx] = EventType.DISK_WRITE;
		_startTimestamps[idx] = startTimestamp;
		_endTimestamps[idx] = endTimestamp;
		_callerIds[idx] = callerId;
		_threadIds[idx] = Thread.currentThread().getId();
		_data[idx] = size;
	}

	public static void onDiskReadEvent(int callerId, long startTimestamp, long endTimestamp, long size) {
		int idx = _logCtr.getAndIncrement();
		_eventTypes[idx] = EventType.DISK_READ;
		_startTimestamps[idx] = startTimestamp;
		_endTimestamps[idx] = endTimestamp;
		_callerIds[idx] = callerId;
		_threadIds[idx] = Thread.currentThread().getId();
		_data[idx] = size;
	}

	public static String getComputeEventsCSV() {
		return getFilteredCSV("ThreadID,CallerID,StartNanos,EndNanos\n", EventType.COMPUTE, false);
	}

	public static String getDiskReadEventsCSV() {
		return getFilteredCSV("ThreadID,CallerID,StartNanos,EndNanos,NumBytes\n", EventType.DISK_READ, true);
	}

	public static String getDiskWriteEventsCSV() {
		return getFilteredCSV("ThreadID,CallerID,StartNanos,EndNanos,NumBytes\n", EventType.DISK_WRITE, true);
	}

	private static String getFilteredCSV(String header, EventType filter, boolean data) {
		StringBuilder sb = new StringBuilder();
		sb.append(header);

		int maxIdx = _logCtr.get();
		for (int i = 0; i < maxIdx; i++) {
			if (_eventTypes[i] != filter)
				continue;
			sb.append(_threadIds[i]);
			sb.append(',');
			sb.append(_callerNames.get(_callerIds[i]));
			sb.append(',');
			sb.append(_startTimestamps[i]);
			sb.append(',');
			sb.append(_endTimestamps[i]);
			if (data) {
				sb.append(',');
				sb.append(_data[i]);
			}
			sb.append('\n');
		}

		return sb.toString();
	}

	public static void clear() {
		_callerCtr.set(0);
		_logCtr.set(0);
		_callerNames.clear();
	}

	public enum EventType {
		COMPUTE,
		DISK_WRITE,
		DISK_READ
	}
}
