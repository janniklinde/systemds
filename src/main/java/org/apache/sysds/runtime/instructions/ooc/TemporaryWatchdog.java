package org.apache.sysds.runtime.instructions.ooc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Temporary, lightweight watchdog to help debug OOC streams/tasks that never close.
 * Keep usage explicit (register on open, close on completion); easy to remove once
 * the underlying issue is fixed.
 */
public final class TemporaryWatchdog {
	private static final ConcurrentHashMap<String, Entry> OPEN = new ConcurrentHashMap<>();
	private static final ScheduledExecutorService EXEC =
		Executors.newSingleThreadScheduledExecutor(r -> {
			Thread t = new Thread(r, "TemporaryWatchdog");
			t.setDaemon(true);
			return t;
		});

	private static final long STALE_MS = TimeUnit.SECONDS.toMillis(10);
	private static final long SCAN_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

	static {
		EXEC.scheduleAtFixedRate(TemporaryWatchdog::scan, SCAN_INTERVAL_MS, SCAN_INTERVAL_MS, TimeUnit.MILLISECONDS);
	}

	private TemporaryWatchdog() {
		// no-op
	}

	public static void registerOpen(String id, String desc) {
		registerOpen(id, desc, null);
	}

	public static void registerOpen(String id, String desc, String context) {
		OPEN.put(id, new Entry(desc, context, System.currentTimeMillis()));
	}

	public static void registerClose(String id) {
		OPEN.remove(id);
	}

	private static void scan() {
		long now = System.currentTimeMillis();
		for (Map.Entry<String, Entry> e : OPEN.entrySet()) {
			if (now - e.getValue().openedAt >= STALE_MS) {
				System.err.println("[TemporaryWatchdog] Still open after " + (now - e.getValue().openedAt) + "ms: "
					+ e.getKey() + " (" + e.getValue().desc + ")"
					+ (e.getValue().context != null ? " ctx=" + e.getValue().context : ""));
			}
		}
	}

	private static class Entry {
		final String desc;
		final String context;
		final long openedAt;

		Entry(String desc, String context, long openedAt) {
			this.desc = desc;
			this.context = context;
			this.openedAt = openedAt;
		}
	}
}
