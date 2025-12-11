package org.apache.sysds.runtime.ooc.cache;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CloseableQueue<T> {
	private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
	private final Object POISON = new Object();  // sentinel
	private volatile boolean closed = false;

	public CloseableQueue() { }

	/**
	 * Enqueue if the queue is not closed.
	 * @return false if already closed
	 */
	public boolean enqueueIfOpen(T task) throws InterruptedException {
		if (task == null)
			throw new IllegalArgumentException("null tasks not allowed");
		synchronized (this) {
			if (closed)
				return false;
			queue.put(task);
		}
		return true;
	}

	public T take() throws InterruptedException {
		if (closed && queue.isEmpty())
			return null;

		Object x = queue.take();

		if (x == POISON)
			return null;

		return (T) x;
	}

	/**
	 * Poll with max timeout.
	 * @return item, or null if:
	 *   - timeout, or
	 *   - queue has been closed and this consumer reached its poison pill
	 */
	@SuppressWarnings("unchecked")
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		if (closed && queue.isEmpty())
			return null;

		Object x = queue.poll(timeout, unit);
		if (x == null)
			return null;          // timeout

		if (x == POISON)
			return null;

		return (T) x;
	}

	/**
	 * Close queue for N consumers.
	 * Each consumer will receive exactly one poison pill and then should stop.
	 */
	public boolean close() throws InterruptedException {
		synchronized (this) {
			if (closed)
				return false;           // idempotent
			closed = true;
		}
		queue.put(POISON);
		return true;
	}

	public synchronized boolean isFinished() {
		return closed && queue.isEmpty();
	}
}
