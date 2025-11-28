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

package org.apache.sysds.runtime.instructions.ooc;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.parfor.LocalTaskQueue;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class SubscribableTaskQueue<T> extends LocalTaskQueue<T> implements OOCStream<T> {

	private Consumer<QueueCallback<T>> _subscriber = null;
	private final AtomicInteger _availableCtr = new AtomicInteger(1);
	private final AtomicBoolean _closed = new AtomicBoolean(false);
	private final String _watchdogId;

	public SubscribableTaskQueue() {
		_watchdogId = "STQ-" + hashCode();
		// Capture a short context to help identify origin
		String ctx = null;
		try {
			StackTraceElement[] st = new Exception().getStackTrace();
			// Skip the first few frames (constructor, createWritableStream, etc.)
			StringBuilder sb = new StringBuilder();
			int limit = Math.min(st.length, 6);
			for (int i = 1; i < limit; i++) {
				sb.append(st[i].getClassName()).append(".").append(st[i].getMethodName())
					.append(":").append(st[i].getLineNumber());
				if (i < limit - 1)
					sb.append(" <- ");
			}
			ctx = sb.toString();
		}
		catch (Exception ignored) {
			// best-effort
		}
		TemporaryWatchdog.registerOpen(_watchdogId, "SubscribableTaskQueue@" + hashCode(), ctx);
	}

	@Override
	public void enqueue(T t) {
		if (t == NO_MORE_TASKS)
			throw new DMLRuntimeException("Cannot enqueue NO_MORE_TASKS item");

		int cnt = _availableCtr.incrementAndGet();

		if (cnt <= 1) // Then the queue was already closed and we disallow further enqueues
			throw new DMLRuntimeException("Cannot enqueue into closed SubscribableTaskQueue");

		Consumer<QueueCallback<T>> s = _subscriber;

		if (s != null) {
			s.accept(new QueueCallback<>(t, _failure));
			onDeliveryFinished();
			return;
		}

		synchronized (this) {
			if (_subscriber == null) {
				// Re-check that subscriber is really null to avoid race conditions
				try {
					super.enqueueTask(t);
				}
				catch(InterruptedException e) {
					throw new DMLRuntimeException(e);
				}
				return;
			} else {
				// Otherwise do not insert and re-schedule subscriber invokation
				s = _subscriber;
			}
		}

		// Last case if due to race a subscriber has been set
		s.accept(new QueueCallback<>(t, _failure));
		onDeliveryFinished();
	}

	@Override
	public T dequeue() {
		try {
			T deq = super.dequeueTask();
			onDeliveryFinished();
			return deq;
		}
		catch(InterruptedException e) {
			throw new DMLRuntimeException(e);
		}
	}

	@Override
	public void closeInput() {
		if (_closed.compareAndSet(false, true)) {
			System.out.println("Closing --> " + hashCode());
			super.closeInput();
			onDeliveryFinished();
		}
	}

	@Override
	public void setSubscriber(Consumer<QueueCallback<T>> subscriber) {
		if(subscriber == null)
			throw new IllegalArgumentException("Cannot set subscriber to null");

		LinkedList<T> data;

		synchronized(this) {
			if(_subscriber != null)
				throw new DMLRuntimeException("Cannot set multiple subscribers");
			_subscriber = subscriber;
			if(_failure != null)
				throw _failure;
			data = _data;
			_data = new LinkedList<>();
		}

		for (T t : data) {
			subscriber.accept(new QueueCallback<>(t, _failure));
			onDeliveryFinished();
		}

		synchronized(this) {
			// Re-check if queue hasn't seen other items in the meantime
		}
	}

	private void onDeliveryFinished() {
		int ctr = _availableCtr.decrementAndGet();

		if (ctr == 0) {
			Consumer<QueueCallback<T>> s = _subscriber;
			if (s != null)
				s.accept(new QueueCallback<>((T) LocalTaskQueue.NO_MORE_TASKS, _failure));

			TemporaryWatchdog.registerClose(_watchdogId);
		} else {
			System.out.println("Close rejected --> " + hashCode());
		}
	}

	@Override
	public synchronized void propagateFailure(DMLRuntimeException re) {
		super.propagateFailure(re);
		Consumer<QueueCallback<T>> s = _subscriber;
		if(s != null)
			s.accept(new QueueCallback<>(null, re));
	}

	@Override
	public LocalTaskQueue<T> toLocalTaskQueue() {
		return this;
	}

	@Override
	public OOCStream<T> getReadStream() {
		return this;
	}

	@Override
	public OOCStream<T> getWriteStream() {
		return this;
	}

	@Override
	public boolean hasStreamCache() {
		return false;
	}

	@Override
	public CachingStream getStreamCache() {
		return null;
	}
}
