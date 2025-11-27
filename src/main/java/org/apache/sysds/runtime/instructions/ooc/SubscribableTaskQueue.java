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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class SubscribableTaskQueue<T> extends LocalTaskQueue<T> implements OOCStream<T> {

	private final AtomicReference<Consumer<QueueCallback<T>>> _subscriber = new AtomicReference<>();

	private final AtomicInteger _inFlight = new AtomicInteger(0);
	private final AtomicBoolean _closed = new AtomicBoolean(false);
	private final AtomicBoolean _terminalSent = new AtomicBoolean(false);

	@Override
	public void enqueue(T t) {
		// Disallow enqueues after logical close
		if (_closed.get() || _closedInput)
			throw new DMLRuntimeException("Cannot enqueue on a closed SubscribableTaskQueue");

		// Fast path: subscriber already installed -> no monitor
		Consumer<QueueCallback<T>> s = _subscriber.get();
		if(s != null && !_closed.get()) {
			// Count this task as in-flight
			_inFlight.incrementAndGet();
			s.accept(new QueueCallback<>(t, _failure));
			onDeliveryFinished(s);
			return;
		}

		// No subscriber or we raced with close/subscriber change:
		// use the underlying queue (this is the only place we pay monitors).
		try {
			super.enqueueTask(t);
		}
		catch(InterruptedException e) {
			throw new DMLRuntimeException(e);
		}
	}

	@Override
	public T dequeue() {
		try {
			return super.dequeueTask();
		}
		catch(InterruptedException e) {
			throw new DMLRuntimeException(e);
		}
	}

	@Override
	public void closeInput() {
		// Mark logically closed first to block racing enqueues
		_closed.set(true);

		// First close the underlying queue in its own synchronized semantics
		synchronized(this) {
			if(_closedInput)
				return;
			super.closeInput();
		}

		Consumer<QueueCallback<T>> s = _subscriber.get();
		if(s == null) // No subscriber yet: NO_MORE_TASKS will be signalled on dequeue() path
			return;

		// Drain backlog (tasks queued before subscriber or before closeInput)
		drainBacklogToSubscriber(s);

		// If nothing in-flight anymore, send terminal
		trySendTerminal(s);
	}

	@Override
	public void setSubscriber(Consumer<QueueCallback<T>> subscriber) {
		if(subscriber == null)
			throw new IllegalArgumentException("Cannot set subscriber to null");

		// Install subscriber once
		if(!_subscriber.compareAndSet(null, subscriber))
			throw new DMLRuntimeException("Cannot set multiple subscribers");

		// Fail fast if queue already failed
		synchronized(this) {
			if(_failure != null)
				throw _failure;
		}

		// Drain everything currently in LocalTaskQueue._data
		drainBacklogToSubscriber(subscriber);

		// If already closed and nothing else in-flight, send NO_MORE_TASKS
		trySendTerminal(subscriber);
	}

	private void drainBacklogToSubscriber(Consumer<QueueCallback<T>> s) {
		LinkedList<T> l;
		synchronized(this) {
			// Steal the current list; LocalTaskQueue should be fine with us
			// replacing _data atomically under its lock
			l = _data;
			_data = new LinkedList<>();
		}

		int backlogSize = l.size();
		if(backlogSize == 0)
			return;

		_inFlight.addAndGet(backlogSize);
		for(T t : l) {
			s.accept(new QueueCallback<>(t, _failure));
		}
		_inFlight.addAndGet(-backlogSize);
		trySendTerminal(s);
	}

	private void onDeliveryFinished(Consumer<QueueCallback<T>> s) {
		int remaining = _inFlight.decrementAndGet();
		if(remaining == 0 && _closed.get() && s != null) {
			trySendTerminal(s);
		}
	}

	private void trySendTerminal(Consumer<QueueCallback<T>> s) {
		if(s == null || !_closed.get() || _inFlight.get() != 0)
			return;

		if(_terminalSent.compareAndSet(false, true)) {
			s.accept(new QueueCallback<>((T) LocalTaskQueue.NO_MORE_TASKS, _failure));
		}
	}

	@Override
	public void propagateFailure(DMLRuntimeException re) {
		super.propagateFailure(re);
		Consumer<QueueCallback<T>> s = _subscriber.get();
		if(s != null) {
			s.accept(new QueueCallback<>(null, re));
		}
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
