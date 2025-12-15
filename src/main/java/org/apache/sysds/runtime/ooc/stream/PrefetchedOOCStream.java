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

package org.apache.sysds.runtime.ooc.stream;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.parfor.LocalTaskQueue;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;

import java.util.function.Consumer;

/**
 * A PrefetchedOOCStream is a not-thread-safe wrapper around an OOCStream
 * to improve cache access efficiency. It must be guaranteed
 * that the stream is consumed until the NO_MORE_TASKS item.
 * Note that PrefetchedOOCStream only guarantees the last dequeued item
 * to be pinned in memory.
 */
public class PrefetchedOOCStream<T> implements OOCStream<T> {
	private OOCStream<T> _stream;
	private LocalTaskQueue<OOCStream.QueueCallback<T>> _queue;
	private OOCStream.QueueCallback<T> _current;

	public PrefetchedOOCStream(OOCStream<T> stream) {
		_stream = stream;
		_queue = new LocalTaskQueue<>();
		_stream.setSubscriber(cb -> {
			try {
				_queue.enqueueTask(cb.keepOpen());
			}
			catch(InterruptedException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	public void enqueue(T t) {
		throw new UnsupportedOperationException("Prefetched OOC streams are read-only");
	}

	@Override
	public T dequeue() {
		if (_current != null)
			_current.close();

		try {
			_current = _queue.dequeueTask();
		}
		catch(InterruptedException e) {
			throw new DMLRuntimeException(e);
		}

		return _current.get();
	}

	@Override
	public void closeInput() {
		throw new UnsupportedOperationException("Prefetched OOC streams are read-only");
	}

	@Override
	public void propagateFailure(DMLRuntimeException re) {
		throw new UnsupportedOperationException("Prefetched OOC streams are read-only");
	}

	@Override
	public boolean hasStreamCache() {
		return _stream.hasStreamCache();
	}

	@Override
	public CachingStream getStreamCache() {
		return _stream.getStreamCache();
	}

	@Override
	public void setSubscriber(Consumer<QueueCallback<T>> subscriber) {
		throw new UnsupportedOperationException("Prefetched OOC stream cannot use subscribers");
	}

	@Override
	public OOCStream<T> getReadStream() {
		return this;
	}

	@Override
	public OOCStream<T> getWriteStream() {
		return _stream.getWriteStream();
	}

	@Override
	public boolean isProcessed() {
		return _queue.isProcessed();
	}
}
