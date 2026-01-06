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
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCIOHandler;
import org.apache.sysds.runtime.ooc.stream.message.OOCStreamMessage;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.util.OOCMemoryManager;

import java.util.concurrent.ConcurrentHashMap;

public class OOCSourceStream extends SubscribableTaskQueue<IndexedMatrixValue> {
	private final ConcurrentHashMap<MatrixIndexes, OOCIOHandler.SourceBlockDescriptor> _idx;
	private final OOCMemoryManager.Allowance _memoryAllowance;

	public OOCSourceStream() {
		this._idx = new ConcurrentHashMap<>();
		this._memoryAllowance = OOCCacheManager.getMemoryManager().createAllowance(1024L * 1024L);
	}

	public void enqueue(IndexedMatrixValue value, OOCIOHandler.SourceBlockDescriptor descriptor) {
		if(descriptor == null)
			throw new IllegalArgumentException("Source descriptor must not be null");
		try {
			_memoryAllowance.acquire(descriptor.serializedSize, OOCMemoryManager.Priority.LOW);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new DMLRuntimeException("Interrupted while acquiring memory for source stream", e);
		}
		catch (IllegalStateException e) {
			throw new DMLRuntimeException("Cannot acquire memory for a closed source stream", e);
		}
		MatrixIndexes key = new MatrixIndexes(descriptor.indexes);
		_idx.put(key, descriptor);
		super.enqueue(value);
	}

	@Override
	public void enqueue(IndexedMatrixValue val) {
		throw new UnsupportedOperationException("Use enqueue(value, descriptor) for source streams");
	}

	public OOCIOHandler.SourceBlockDescriptor getDescriptor(MatrixIndexes indexes) {
		return _idx.get(indexes);
	}

	@Override
	public void messageUpstream(OOCStreamMessage msg) {
		if (msg.isCancelled())
			return;
		super.messageUpstream(msg);
	}

	@Override
	public synchronized void closeInput() {
		super.closeInput();
		_memoryAllowance.close();
	}

	@Override
	protected OOCStream.QueueCallback<IndexedMatrixValue> createCallback(IndexedMatrixValue value) {
		OOCIOHandler.SourceBlockDescriptor desc = _idx.get(value.getIndexes());
		long bytes = desc != null ? desc.serializedSize : 0;
		return new OOCStream.MemoryManagedQueueCallback<>(value, _failure, _memoryAllowance, bytes);
	}
}
