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

package org.apache.sysds.runtime.ooc.primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;

public final class FanoutOOCPrimitive extends OOCPrimitive implements OOCStreamable<IndexedMatrixValue> {
	private final List<OOCStream<IndexedMatrixValue>> _outputs = new ArrayList<>();
	private final AtomicBoolean _done = new AtomicBoolean();
	private final boolean _row;
	private final int _consumers;
	private CacheableData<?> _data;
	private OOCStream<IndexedMatrixValue> _source;
	private boolean _subscribed;

	public FanoutOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, boolean row, int consumers,
		StreamContext context) {
		super(context, input);
		if(consumers < 1)
			throw new DMLRuntimeException("Fanout requires at least one consumer.");
		_row = row;
		_consumers = consumers;
		_data = input.getData();
	}

	@Override
	public synchronized OOCStream<IndexedMatrixValue> getReadStream() {
		if(_outputs.size() == _consumers)
			throw new DMLRuntimeException("All fanout consumers are already registered.");
		OOCStream<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		output.setData(_data);
		output.assignPrimitive(this);
		_outputs.add(output);
		getContext().addOutStream(output);
		subscribeWhenReady();
		return output;
	}

	@Override
	protected synchronized void startExecution() {
		_source = getInputReadStream(0);
		getContext().addInStream(_source);
		subscribeWhenReady();
	}

	private void subscribeWhenReady() {
		if(_source != null && _outputs.size() == _consumers && !_subscribed) {
			_subscribed = true;
			_source.setSubscriber(this::distribute);
		}
	}

	private void distribute(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		try(callback) {
			if(_done.get())
				return;
			if(callback.isFailure())
				callback.get();
			if(callback.isEos()) {
				if(_done.compareAndSet(false, true)) {
					try {
						for(OOCStream<IndexedMatrixValue> output : _outputs)
							output.closeInput();
					}
					finally {
						onComplete();
					}
				}
				return;
			}
			if(!(callback instanceof MaterializedCallback<?>))
				throw new DMLRuntimeException("Fanout requires materialized callbacks.");
			for(OOCStream<IndexedMatrixValue> output : _outputs) {
				OOCStream.QueueCallback<IndexedMatrixValue> retained = callback.keepOpen();
				try {
					output.enqueue(retained);
				}
				catch(Throwable error) {
					retained.close();
					throw error;
				}
			}
		}
		catch(Throwable error) {
			fail(error);
			if(_done.compareAndSet(false, true))
				onComplete();
		}
	}

	@Override
	protected void inferPatternsInternal() {
		requestPatternInternal(_row ? OOCAccessPattern.ROW_MAJOR : OOCAccessPattern.COL_MAJOR);
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern pattern) {
		_pattern = _row ? OOCAccessPattern.ROW_MAJOR : OOCAccessPattern.COL_MAJOR;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(_pattern);
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return this;
	}

	@Override
	public OOCStream<IndexedMatrixValue> getWriteStream() {
		throw new UnsupportedOperationException("Fanout is read-only.");
	}

	@Override
	public boolean hasStreamCache() {
		return false;
	}

	@Override
	public CachingStream getStreamCache() {
		return null;
	}

	@Override
	public boolean isProcessed() {
		return _done.get();
	}

	@Override
	public DataCharacteristics getDataCharacteristics() {
		return _data.getDataCharacteristics();
	}

	@Override
	public CacheableData<?> getData() {
		return _data;
	}

	@Override
	public void setData(CacheableData<?> data) {
		_data = data;
	}
}
