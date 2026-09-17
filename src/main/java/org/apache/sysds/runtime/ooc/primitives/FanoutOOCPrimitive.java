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
import java.util.function.Consumer;

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
	private final List<Output> _outputs = new ArrayList<>();
	private final AtomicBoolean _done = new AtomicBoolean();
	private final boolean _row;
	private final int _consumers;
	private CacheableData<?> _data;
	private OOCStream<IndexedMatrixValue> _source;
	private boolean _subscribed;
	private boolean _planStarted;
	private Boolean _sharing;
	private Output _primary;
	private int _replayClaims;
	private int _lazyHandles;
	private int _finishedOutputs;
	private int _abandonedOutputs;
	private boolean _deleteScheduled;

	public FanoutOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, boolean row, int consumers,
		StreamContext context) {
		super(context, input);
		if(consumers < 1)
			throw new DMLRuntimeException("Fanout requires at least one consumer.");
		_row = row;
		_consumers = consumers;
		_data = input.getData();
		for(int i = 1; i < consumers; i++) {
			input.reserveLazyHandle();
			_replayClaims++;
		}
	}

	@Override
	public synchronized OOCStream<IndexedMatrixValue> getReadStream() {
		if(_outputs.size() == _consumers)
			throw new DMLRuntimeException("All fanout consumers are already registered.");
		Output output = new Output();
		output.setData(_data);
		output.assignPrimitive(this);
		_outputs.add(output);
		getContext().addOutStream(output);
		return output;
	}

	@Override
	public synchronized OOCStream<IndexedMatrixValue> getReservedReadStream() {
		if(_lazyHandles > 0)
			_lazyHandles--;
		return getReadStream();
	}

	@Override
	public synchronized void reserveLazyHandle() {
		_lazyHandles++;
	}

	@Override
	public synchronized void discardHandle() {
		if(_lazyHandles > 0)
			_lazyHandles--;
		if(_deleteScheduled)
			releaseUnusedClaims();
	}

	@Override
	public boolean hasMaterializedStore() {
		return true;
	}

	@Override
	public synchronized void scheduleMaterializedStoreDeletion() {
		_deleteScheduled = true;
		releaseUnusedClaims();
	}

	private void releaseUnusedClaims() {
		int unused = _consumers - _outputs.size() - _lazyHandles - _abandonedOutputs;
		for(int i = 0; i < unused; i++) {
			if(_replayClaims > 0) {
				getInput(0).discardHandle();
				_replayClaims--;
			}
			_abandonedOutputs++;
		}
		if(_finishedOutputs + _abandonedOutputs == _consumers && _done.compareAndSet(false, true)) {
			if(hasStartedExecution())
				onComplete();
			else
				discardInputHandle(0);
		}
	}

	@Override
	protected synchronized void startExecution() {
		_source = getInputReadStream(0);
		getContext().addInStream(_source);
		subscribeWhenReady();
	}

	private void subscribeWhenReady() {
		if(_planStarted && _source != null && _primary != null && !_subscribed) {
			_subscribed = true;
			_source.setSubscriber(this::distribute);
		}
	}

	@Override
	public synchronized void onPlanStarted() {
		_planStarted = true;
		subscribeWhenReady();
	}

	private void distribute(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		try {
			List<Output> replay = new ArrayList<>();
			synchronized(this) {
				if(_sharing == null) {
					_sharing = _outputs.size() == _consumers && _outputs.stream().allMatch(output -> output._active);
					if(_sharing) {
						while(_replayClaims > 0) {
							getInput(0).discardHandle();
							_replayClaims--;
						}
					}
					else {
						for(Output output : _outputs)
							if(output._active && output != _primary)
								replay.add(output);
					}
				}
			}
			for(Output output : replay)
				output.openReplay();
			forward(_sharing ? _outputs : List.of(_primary), callback);
		}
		catch(Throwable error) {
			callback.close();
			finishFailure(error);
		}
	}

	private void forward(List<Output> outputs, OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		try(callback) {
			if(_done.get())
				return;
			if(callback.isFailure())
				callback.get();
			if(callback.isEos()) {
				for(Output output : outputs)
					output.closeInput();
				synchronized(this) {
					_finishedOutputs += outputs.size();
					if(_finishedOutputs + _abandonedOutputs == _consumers && _done.compareAndSet(false, true))
						onComplete();
				}
			}
			else {
				if(!(callback instanceof MaterializedCallback<?>))
					throw new DMLRuntimeException("Fanout requires materialized callbacks.");
				for(Output output : outputs) {
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
		}
		catch(Throwable error) {
			finishFailure(error);
		}
	}

	private void finishFailure(Throwable error) {
		fail(error);
		synchronized(this) {
			while(_replayClaims > 0) {
				getInput(0).discardHandle();
				_replayClaims--;
			}
		}
		if(_done.compareAndSet(false, true))
			onComplete();
	}

	private final class Output extends SubscribableTaskQueue<IndexedMatrixValue> {
		private boolean _active;
		private boolean _replaying;

		@Override
		public void setSubscriber(Consumer<QueueCallback<IndexedMatrixValue>> subscriber) {
			super.setSubscriber(subscriber);
			activate();
		}

		@Override
		public IndexedMatrixValue dequeue() {
			activate();
			return super.dequeue();
		}

		@Override
		public QueueCallback<IndexedMatrixValue> dequeueCB() {
			activate();
			return super.dequeueCB();
		}

		private void activate() {
			boolean replay;
			synchronized(FanoutOOCPrimitive.this) {
				if(_active)
					return;
				_active = true;
				if(_primary == null)
					_primary = this;
				replay = Boolean.FALSE.equals(_sharing) && this != _primary;
				subscribeWhenReady();
			}
			if(replay)
				openReplay();
		}

		private void openReplay() {
			OOCStream<IndexedMatrixValue> reader;
			synchronized(FanoutOOCPrimitive.this) {
				if(_replaying)
					return;
				_replaying = true;
				_replayClaims--;
				reader = (OOCStream<IndexedMatrixValue>) getInput(0).getReservedReadStream();
			}
			reader.setSubscriber(callback -> forward(List.of(this), callback));
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
