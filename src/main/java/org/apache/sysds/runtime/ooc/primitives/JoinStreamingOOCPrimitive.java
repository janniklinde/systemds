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

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable.InputView;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.StateTableUtils;

public class JoinStreamingOOCPrimitive<L extends SpillableObject, R extends SpillableObject, O> extends OOCPrimitive {
	private final OOCStreamable<O> _output;
	private final ToIntFunction<L> _leftKey;
	private final ToIntFunction<R> _rightKey;
	private final ToLongFunction<O> _outputSize;
	private final BiFunction<L, R, O> _operation;
	private final long _taskBytes;
	private final boolean _indexedInputs;
	private final AtomicInteger _pendingArrivals = new AtomicInteger(2);
	private final AtomicInteger _pendingMatches = new AtomicInteger(1);
	private final BitSet _leftArrived = new BitSet();
	private final BitSet _rightArrived = new BitSet();
	private StateTable<L> _left;
	private StateTable<R> _right;
	private OOCStream<Integer> _matches;
	private OOCStream<JoinWork> _ready;
	private OOCStream<O> _outputStream;
	private InputView _leftView;
	private InputView _rightView;

	public JoinStreamingOOCPrimitive(OOCStreamable<L> left, OOCStreamable<R> right, OOCStreamable<O> output,
		ToIntFunction<L> leftKey, ToIntFunction<R> rightKey, ToLongFunction<O> outputSize,
		BiFunction<L, R, O> operation, long taskBytes, StreamContext context) {
		this(left, right, output, leftKey, rightKey, outputSize, operation, taskBytes, false, context);
	}

	public JoinStreamingOOCPrimitive(OOCStreamable<L> left, OOCStreamable<R> right, OOCStreamable<O> output,
		ToIntFunction<L> leftKey, ToIntFunction<R> rightKey, ToLongFunction<O> outputSize,
		BiFunction<L, R, O> operation, long taskBytes, boolean indexedInputs, StreamContext context) {
		super(context, left, right);
		_output = output;
		_leftKey = leftKey;
		_rightKey = rightKey;
		_outputSize = outputSize;
		_operation = operation;
		_taskBytes = taskBytes;
		_indexedInputs = indexedInputs;
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.ANY;
		for(OOCPrimitive child : getChildren())
			_pattern = _pattern.fused(child.getAccessPattern());
		if(_pattern.isPlannable() && _pattern != OOCAccessPattern.ANY)
			for(OOCPrimitive child : getChildren())
				child.requestPattern(_pattern);
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = accessPattern;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(accessPattern);
	}

	@Override
	protected boolean isStreamingInput(int index) {
		return true;
	}

	@Override
	protected long getMaxTaskReservationBytes() {
		return _taskBytes;
	}

	@Override
	protected void startExecution() {
		_left = new StateTable<>();
		_right = new StateTable<>();
		DataCharacteristics dc = _output.getDataCharacteristics();
		long slots = _indexedInputs && dc != null && dc.dimsKnown() ?
			dc.getNumRowBlocks() * dc.getNumColBlocks() : 0;
		_left.addEvictionPolicy(key -> slots > 0 ? key - slots : -1);
		_right.addEvictionPolicy(key -> slots > 0 ? key - slots : -1);
		_outputStream = _output.getWriteStream();
		_matches = new SubscribableTaskQueue<>();
		_ready = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _ready);
		OOCInstructionUtils.submitCloseableOOCTasks(_ready, this::process, getContext())
			.whenComplete((ignored, error) -> {
				try {
					if(error != null)
						fail(error);
					if(!hasFailed())
						_outputStream.closeInput();
				}
				finally {
					_left.close();
					_right.close();
					if(_leftView != null)
						_leftView.close();
					if(_rightView != null)
						_rightView.close();
					onComplete();
				}
			});
		AllocatedOOCStream<Integer> admitted = new AllocatedOOCStream<>(_matches, _allowance,
			key -> _taskBytes, true);
		getContext().addInStream(_matches, admitted);
		admitted.setSubscriber(this::match);
		startInput(0, _left, _leftKey);
		startInput(1, _right, _rightKey);
	}

	private <T extends SpillableObject> void startInput(int index, StateTable<T> table, ToIntFunction<T> key) {
		if(_indexedInputs && getInput(index) instanceof MaterializedStoreStreamable source) {
			consumeInputHandle(index);
			InputView view = source.getReservedInputView();
			if(index == 0)
				_leftView = view;
			else
				_rightView = view;
			view.start(slot -> {
				MatrixIndexes indexes = view.indexes(slot);
				long cols = source.getDataCharacteristics().getNumColBlocks();
				arrived(Math.toIntExact((indexes.getRowIndex() - 1) * cols + indexes.getColumnIndex() - 1), index == 0);
			}, error -> {
				if(error != null)
					fail(error);
				finishArrival();
			});
		}
		else {
			OOCStream<T> input = getInputReadStream(index);
			getContext().addInStream(input);
			input.setSubscriber(callback -> accept(callback, table, key, index == 0));
		}
	}

	private void arrived(int key, boolean left) {
		boolean ready;
		synchronized(_leftArrived) {
			BitSet own = left ? _leftArrived : _rightArrived;
			BitSet other = left ? _rightArrived : _leftArrived;
			ready = other.get(key);
			if(ready)
				other.clear(key);
			else
				own.set(key);
		}
		if(ready)
			_matches.enqueue(key);
	}

	private <T extends SpillableObject> void accept(OOCStream.QueueCallback<T> callback, StateTable<T> table,
		ToIntFunction<T> keyFunction, boolean left) {
		if(callback.isEos() || callback.isFailure()) {
			try(callback) {
				if(callback.isFailure())
					callback.get();
			}
			catch(Throwable error) {
				fail(error);
			}
			finally {
				finishArrival();
			}
			return;
		}
		_pendingArrivals.incrementAndGet();
		try(callback) {
			int key = keyFunction.applyAsInt(callback.get());
			StateTableUtils.put(table, key, callback, _allowance);
			arrived(key, left);
		}
		catch(Throwable error) {
			fail(error);
		}
		finally {
			finishArrival();
		}
	}

	private void finishArrival() {
		if(_pendingArrivals.decrementAndGet() != 0)
			return;
		synchronized(_leftArrived) {
			if(!_leftArrived.isEmpty() || !_rightArrived.isEmpty())
				fail(new DMLRuntimeException("Join inputs contain unmatched blocks"));
		}
		if(!hasFailed())
			_matches.closeInput();
	}

	private void match(OOCStream.QueueCallback<Integer> callback) {
		if(callback.isEos() || callback.isFailure()) {
			try(callback) {
				if(callback.isFailure())
					callback.get();
			}
			catch(Throwable error) {
				fail(error);
			}
			finally {
				finishMatch();
			}
			return;
		}
		_pendingMatches.incrementAndGet();
		ReservationBudget budget = AllocatedOOCStream.detachBudget(callback).enableReuse();
		try(callback) {
			int key = callback.get();
			acquire(_leftView, _left, key, 0, budget).whenComplete((left, error) -> {
				if(error != null || left == null) {
					fail(error != null ? error : new DMLRuntimeException("Missing left join input"));
					budget.close();
					finishMatch();
					return;
				}
				try {
					acquire(_rightView, _right, key, 1, budget).whenComplete((right, failure) -> {
						try {
							if(failure != null)
								throw DMLRuntimeException.of(failure);
							if(right == null)
								throw new DMLRuntimeException("Missing right join input");
							_ready.enqueue(new JoinWork(left, right, budget, key));
						}
						catch(Throwable problem) {
							left.close();
							if(right != null)
								right.close();
							budget.close();
							fail(problem);
						}
						finally {
							finishMatch();
						}
					});
				}
				catch(Throwable failure) {
					left.close();
					budget.close();
					fail(failure);
					finishMatch();
				}
			});
		}
		catch(Throwable error) {
			budget.close();
			fail(error);
			finishMatch();
		}
	}

	private void finishMatch() {
		if(_pendingMatches.decrementAndGet() == 0 && !hasFailed())
			_ready.closeInput();
	}

	@SuppressWarnings("unchecked")
	private <T extends SpillableObject> OOCFuture<StoreLease<T>> acquire(InputView view, StateTable<T> table,
		int key, int input, ReservationBudget budget) {
		if(view == null)
			return table.take(key, budget);
		long cols = getInput(input).getDataCharacteristics().getNumColBlocks();
		return (OOCFuture<StoreLease<T>>) (OOCFuture<?>) view.acquire(key / cols + 1, key % cols + 1, budget);
	}

	private void consumed(InputView view, int key, int input) {
		if(view != null) {
			long cols = getInput(input).getDataCharacteristics().getNumColBlocks();
			view.clear(key / cols + 1, key % cols + 1);
		}
	}

	private void process(JoinWork work) {
		O value = _operation.apply(work._left.value(), work._right.value());
		long bytes = _outputSize.applyAsLong(value);
		work._budget.reserveBlocking(bytes);
		OOCStream.QueueCallback<O> callback = new InMemoryQueueCallback<>(value, null, work._budget, bytes);
		try {
			_outputStream.enqueue(callback);
			callback = null;
		}
		finally {
			if(callback != null)
				callback.close();
		}
	}

	private final class JoinWork implements AutoCloseable {
		private final StoreLease<L> _left;
		private final StoreLease<R> _right;
		private final ReservationBudget _budget;
		private final int _key;

		private JoinWork(StoreLease<L> left, StoreLease<R> right, ReservationBudget budget, int key) {
			_left = left;
			_right = right;
			_budget = budget;
			_key = key;
		}

		@Override
		public void close() {
			try {
				consumed(_leftView, _key, 0);
				consumed(_rightView, _key, 1);
				_left.close();
				_right.close();
			}
			finally {
				_budget.close();
			}
		}
	}
}
