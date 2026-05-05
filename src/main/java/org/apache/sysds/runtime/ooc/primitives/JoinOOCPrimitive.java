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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class JoinOOCPrimitive extends OOCPrimitive {
	private final List<OOCStreamable<IndexedMatrixValue>> _inputStreamables;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final Function<List<MatrixBlock>, MatrixBlock> _fn;
	private final StreamContext _sc;
	private CachedAllowance _cache;

	private JoinOOCPrimitive(List<OOCPrimitive> inputPrimitives, List<OOCStreamable<IndexedMatrixValue>> inputs, OOCStreamable<IndexedMatrixValue> output, Function<List<MatrixBlock>, MatrixBlock> fn, StreamContext sc) {
		super(inputPrimitives);
		_inputStreamables = inputs;
		_outputStreamable = output;
		_fn = fn;
		_sc = sc;
	}

	public JoinOOCPrimitive(List<OOCStreamable<IndexedMatrixValue>> inputs, OOCStreamable<IndexedMatrixValue> output, Function<List<MatrixBlock>, MatrixBlock> fn, StreamContext sc) {
		this(inputs.stream().map(OOCStreamable::getPrimitive).toList(), inputs, output, fn, sc);
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return new ArrayList<>(_inputStreamables);
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStreamable);
	}

	@Override
	public boolean isMaterializationBoundary() {
		return true;
	}

	@Override
	public boolean isTileLocal() {
		return true;
	}

	@Override
	public boolean requiresCache() {
		return true;
	}

	@Override
	public void bindCache(CachedAllowance cache) {
		_cache = cache;
	}

	@Override
	public void onComplete() {
		try {
			if(_cache != null)
				_cache.shutdown();
		}
		finally {
			super.onComplete();
		}
	}

	@Override
	public void inferPatterns() {
		_pattern = OOCAccessPattern.ANY;
		for(OOCPrimitive p : getChildren()) {
			if(p.getAccessPattern() == OOCAccessPattern.UNSET)
				return;
			_pattern = _pattern.fused(p.getAccessPattern());
		}
		if(_pattern.isPlannable() && _pattern != OOCAccessPattern.ANY) {
			for(OOCPrimitive p : getChildren())
				p.requestPattern(_pattern);
		}
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = accessPattern;
		for(OOCPrimitive p : getChildren())
			p.requestPattern(accessPattern);
	}

	@Override
	public void startExecution() {
		if(_inputStreamables.size() != 2)
			throw new IllegalArgumentException();
		OOCStream<IndexedMatrixValue> l = _inputStreamables.get(0).getReadStream();
		OOCStream<IndexedMatrixValue> r = _inputStreamables.get(1).getReadStream();
		OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		OOCStream<Tuple3<OOCStream.QueueCallback<IndexedMatrixValue>, OOCStream.QueueCallback<IndexedMatrixValue>, Integer>> intermediate = new SubscribableTaskQueue<>();

		new Thread(() -> {
			try {
				long cols = OOCUtils.getNumColBlocks(r.getDataCharacteristics());
				OOCStream.QueueCallback<IndexedMatrixValue> next;
				IndexedMatrixValue nextValue;
				boolean nextLeft = true;
				AtomicInteger pendingRequests = new AtomicInteger(1);

				while(!(next = (nextLeft ? l : r).dequeueCB()).isEos()) {
					try {
						nextValue = next.get();
						long rIdx = nextValue.getIndexes().getRowIndex()-1;
						long cIdx =  nextValue.getIndexes().getColumnIndex()-1;
						int idx = (int) (rIdx * cols + cIdx);
						var future = _cache.get(idx);
						if(future.isDone()) {
							var cb = future.getNow(null);
							if(cb == null) {
								_cache.handover(next, idx);
							}
							else {
								try(cb) {
									_allowance.reserveBlocking(_allocFn.applyAsLong(nextValue.getIndexes()));
									intermediate.enqueue(nextLeft ? new Tuple3<>(next.keepOpen(), cb.keepOpen(), idx) :
										new Tuple3<>(cb.keepOpen(), next.keepOpen(), idx));
								}
							}
						}
						else {
							pendingRequests.incrementAndGet();
							final var pinned = next.keepOpen();
							final boolean isLeft = nextLeft;
							future.whenComplete((cb, err) -> {
								try {
									if(err != null)
										throw DMLRuntimeException.of(err);
									try(cb; pinned) {
										_allowance.reserveBlocking(_allocFn.applyAsLong(cb.get().getIndexes()));
										intermediate.enqueue(
											isLeft ? new Tuple3<>(pinned.keepOpen(), cb.keepOpen(), idx) :
												new Tuple3<>(cb.keepOpen(), pinned.keepOpen(), idx));
									}
								}
								catch(Throwable t) {
									failJoin(t, intermediate, out);
								}
								finally {
									if(pendingRequests.decrementAndGet() == 0)
										intermediate.closeInput();
								}
							});
						}

						nextLeft = !nextLeft;
					}
					finally {
						next.close();
					}
				}

				if(pendingRequests.decrementAndGet() == 0) {
					intermediate.closeInput();
				}
			}
			catch(Throwable t) {
				failJoin(t, intermediate, out);
			}
		}).start();

		OOCInstructionUtils.submitOOCTasks(intermediate, cb -> {
			var t = cb.get();
			var qL = t._1();
			var qR = t._2();
			try(qL; qR) {
				var imv = new IndexedMatrixValue(qL.get().getIndexes(),
					_fn.apply(List.of((MatrixBlock)qL.get().getValue(), (MatrixBlock)qR.get().getValue())));
				if(_crossBoundaries)
					out.enqueue(new InMemoryQueueCallback(imv, null, _allowance,
						_allocFn.applyAsLong(imv.getIndexes())));
				else
					out.enqueue(new OOCStream.SimpleQueueCallback<>(imv, null));
			}
			finally {
				_cache.clear(t._3());
			}
		}, _sc).thenRun(out::closeInput).exceptionally(t -> {
			out.propagateFailure(DMLRuntimeException.of(t));
			return null;
		}).thenRun(this::onComplete);
	}

	private void failJoin(Throwable t, OOCStream<?> intermediate, OOCStream<IndexedMatrixValue> out) {
		DMLRuntimeException re = DMLRuntimeException.of(t);
		if(_sc.inStreamsDefined() && _sc.outStreamsDefined())
			_sc.failAll(re);
		else {
			intermediate.propagateFailure(re);
			out.propagateFailure(re);
		}
	}
}
