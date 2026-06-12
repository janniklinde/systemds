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
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;
import org.apache.sysds.runtime.ooc.store.TableRendezvous;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class JoinOOCPrimitive extends OOCPrimitive {
	//migration toggle (TODO Step 4): the OperatorStateTable rendezvous is the default; the legacy
	//CachedAllowance path stays selectable until the full migration (Step 5) removes it
	private static volatile boolean USE_STATE_TABLE =
		Boolean.parseBoolean(System.getProperty("sysds.ooc.join.stateTable", "true"));

	private final List<OOCStreamable<IndexedMatrixValue>> _inputStreamables;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final Function<List<MatrixBlock>, MatrixBlock> _fn;
	private final StreamContext _sc;
	private CachedAllowance _cache;
	private OperatorStateTable<IndexedMatrixValue> _table;

	private JoinOOCPrimitive(List<OOCPrimitive> inputPrimitives, List<OOCStreamable<IndexedMatrixValue>> inputs, OOCStreamable<IndexedMatrixValue> output, Function<List<MatrixBlock>, MatrixBlock> fn, StreamContext sc) {
		super(inputPrimitives);
		_inputStreamables = inputs.stream().map(OOCPrimitive::reserveLazyHandle).toList();
		_outputStreamable = output;
		_fn = fn;
		_sc = sc;
	}

	public JoinOOCPrimitive(List<OOCStreamable<IndexedMatrixValue>> inputs, OOCStreamable<IndexedMatrixValue> output, Function<List<MatrixBlock>, MatrixBlock> fn, StreamContext sc) {
		this(inputs.stream().map(OOCPrimitive::safePrimitive).toList(), inputs, output, fn, sc);
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
	public boolean requiresStateTable() {
		return USE_STATE_TABLE;
	}

	@Override
	public void bindStateTable(OperatorStateTable<IndexedMatrixValue> table) {
		_table = table;
	}

	/**
	 * Selects the rendezvous backend for primitives planned AFTER this call; already compiled
	 * pipelines keep their binding. Test/migration hook only.
	 */
	public static void setUseStateTable(boolean useStateTable) {
		USE_STATE_TABLE = useStateTable;
	}

	@Override
	public void onComplete() {
		try {
			if(_table != null)
				_table.close();
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
			for(OOCPrimitive p : getChildren()) {
				if(!p.hasStartedExecution())
					p.requestPattern(_pattern);
			}
		}
		inferPatterns(getParents());
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = accessPattern;
		for(OOCPrimitive p : getChildren()) {
			if(!p.hasStartedExecution())
				p.requestPattern(accessPattern);
		}
	}

	@Override
	public void startExecution() {
		if(_inputStreamables.size() != 2)
			throw new IllegalArgumentException();
		OOCStream<IndexedMatrixValue> l = _inputStreamables.get(0).getReadStream();
		OOCStream<IndexedMatrixValue> r = _inputStreamables.get(1).getReadStream();
		OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		OOCStream<JoinWork> intermediate = new SubscribableTaskQueue<>();

		if(_table != null) {
			startTableDriver(l, r, intermediate, out);
		}
		else new Thread(() -> {
			try {
				long cols = OOCUtils.getNumColBlocks(r.getDataCharacteristics());
				OOCStream.QueueCallback<IndexedMatrixValue> next;
				IndexedMatrixValue nextValue;
				boolean nextLeft = true;
				AtomicInteger pendingRequests = new AtomicInteger(1);

					while((next = (nextLeft ? l : r).dequeueCB()) != null && !next.isEos()) {
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
									intermediate.enqueue(nextLeft ?
										new JoinWork(next.keepOpen(), cb.keepOpen(), idx, 0) :
										new JoinWork(cb.keepOpen(), next.keepOpen(), idx, 0));
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
										intermediate.enqueue(
											isLeft ? new JoinWork(pinned.keepOpen(), cb.keepOpen(), idx, 0) :
												new JoinWork(cb.keepOpen(), pinned.keepOpen(), idx, 0));
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
			var qL = t._left;
			var qR = t._right;
			long bytes = t._outputBytes;
			boolean reservationOwned = bytes > 0;
			try(qL; qR) {
				var imv = new IndexedMatrixValue(qL.get().getIndexes(),
					_fn.apply(List.of((MatrixBlock)qL.get().getValue(), (MatrixBlock)qR.get().getValue())));
				if(bytes == 0)
					bytes = _allocFn.applyAsLong(imv.getIndexes());
				if(_startsRegion && !reservationOwned) {
					_allowance.reserveBlocking(bytes);
					reservationOwned = true;
				}
				if(_crossBoundaries) {
					out.enqueue(new InMemoryQueueCallback(imv, null, _allowance, bytes));
					reservationOwned = false;
				}
				else
					out.enqueue(new OOCStream.SimpleQueueCallback<>(imv, null));
			}
			finally {
				if(reservationOwned)
					_allowance.release(bytes);
				//the table path has nothing to clear: the take already removed the slot
				if(_cache != null)
					_cache.clear(t._index);
			}
		}, _sc).thenRun(out::closeInput).exceptionally(t -> {
			out.propagateFailure(DMLRuntimeException.of(t));
			return null;
		}).thenRun(this::onComplete);
	}

	/**
	 * The rendezvous driver on the new contract: one thread alternates dequeues between both inputs
	 * (the legacy idiom), and every tile goes through {@link TableRendezvous#installOrTake} — install
	 * when the partner has not arrived, take-and-pair when it has. Both inputs share the ONE bound
	 * table (one cache stream id), so eviction sees one population. The driver keeps one worst-case
	 * output reservation prepaid; a resolved match takes that reservation and the driver replenishes
	 * it before admitting another tile.
	 */
	private void startTableDriver(OOCStream<IndexedMatrixValue> l, OOCStream<IndexedMatrixValue> r,
		OOCStream<JoinWork> intermediate,
		OOCStream<IndexedMatrixValue> out) {
		new Thread(() -> {
			OOCStream.QueueCallback<IndexedMatrixValue> next = null;
			long outputBytes = 0;
			boolean reservationOwned = false;
			try {
				long cols = OOCUtils.getNumColBlocks(r.getDataCharacteristics());
				boolean nextLeft = true;
				if(_startsRegion) {
					outputBytes = _allocFn.applyAsLong(new MatrixIndexes(1, 1));
					_allowance.reserveBlocking(outputBytes);
					reservationOwned = true;
				}

				while((next = (nextLeft ? l : r).dequeueCB()) != null && !next.isEos()) {
					IndexedMatrixValue nextValue = next.get();
					long rIdx = nextValue.getIndexes().getRowIndex() - 1;
					long cIdx = nextValue.getIndexes().getColumnIndex() - 1;
					final int idx = (int) (rIdx * cols + cIdx);
					long bytes = _allocFn.applyAsLong(nextValue.getIndexes());
					final boolean isLeft = nextLeft;
					OOCFuture<TableRendezvous.Match> rendezvous =
						TableRendezvous.installOrTake(_table, idx, next, _allowance, bytes);
					next = null; //ownership transferred to the helper
					TableRendezvous.Match match = getRendezvous(rendezvous);
					if(match != null) {
						long reservedBytes = reservationOwned ? outputBytes : 0;
						JoinWork work = isLeft ?
							new JoinWork(match.own(), match.partner(), idx, reservedBytes) :
							new JoinWork(match.partner(), match.own(), idx, reservedBytes);
						try {
							intermediate.enqueue(work);
						}
						catch(Throwable t) {
							work.closeInputs();
							throw t;
						}
						reservationOwned = false;
						if(_startsRegion) {
							_allowance.reserveBlocking(outputBytes);
							reservationOwned = true;
						}
					}
					nextLeft = !nextLeft;
				}
				if(next != null)
					next.close();
				intermediate.closeInput();
			}
			catch(Throwable t) {
				if(next != null)
					next.close();
				failJoin(t, intermediate, out);
			}
			finally {
				if(reservationOwned)
					_allowance.release(outputBytes);
			}
		}).start();
	}

	private static TableRendezvous.Match getRendezvous(OOCFuture<TableRendezvous.Match> rendezvous)
		throws InterruptedException {
		try {
			return rendezvous.get();
		}
		catch(ExecutionException ex) {
			throw DMLRuntimeException.of(ex.getCause());
		}
	}

	private static final class JoinWork {
		private final OOCStream.QueueCallback<IndexedMatrixValue> _left;
		private final OOCStream.QueueCallback<IndexedMatrixValue> _right;
		private final int _index;
		private final long _outputBytes;

		private JoinWork(OOCStream.QueueCallback<IndexedMatrixValue> left,
			OOCStream.QueueCallback<IndexedMatrixValue> right, int index, long outputBytes) {
			_left = left;
			_right = right;
			_index = index;
			_outputBytes = outputBytes;
		}

		private void closeInputs() {
			try(_left; _right) {
				// Closing both callbacks releases the matched input ownership.
			}
		}
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
