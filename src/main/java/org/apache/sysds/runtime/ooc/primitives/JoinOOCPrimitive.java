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
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.JoinTable;
import org.apache.sysds.runtime.ooc.stream.OwnedQueueCallback;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class JoinOOCPrimitive extends OOCPrimitive {
	private final List<OOCStreamable<IndexedMatrixValue>> _inputStreamables;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final Function<List<MatrixBlock>, MatrixBlock> _fn;
	private final StreamContext _sc;
	private StateTable<IndexedMatrixValue> _table;
	private volatile long _policyRows;
	private volatile long _policyCols;

	private JoinOOCPrimitive(List<OOCPrimitive> inputPrimitives, List<OOCStreamable<IndexedMatrixValue>> inputs,
		OOCStreamable<IndexedMatrixValue> output, Function<List<MatrixBlock>, MatrixBlock> fn, StreamContext sc) {
		super(inputPrimitives);
		_inputStreamables = inputs.stream().map(OOCPrimitive::reserveLazyHandle).toList();
		_outputStreamable = output;
		_fn = fn;
		_sc = sc;
	}

	public JoinOOCPrimitive(List<OOCStreamable<IndexedMatrixValue>> inputs, OOCStreamable<IndexedMatrixValue> output,
		Function<List<MatrixBlock>, MatrixBlock> fn, StreamContext sc) {
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
		return false;
	}

	@Override
	public long getMinimumOperatingMemoryFactor() {
		return 3;
	}

	@Override
	public long getMinimumOperatingMemoryBytes() {
		OOCStreamable<IndexedMatrixValue> leftInput = _inputStreamables.get(0);
		OOCStreamable<IndexedMatrixValue> rightInput = _inputStreamables.get(1);
		long tableTileBytes = joinTableTileBytes(leftInput, rightInput);
		long outputBudgetBytes = Math.max(tableTileBytes, OOCInstructionUtils.estimateOutputTileBytes(
			_outputStreamable.getDataCharacteristics()));
		return saturatingAdd(outputBudgetBytes, saturatingAdd(tableTileBytes, tableTileBytes));
	}

	@Override
	public void onComplete() {
		try {
			if(_table != null)
				_table.close();
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
		_table = new StateTable<>(OOCCacheManager.getGlobalCache(), CachingStream._streamSeq.getNextID());
		_table.addEvictionPolicy(this::scoreTableSlot);
		OOCStreamable<IndexedMatrixValue> leftInput = _inputStreamables.get(0);
		OOCStreamable<IndexedMatrixValue> rightInput = _inputStreamables.get(1);
		_policyRows = OOCUtils.getNumRowBlocks(rightInput.getDataCharacteristics());
		_policyCols = OOCUtils.getNumColBlocks(rightInput.getDataCharacteristics());
		addMaterializedInputPolicy(leftInput);
		addMaterializedInputPolicy(rightInput);
		OOCStream<IndexedMatrixValue> l = leftInput.getReadStream();
		OOCStream<IndexedMatrixValue> r = rightInput.getReadStream();
		OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		OOCStream<JoinWork> intermediate = new SubscribableTaskQueue<>();
		long tableTileBytes = joinTableTileBytes(leftInput, rightInput);
		long outputBudgetBytes = Math.max(tableTileBytes, OOCInstructionUtils.estimateOutputTileBytes(
			_outputStreamable.getDataCharacteristics()));

		startTableDriver(l, r, intermediate, out, tableTileBytes, outputBudgetBytes);

		OOCInstructionUtils.submitOOCTasks(intermediate, cb -> {
			try(cb) {
				var t = cb.get();
				var qL = t._left;
				var qR = t._right;
				var imv = new IndexedMatrixValue(qL.get().getIndexes(),
					_fn.apply(List.of((MatrixBlock) qL.get().getValue(), (MatrixBlock) qR.get().getValue())));
				OOCInstructionUtils.enqueueExact(out, imv, t._budget);
			}
		}, cb -> true, (i, cb) -> cb.get().close(), _sc).thenRun(out::closeInput).exceptionally(t -> {
			out.propagateFailure(DMLRuntimeException.of(t));
			return null;
		}).thenRun(this::onComplete);
	}

	/**
	 * The rendezvous driver on the new contract: one thread alternates dequeues between both inputs (the legacy idiom),
	 * and every tile goes through {@link JoinTable#putIfAbsent} — install when the partner has not arrived,
	 * take-and-pair when it has. Both inputs share the ONE bound table (one cache stream id), so eviction sees one
	 * population. Each arriving tile reserves one budget for memory this task may acquire: the partner pin and output,
	 * plus an owned table copy only when the callback does not already carry a reservation. An unmatched install
	 * immediately releases the unused portions; a match carries the same budget into computation.
	 */
	private void startTableDriver(OOCStream<IndexedMatrixValue> l, OOCStream<IndexedMatrixValue> r,
		OOCStream<JoinWork> intermediate, OOCStream<IndexedMatrixValue> out, long tableTileBytes,
		long outputBudgetBytes) {
		runCoordinator("ooc-join-table-driver", () -> {
			OOCStream.QueueCallback<IndexedMatrixValue> next = null;
			try {
				_policyRows = OOCUtils.getNumRowBlocks(r.getDataCharacteristics());
				long cols = OOCUtils.getNumColBlocks(r.getDataCharacteristics());
				_policyCols = cols;
				boolean nextLeft = true;

				while((next = (nextLeft ? l : r).dequeueCB()) != null && !next.isEos()) {
					IndexedMatrixValue nextValue = next.get();
					final boolean isLeft = nextLeft;
					long rIdx = nextValue.getIndexes().getRowIndex() - 1;
					long cIdx = nextValue.getIndexes().getColumnIndex() - 1;
					final int idx = (int) (rIdx * cols + cIdx);
					OOCStream.QueueCallback<IndexedMatrixValue> ownedNext = next.keepOpen();
					next.close();
					next = null; // detach from dequeueCB auto-close before handing ownership to rendezvous
					OOCFuture<JoinTable.Match> rendezvous;
					ReservationBudget tableBudget = null;
					try {
						boolean incomingReservationOwned = ownedNext instanceof MaterializedCallback
							|| ownedNext instanceof InMemoryQueueCallback && ownedNext.getManagedBytes() > 0;
						long tableBudgetBytes = saturatingAdd(outputBudgetBytes, incomingReservationOwned ?
							tableTileBytes : saturatingAdd(tableTileBytes, tableTileBytes));
						tableBudget = OOCInstructionUtils.reserveBudget(_allowance, tableBudgetBytes);
						rendezvous = JoinTable.putIfAbsent(_table, idx, ownedNext,
							tableBudget == null ? _allowance : tableBudget);
						ownedNext = null; //callback lifecycle ownership transferred to the helper
						JoinTable.Match match = getRendezvous(rendezvous);
						if(match != null) {
							JoinWork work = null;
							try {
								work = isLeft ? new JoinWork(match.own(), match.partner(), tableBudget) : new JoinWork(
									match.partner(), match.own(), tableBudget);
								tableBudget = null;
								intermediate.enqueue(new OwnedQueueCallback<>(work));
								work = null;
							}
							catch(Throwable t) {
								if(work != null)
									work.close();
								else
									closeMatch(match);
								throw t;
							}
						}
					}
					finally {
						if(tableBudget != null)
							tableBudget.close();
						if(ownedNext != null)
							ownedNext.close();
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
		});
	}

	private static long joinTableTileBytes(OOCStreamable<IndexedMatrixValue> leftInput,
		OOCStreamable<IndexedMatrixValue> rightInput) {
		long leftBytes = OOCInstructionUtils.estimateOutputTileBytes(leftInput.getDataCharacteristics());
		long rightBytes = OOCInstructionUtils.estimateOutputTileBytes(rightInput.getDataCharacteristics());
		return Math.max(leftBytes, rightBytes);
	}

	private static long saturatingAdd(long a, long b) {
		long result = a + b;
		return result < 0 ? Long.MAX_VALUE : result;
	}

	private void addMaterializedInputPolicy(OOCStreamable<IndexedMatrixValue> input) {
		if(input.hasMaterializedView())
			input.materializedView().addEvictionPolicy(this::scoreTile);
	}

	private long scoreTableSlot(int slot) {
		long cols = _policyCols;
		if(cols <= 0)
			return slot;
		return scoreTile(new MatrixIndexes(slot / cols + 1, slot % cols + 1));
	}

	private long scoreTile(MatrixIndexes ix) {
		long cols = _policyCols;
		if(cols <= 0)
			return Math.max(0, ix.getRowIndex() - 1);
		long row = ix.getRowIndex() - 1;
		long col = ix.getColumnIndex() - 1;
		if(_pattern == OOCAccessPattern.COL_MAJOR) {
			long rows = _policyRows;
			if(rows > 0)
				return col * rows + row;
		}
		return row * cols + col;
	}

	private static void closeMatch(JoinTable.Match match) {
		try(OOCStream.QueueCallback<IndexedMatrixValue> own = match.own();
			OOCStream.QueueCallback<IndexedMatrixValue> partner = match.partner()) {
			// Close both callbacks if the matched work cannot be handed to workers.
		}
	}

	private static JoinTable.Match getRendezvous(OOCFuture<JoinTable.Match> rendezvous)
		throws InterruptedException {
		try {
			return rendezvous.get();
		}
		catch(ExecutionException ex) {
			throw DMLRuntimeException.of(ex.getCause());
		}
	}

	private static final class JoinWork implements AutoCloseable {
		private final OOCStream.QueueCallback<IndexedMatrixValue> _left;
		private final OOCStream.QueueCallback<IndexedMatrixValue> _right;
		private final ReservationBudget _budget;

		private JoinWork(OOCStream.QueueCallback<IndexedMatrixValue> left,
			OOCStream.QueueCallback<IndexedMatrixValue> right, ReservationBudget budget) {
			_left = left;
			_right = right;
			_budget = budget;
		}

		@Override
		public void close() {
			try(_left; _right; _budget) {
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
