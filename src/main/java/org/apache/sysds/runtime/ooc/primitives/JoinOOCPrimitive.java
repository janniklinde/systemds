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
import java.util.function.Function;

public class JoinOOCPrimitive extends OOCPrimitive {
	private final List<OOCStreamable<IndexedMatrixValue>> _inputStreamables;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final Function<List<MatrixBlock>, MatrixBlock> _fn;
	private final StreamContext _sc;
	private OperatorStateTable<IndexedMatrixValue> _table;
	private volatile long _policyRows;
	private volatile long _policyCols;

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
		return false;
	}

	@Override
	public boolean requiresStateTable() {
		return true;
	}

	@Override
	public long getDenseTileMemoryFactor() {
		return 2;
	}

	@Override
	public long getMinimumOperatingMemoryFactor() {
		return 3;
	}

	@Override
	public void bindStateTable(OperatorStateTable<IndexedMatrixValue> table) {
		_table = table;
		_table.addEvictionPolicy(this::scoreTableSlot);
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

		startTableDriver(l, r, intermediate, out);

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
				if(_startsRegion && !reservationOwned)
					throw new IllegalStateException("Join output reservation was not pre-admitted.");
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
	 * table (one cache stream id), so eviction sees one population. The driver admits output memory
	 * only after a match exists, so unmatched lookup-table entries do not hold output reservations.
	 */
	private void startTableDriver(OOCStream<IndexedMatrixValue> l, OOCStream<IndexedMatrixValue> r,
		OOCStream<JoinWork> intermediate,
		OOCStream<IndexedMatrixValue> out) {
		runCoordinator("ooc-join-table-driver", () -> {
			OOCStream.QueueCallback<IndexedMatrixValue> next = null;
			long outputBytes = 0;
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
					long bytes = _allocFn.applyAsLong(nextValue.getIndexes());
					OOCStream.QueueCallback<IndexedMatrixValue> ownedNext = next.keepOpen();
					next.close();
					next = null; // detach from dequeueCB auto-close before handing ownership to rendezvous
					OOCFuture<TableRendezvous.Match> rendezvous;
					try {
						rendezvous = TableRendezvous.installOrTake(_table, idx, ownedNext, _allowance, bytes);
						ownedNext = null; //ownership transferred to the helper
					}
					finally {
						if(ownedNext != null)
							ownedNext.close();
					}
					TableRendezvous.Match match = getRendezvous(rendezvous);
					if(match != null) {
						long reservedBytes = 0;
						JoinWork work = null;
						try {
							if(_startsRegion) {
								if(outputBytes == 0)
									outputBytes = _allocFn.applyAsLong(new MatrixIndexes(1, 1));
								_allowance.reserveBlocking(outputBytes);
								reservedBytes = outputBytes;
							}
							work = isLeft ?
								new JoinWork(match.own(), match.partner(), reservedBytes) :
								new JoinWork(match.partner(), match.own(), reservedBytes);
							intermediate.enqueue(work);
							work = null;
						}
						catch(Throwable t) {
							if(work != null)
								work.closeInputs();
							else
								closeMatch(match);
							if(reservedBytes > 0)
								_allowance.release(reservedBytes);
							throw t;
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
		});
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

	private static void closeMatch(TableRendezvous.Match match) {
		try(OOCStream.QueueCallback<IndexedMatrixValue> own = match.own();
			OOCStream.QueueCallback<IndexedMatrixValue> partner = match.partner()) {
			// Close both callbacks if the matched work cannot be handed to workers.
		}
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
		private final long _outputBytes;

		private JoinWork(OOCStream.QueueCallback<IndexedMatrixValue> left,
			OOCStream.QueueCallback<IndexedMatrixValue> right, long outputBytes) {
			_left = left;
			_right = right;
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
