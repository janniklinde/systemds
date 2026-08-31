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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

/**
 * Streams one input and pairs every streamed tile with a targeted band of indexed tiles from each of one or more
 * secondary inputs.
 *
 * The secondary inputs are not replicated in memory: each is materialized into a store and read through an indexed
 * reader that pins tiles on demand and evicts them once their liveness count is exhausted. A side therefore degrades to
 * disk-backed random access rather than failing when it does not fit, which is why there is no size precondition on the
 * indexed sides. A side's band may span several column tiles, so a secondary input is not restricted to a single column
 * block the way a replicated broadcast would be.
 */
public final class BroadcastOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue>[] _broadcasts;
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final ToLongFunction<IndexedMatrixValue>[] _lookupRows;
	private final ToLongFunction<IndexedMatrixValue>[] _lookupCols;
	private final int[] _bandWidths;
	private final Supplier<MaterializedStore.Liveness>[] _liveness;
	private final BiFunction<IndexedMatrixValue, IndexedMatrixValue[][], IndexedMatrixValue> _operation;
	private final AtomicBoolean _cleaned;
	private final AtomicBoolean _sourceComplete;
	private final AtomicInteger _active;
	private final AtomicInteger _pendingStores;
	private final MaterializedStore<IndexedMatrixValue>[] _stores;
	private final IndexedMaterializedStoreReader<IndexedMatrixValue>[] _readers;
	private OOCStream<BroadcastWork> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;

	public BroadcastOOCPrimitive(OOCStreamable<IndexedMatrixValue> streamed,
		OOCStreamable<IndexedMatrixValue> broadcast, OOCStreamable<IndexedMatrixValue> output,
		ToLongFunction<IndexedMatrixValue> lookupRow, ToLongFunction<IndexedMatrixValue> lookupCol,
		Supplier<MaterializedStore.Liveness> liveness,
		BiFunction<IndexedMatrixValue, IndexedMatrixValue, IndexedMatrixValue> operation, StreamContext context) {
		this(streamed, List.of(broadcast), output, List.of(lookupRow), List.of(lookupCol), List.of(1),
			List.of(liveness), (value, tiles) -> operation.apply(value, tiles[0][0]), context);
	}

	@SuppressWarnings("unchecked")
	public BroadcastOOCPrimitive(OOCStreamable<IndexedMatrixValue> streamed,
		List<OOCStreamable<IndexedMatrixValue>> broadcasts, OOCStreamable<IndexedMatrixValue> output,
		List<ToLongFunction<IndexedMatrixValue>> lookupRows, List<ToLongFunction<IndexedMatrixValue>> lookupCols,
		List<Integer> bandWidths, List<Supplier<MaterializedStore.Liveness>> liveness,
		BiFunction<IndexedMatrixValue, IndexedMatrixValue[][], IndexedMatrixValue> operation, StreamContext context) {
		super(context, inputs(streamed, broadcasts));
		if(broadcasts.isEmpty())
			throw new DMLRuntimeException("Broadcast primitive requires at least one indexed input.");
		if(lookupRows.size() != broadcasts.size() || lookupCols.size() != broadcasts.size() ||
			bandWidths.size() != broadcasts.size() || liveness.size() != broadcasts.size())
			throw new DMLRuntimeException(
				"Broadcast primitive requires one lookup, band width and liveness per " + "indexed input.");
		_broadcasts = broadcasts.toArray(new OOCStreamable[0]);
		_output = output;
		_lookupRows = lookupRows.toArray(new ToLongFunction[0]);
		_lookupCols = lookupCols.toArray(new ToLongFunction[0]);
		_bandWidths = new int[broadcasts.size()];
		for(int i = 0; i < _bandWidths.length; i++) {
			_bandWidths[i] = bandWidths.get(i);
			if(_bandWidths[i] < 1)
				throw new DMLRuntimeException("Indexed input " + (i + 1) + " needs a positive band width.");
		}
		_liveness = liveness.toArray(new Supplier[0]);
		_operation = operation;
		_cleaned = new AtomicBoolean();
		_sourceComplete = new AtomicBoolean();
		_active = new AtomicInteger(1);
		_pendingStores = new AtomicInteger(_broadcasts.length);
		_stores = new MaterializedStore[_broadcasts.length];
		_readers = new IndexedMaterializedStoreReader[_broadcasts.length];
	}

	public ToLongFunction<IndexedMatrixValue>[] getLookupRows() {
		return _lookupRows.clone();
	}

	public ToLongFunction<IndexedMatrixValue>[] getLookupCols() {
		return _lookupCols.clone();
	}

	public int[] getBandWidths() {
		return _bandWidths.clone();
	}

	public BiFunction<IndexedMatrixValue, IndexedMatrixValue[][], IndexedMatrixValue> getOperation() {
		return _operation;
	}

	public OOCStreamable<IndexedMatrixValue> getOutput() {
		return _output;
	}

	@Override
	public long getMaxTaskReservationBytes(IndexedMatrixValue... inputs) {
		long broadcastPin = 0;
		for(int i = 0; i < _broadcasts.length; i++) {
			long logical = inputs.length > i + 1 ? OOCUtils.memoryCharge(inputs[i + 1]) : OOCUtils
				.estimateFullTileBytes(_broadcasts[i].getDataCharacteristics());
			broadcastPin += OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(logical) * _bandWidths[i];
		}
		return broadcastPin + OOCUtils.estimateFullTileBytes(_output.getDataCharacteristics());
	}

	private static OOCStreamable<?>[] inputs(OOCStreamable<IndexedMatrixValue> streamed,
		List<OOCStreamable<IndexedMatrixValue>> broadcasts) {
		OOCStreamable<?>[] inputs = new OOCStreamable<?>[broadcasts.size() + 1];
		inputs[0] = streamed;
		for(int i = 0; i < broadcasts.size(); i++)
			inputs[i + 1] = broadcasts.get(i);
		return inputs;
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		List<OOCMaterializedInputRequest> requests = new ArrayList<>(_broadcasts.length);
		for(int i = 0; i < _broadcasts.length; i++)
			requests.add(new OOCMaterializedInputRequest(i + 1, OOCStoreLayout.ROW_MAJOR, 1));
		return requests;
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(OOCAccessPattern.ROW_MAJOR);
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(OOCAccessPattern.ROW_MAJOR);
	}

	@Override
	protected void startExecution() {
		_outputStream = _output.getWriteStream();
		_ready = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _ready);
		OOCInstructionUtils.submitCloseableOOCTasks(_ready, this::process, getContext())
			.whenComplete((ignored, error) -> {
				try {
					if(error != null)
						fail(error);
					_outputStream.closeInput();
				}
				catch(Throwable failure) {
					fail(failure);
				}
				finally {
					cleanup();
				}
			});

		for(int i = 0; i < _broadcasts.length; i++) {
			int side = i;
			getMaterializedInput(side + 1).whenComplete((store, error) -> {
				if(error != null) {
					fail(error);
					finishSource();
					return;
				}
				_stores[side] = store;
				store.completion().whenComplete((ignored, completionError) -> {
					if(completionError != null) {
						fail(completionError);
						finishSource();
						return;
					}
					try {
						_readers[side] = store.openIndexedReader(_liveness[side].get());
						if(_pendingStores.decrementAndGet() == 0)
							startBroadcast();
					}
					catch(Throwable failure) {
						fail(failure);
						finishSource();
					}
				});
			});
		}
	}

	private void startBroadcast() {
		OOCStream<IndexedMatrixValue> streamed = getInputReadStream(0);
		AllocatedOOCStream<IndexedMatrixValue> admitted = new AllocatedOOCStream<>(streamed, _allowance,
			value -> getMaxTaskReservationBytes(value), true);
		getContext().addInStream(streamed, admitted);
		admitted.setSubscriber(this::accept);
	}

	private void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback.isEos() || callback.isFailure()) {
			try(callback) {
				if(callback.isFailure())
					callback.get();
			}
			catch(Throwable failure) {
				fail(failure);
			}
			finishSource();
			return;
		}

		ReservationBudget budget = null;
		OOCStream.QueueCallback<IndexedMatrixValue> retained = null;
		_active.incrementAndGet();
		try(callback) {
			budget = AllocatedOOCStream.detachBudget(callback);
			if(budget == null)
				throw new DMLRuntimeException("Missing admitted broadcast task budget.");
			IndexedMatrixValue streamed = callback.get();
			long[] lookupRows = new long[_broadcasts.length];
			long[] lookupCols = new long[_broadcasts.length];
			List<OOCFuture<StoreLease<IndexedMatrixValue>>> requests = new ArrayList<>(_broadcasts.length);
			try {
				for(int i = 0; i < _broadcasts.length; i++) {
					lookupRows[i] = _lookupRows[i].applyAsLong(streamed);
					lookupCols[i] = _lookupCols[i].applyAsLong(streamed);
					// a band spans consecutive column tiles from the looked-up one, so a side is not limited to a
					// single column block; the flat request list is regrouped per side before the operation runs
					for(int tile = 0; tile < _bandWidths[i]; tile++)
						requests.add(_readers[i].request(lookupRows[i], lookupCols[i] + tile, budget));
				}
			}
			catch(Throwable failure) {
				// a later side failed to issue, so the tiles the earlier sides are still pinning would never be
				// handed to anyone; release them as they arrive instead of leaking the pins
				for(OOCFuture<StoreLease<IndexedMatrixValue>> issued : requests)
					issued.whenComplete((lease, ignored) -> {
						if(lease != null)
							lease.close();
					});
				throw failure;
			}
			retained = callback.keepOpen();
			OOCStream.QueueCallback<IndexedMatrixValue> pendingStreamed = retained;
			ReservationBudget pendingBudget = budget;
			retained = null;
			budget = null;
			OOCFuture.allOf(requests, StoreLease::close).whenComplete((leases, error) -> broadcastReady(pendingStreamed,
				leases, pendingBudget, lookupRows, lookupCols, error));
		}
		catch(Throwable failure) {
			fail(failure);
			completeOne();
		}
		finally {
			if(retained != null)
				retained.close();
			if(budget != null)
				budget.close();
		}
	}

	private void broadcastReady(OOCStream.QueueCallback<IndexedMatrixValue> streamed,
		List<StoreLease<IndexedMatrixValue>> leases, ReservationBudget budget, long[] lookupRows, long[] lookupCols,
		Throwable error) {
		int missing = -1;
		if(error == null && leases != null)
			for(int i = 0; i < leases.size() && missing < 0; i++)
				if(leases.get(i) == null)
					missing = i;
		if(error != null || leases == null || missing >= 0) {
			try {
				streamed.close();
				if(leases != null)
					for(StoreLease<IndexedMatrixValue> lease : leases)
						if(lease != null)
							lease.close();
				budget.close();
			}
			finally {
				int side = 0;
				int within = missing;
				while(side < _bandWidths.length - 1 && within >= _bandWidths[side]) {
					within -= _bandWidths[side];
					side++;
				}
				fail(error != null ? error : new IllegalStateException("Missing broadcast tile (" + lookupRows[side]
					+ "," + (lookupCols[side] + within) + ") on indexed input " + (side + 1)));
				completeOne();
			}
			return;
		}
		BroadcastWork work = new BroadcastWork(streamed, leases, budget);
		try {
			_ready.enqueue(work);
		}
		catch(Throwable failure) {
			work.close();
			fail(failure);
			completeOne();
		}
	}

	private void process(BroadcastWork work) {
		ReservationBudget budget = work.takeBudget();
		try {
			IndexedMatrixValue[][] tiles = new IndexedMatrixValue[_broadcasts.length][];
			int flat = 0;
			for(int i = 0; i < tiles.length; i++) {
				tiles[i] = new IndexedMatrixValue[_bandWidths[i]];
				for(int tile = 0; tile < _bandWidths[i]; tile++)
					tiles[i][tile] = work._broadcasts.get(flat++).value();
			}
			IndexedMatrixValue output = _operation.apply(work._streamed.get(), tiles);
			OOCUtils.enqueueExact(_outputStream, output, budget);
			budget = null;
		}
		catch(Throwable failure) {
			fail(failure);
		}
		finally {
			if(budget != null)
				budget.close();
			completeOne();
		}
	}

	private void finishSource() {
		if(_sourceComplete.compareAndSet(false, true))
			completeOne();
	}

	private void completeOne() {
		if(_active.decrementAndGet() != 0)
			return;
		try {
			_ready.closeInput();
		}
		catch(IllegalStateException ignored) {
			// Failure propagation may already have closed the ready stream.
		}
	}

	private void cleanup() {
		if(!_cleaned.compareAndSet(false, true))
			return;
		try {
			for(IndexedMaterializedStoreReader<IndexedMatrixValue> reader : _readers)
				if(reader != null)
					reader.close();
		}
		finally {
			try {
				for(MaterializedStore<IndexedMatrixValue> store : _stores)
					if(store != null)
						store.close();
			}
			finally {
				onComplete();
			}
		}
	}

	private static final class BroadcastWork implements AutoCloseable {
		private OOCStream.QueueCallback<IndexedMatrixValue> _streamed;
		private List<StoreLease<IndexedMatrixValue>> _broadcasts;
		private ReservationBudget _budget;

		private BroadcastWork(OOCStream.QueueCallback<IndexedMatrixValue> streamed,
			List<StoreLease<IndexedMatrixValue>> broadcasts, ReservationBudget budget) {
			_streamed = streamed;
			_broadcasts = broadcasts;
			_budget = budget;
		}

		private ReservationBudget takeBudget() {
			ReservationBudget budget = _budget;
			_budget = null;
			return budget;
		}

		@Override
		public void close() {
			if(_streamed != null) {
				_streamed.close();
				_streamed = null;
			}
			if(_broadcasts != null) {
				for(StoreLease<IndexedMatrixValue> lease : _broadcasts)
					if(lease != null)
						lease.close();
				_broadcasts = null;
			}
			if(_budget != null) {
				_budget.close();
				_budget = null;
			}
		}
	}
}
