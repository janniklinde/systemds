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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public final class CartesianOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _streamed;
	private final OOCStreamable<IndexedMatrixValue> _materialized;
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final BiFunction<IndexedMatrixValue, IndexedMatrixValue, IndexedMatrixValue> _operation;
	private final boolean _streamedIsLeft;
	private final long[] _tileRows;
	private final long[] _tileCols;
	private final int _streamedTiles;
	private final AtomicBoolean _cleaned;
	private final AtomicBoolean _sourceComplete;
	private final AtomicInteger _active;
	private MaterializedStore<IndexedMatrixValue> _store;
	private IndexedMaterializedStoreReader<IndexedMatrixValue> _reader;
	private OOCStream<CartesianWork> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;

	public static CartesianOOCPrimitive create(OOCStreamable<IndexedMatrixValue> left,
		OOCStreamable<IndexedMatrixValue> right, OOCStreamable<IndexedMatrixValue> output,
		BiFunction<IndexedMatrixValue, IndexedMatrixValue, IndexedMatrixValue> operation, StreamContext context) {
		boolean streamLeft = logicalBytes(left) >= logicalBytes(right);
		return new CartesianOOCPrimitive(streamLeft ? left : right, streamLeft ? right : left, output, operation,
			streamLeft, context);
	}

	private CartesianOOCPrimitive(OOCStreamable<IndexedMatrixValue> streamed,
		OOCStreamable<IndexedMatrixValue> materialized, OOCStreamable<IndexedMatrixValue> output,
		BiFunction<IndexedMatrixValue, IndexedMatrixValue, IndexedMatrixValue> operation, boolean streamedIsLeft,
		StreamContext context) {
		super(context, streamed, materialized);
		_streamed = streamed;
		_materialized = materialized;
		_output = output;
		_operation = operation;
		_streamedIsLeft = streamedIsLeft;
		DataCharacteristics streamedDc = requireDims(streamed, "streamed");
		DataCharacteristics materializedDc = requireDims(materialized, "materialized");
		int rowBlocks = Math.toIntExact(materializedDc.getNumRowBlocks());
		int colBlocks = Math.toIntExact(materializedDc.getNumColBlocks());
		int tiles = Math.multiplyExact(rowBlocks, colBlocks);
		_tileRows = new long[tiles];
		_tileCols = new long[tiles];
		for(int row = 0, tile = 0; row < rowBlocks; row++)
			for(int col = 0; col < colBlocks; col++, tile++) {
				_tileRows[tile] = row + 1L;
				_tileCols[tile] = col + 1L;
			}
		_streamedTiles = Math.toIntExact(
			Math.multiplyExact(streamedDc.getNumRowBlocks(), streamedDc.getNumColBlocks()));
		_cleaned = new AtomicBoolean();
		_sourceComplete = new AtomicBoolean();
		_active = new AtomicInteger(1);
	}

	private static DataCharacteristics requireDims(OOCStreamable<IndexedMatrixValue> input, String side) {
		DataCharacteristics dc = input.getDataCharacteristics();
		if(dc == null || !dc.dimsKnown() || dc.getBlocksize() <= 0)
			throw new DMLRuntimeException("Cartesian primitive requires known dimensions and block size on its "
				+ side + " input.");
		return dc;
	}

	private static long logicalBytes(OOCStreamable<IndexedMatrixValue> input) {
		DataCharacteristics dc = input.getDataCharacteristics();
		if(dc == null || !dc.dimsKnown())
			return Long.MAX_VALUE;
		long cells = dc.getRows() * dc.getCols();
		return cells < 0 ? Long.MAX_VALUE : cells;
	}

	@Override
	protected long getMaxTaskReservationBytes() {
		return taskBytes(OOCUtils.estimateOutputTileBytes(_streamed.getDataCharacteristics()));
	}

	private long taskBytes(long streamedBytes) {
		long pin = OOCCacheManager.getGlobalCache()
			.maxPhysicalPinBytes(OOCUtils.estimateOutputTileBytes(_materialized.getDataCharacteristics()));
		long outputs = OOCUtils.estimateOutputTileBytes(_output.getDataCharacteristics()) * _tileRows.length;
		return pin * _tileRows.length * 2 + streamedBytes * 2 + outputs * 2;
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		return List.of(new OOCMaterializedInputRequest(1, OOCStoreLayout.ROW_MAJOR, 1));
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

		getMaterializedInput(1).whenComplete((store, error) -> {
			if(error != null) {
				fail(error);
				finishSource();
				return;
			}
			_store = store;
			store.completion().whenComplete((ignored, completionError) -> {
				if(completionError != null) {
					fail(completionError);
					finishSource();
					return;
				}
				try {
					_reader = store.openIndexedReader(new CountingLiveness(_tileRows.length, _streamedTiles));
					startStreaming();
				}
				catch(Throwable failure) {
					fail(failure);
					finishSource();
				}
			});
		});
	}

	private void startStreaming() {
		OOCStream<IndexedMatrixValue> streamed = getInputReadStream(0);
		AllocatedOOCStream<IndexedMatrixValue> admitted = new AllocatedOOCStream<>(streamed, _allowance,
			value -> taskBytes(OOCUtils.memoryCharge(value)), true);
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
				throw new DMLRuntimeException("Missing admitted cartesian task budget.");
			budget.enableReuse();
			List<OOCFuture<StoreLease<IndexedMatrixValue>>> requests = new ArrayList<>(_tileRows.length);
			try {
				for(int tile = 0; tile < _tileRows.length; tile++)
					requests.add(_reader.request(_tileRows[tile], _tileCols[tile], budget));
			}
			catch(Throwable failure) {
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
			OOCFuture.allOf(requests, StoreLease::close)
				.whenComplete((leases, error) -> tilesReady(pendingStreamed, leases, pendingBudget, error));
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

	private void tilesReady(OOCStream.QueueCallback<IndexedMatrixValue> streamed,
		List<StoreLease<IndexedMatrixValue>> leases, ReservationBudget budget, Throwable error) {
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
				fail(error != null ? error : new IllegalStateException("Missing cartesian tile ("
					+ _tileRows[missing] + "," + _tileCols[missing] + ")"));
				completeOne();
			}
			return;
		}
		CartesianWork work = new CartesianWork(streamed, leases, budget);
		try {
			_ready.enqueue(work);
		}
		catch(Throwable failure) {
			work.close();
			fail(failure);
			completeOne();
		}
	}

	private void process(CartesianWork work) {
		ReservationBudget budget = work.takeBudget();
		try {
			IndexedMatrixValue streamed = work._streamed.get();
			for(StoreLease<IndexedMatrixValue> lease : work._tiles) {
				IndexedMatrixValue tile = lease.value();
				IndexedMatrixValue value = _streamedIsLeft ? _operation.apply(streamed, tile)
					: _operation.apply(tile, streamed);
				long bytes = OOCUtils.memoryCharge(value);
				budget.reserveBlocking(bytes);
				OOCStream.QueueCallback<IndexedMatrixValue> result =
					new InMemoryQueueCallback<>(value, null, budget, bytes);
				try {
					_outputStream.enqueue(result);
					result = null;
				}
				finally {
					if(result != null)
						result.close();
				}
			}
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
		}
	}

	private void cleanup() {
		if(!_cleaned.compareAndSet(false, true))
			return;
		try {
			if(_reader != null)
				_reader.close();
		}
		finally {
			try {
				if(_store != null)
					_store.close();
			}
			finally {
				onComplete();
			}
		}
	}

	private static final class CartesianWork implements AutoCloseable {
		private OOCStream.QueueCallback<IndexedMatrixValue> _streamed;
		private List<StoreLease<IndexedMatrixValue>> _tiles;
		private ReservationBudget _budget;

		private CartesianWork(OOCStream.QueueCallback<IndexedMatrixValue> streamed,
			List<StoreLease<IndexedMatrixValue>> tiles, ReservationBudget budget) {
			_streamed = streamed;
			_tiles = tiles;
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
			if(_tiles != null) {
				for(StoreLease<IndexedMatrixValue> lease : _tiles)
					if(lease != null)
						lease.close();
				_tiles = null;
			}
			if(_budget != null) {
				_budget.close();
				_budget = null;
			}
		}
	}
}
