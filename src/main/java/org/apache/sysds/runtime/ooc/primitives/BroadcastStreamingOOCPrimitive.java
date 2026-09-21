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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.ooc.util.StateTableUtils;

public final class BroadcastStreamingOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _matrix;
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final boolean _row;
	private final boolean _transposeSummary;
	private final OOCStreamable<IndexedMatrixValue> _summaryInput;
	private final BiFunction<IndexedMatrixValue, IndexedMatrixValue, IndexedMatrixValue> _operation;
	private final AtomicInteger _pendingArrivals = new AtomicInteger(2);
	private final AtomicInteger _pendingMatches = new AtomicInteger(1);
	private OOCStream<Integer> _matches;
	private StateTable<IndexedMatrixValue> _tiles;
	private StateTable<IndexedMatrixValue> _summaries;
	private OOCStream<Work> _ready;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private AtomicIntegerArray _remaining;
	private int _rows;
	private int _cols;
	private BiConsumer<Integer, Long> _pendingListener = (band, bytes) -> { };

	public BroadcastStreamingOOCPrimitive(OOCStreamable<IndexedMatrixValue> matrix,
		OOCStreamable<IndexedMatrixValue> summaries, OOCStreamable<IndexedMatrixValue> output, boolean row,
		boolean transposeSummary,
		BiFunction<IndexedMatrixValue, IndexedMatrixValue, IndexedMatrixValue> operation, StreamContext context) {
		super(context, matrix, summaries);
		_matrix = matrix;
		_output = output;
		_row = row;
		_transposeSummary = transposeSummary;
		_summaryInput = summaries;
		_operation = operation;
	}

	public void setPendingListener(BiConsumer<Integer, Long> listener) {
		_pendingListener = listener;
	}

	@Override
	protected boolean isStreamingInput(int index) {
		return true;
	}

	@Override
	public boolean propagatesStreamingProperties(int index) {
		return true;
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
	protected long getMaxTaskReservationBytes() {
		return OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(
			OOCUtils.estimateOutputTileBytes(_matrix.getDataCharacteristics()))
			+ OOCCacheManager.getGlobalCache().maxPhysicalPinBytes(
				OOCUtils.estimateOutputTileBytes(_summaryInput.getDataCharacteristics()))
			+ 2 * OOCUtils.estimateOutputTileBytes(_output.getDataCharacteristics());
	}

	@Override
	protected void startExecution() {
		_rows = Math.toIntExact(_matrix.getDataCharacteristics().getNumRowBlocks());
		_cols = Math.toIntExact(_matrix.getDataCharacteristics().getNumColBlocks());
		int bands = _row ? _rows : _cols;
		_remaining = new AtomicIntegerArray(bands);
		for(int band = 0; band < bands; band++)
			_remaining.set(band, _row ? _cols : _rows);
		_tiles = new StateTable<>();
		_summaries = new StateTable<>();
		_outputStream = _output.getWriteStream();
		_ready = new SubscribableTaskQueue<>();
		_matches = new SubscribableTaskQueue<>();
		getContext().addOutStream(_outputStream, _ready);
		OOCStream<IndexedMatrixValue> matrix = getInputReadStream(0);
		OOCStream<IndexedMatrixValue> summaries = getInputReadStream(1);
		getContext().addInStream(matrix, summaries);
		OOCInstructionUtils.submitCloseableOOCTasks(_ready, this::process, getContext())
			.whenComplete((ignored, error) -> {
				try {
					if(error != null)
						fail(error);
					_outputStream.closeInput();
				}
				finally {
					_tiles.close();
					_summaries.close();
					onComplete();
				}
			});
		AllocatedOOCStream<Integer> admitted = new AllocatedOOCStream<>(_matches, _allowance,
			index -> getMaxTaskReservationBytes(), true);
		getContext().addInStream(_matches, admitted);
		admitted.setSubscriber(this::match);
		matrix.setSubscriber(callback -> accept(callback, false));
		summaries.setSubscriber(callback -> accept(callback, true));
	}

	private void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback, boolean summary) {
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
		try {
			IndexedMatrixValue value = callback.get();
			int row = (int) value.getIndexes().getRowIndex() - 1;
			int col = (int) value.getIndexes().getColumnIndex() - 1;
			int band = summary && _transposeSummary ? (_row ? col : row) : (_row ? row : col);
			int index = summary ? band : row * _cols + col;
			long bytes = value.size();
			StateTableUtils.put(summary ? _summaries : _tiles, index, callback, _allowance);
			callback.close();
			if(summary) {
				for(int tile = 0; tile < (_row ? _cols : _rows); tile++)
					_matches.enqueue(_row ? band * _cols + tile : tile * _cols + band);
			}
			else {
				_pendingListener.accept(band, bytes);
				_matches.enqueue(index);
			}
		}
		catch(Throwable error) {
			callback.close();
			fail(error);
		}
		finally {
			finishArrival();
		}
	}

	private void finishArrival() {
		if(_pendingArrivals.decrementAndGet() == 0)
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
			int index = callback.get();
			int band = _row ? index / _cols : index % _cols;
			_summaries.acquire(band, budget).whenComplete((summary, error) -> {
				if(error != null || summary == null) {
					if(error != null)
						fail(error);
					budget.close();
					finishMatch();
					return;
				}
				try {
					_tiles.take(index, budget).whenComplete((tile, failure) -> {
						try {
							if(failure != null)
								throw DMLRuntimeException.of(failure);
							if(tile == null) {
								summary.close();
								budget.close();
							}
							else {
								_pendingListener.accept(band, -tile.value().size());
								_ready.enqueue(new Work(tile, summary, budget, band));
							}
						}
						catch(Throwable problem) {
							if(tile != null)
								tile.close();
							summary.close();
							budget.close();
							fail(problem);
						}
						finally {
							finishMatch();
						}
					});
				}
				catch(Throwable failure) {
					summary.close();
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
		if(_pendingMatches.decrementAndGet() == 0)
			_ready.closeInput();
	}

	private void process(Work work) {
		OOCUtils.enqueueExact(_outputStream, _operation.apply(work.tile.value(), work.summary.value()), work.budget);
	}

	private final class Work implements AutoCloseable {
		private final StoreLease<IndexedMatrixValue> tile;
		private final StoreLease<IndexedMatrixValue> summary;
		private final ReservationBudget budget;
		private final int band;

		private Work(StoreLease<IndexedMatrixValue> tile, StoreLease<IndexedMatrixValue> summary,
			ReservationBudget budget, int band) {
			this.tile = tile;
			this.summary = summary;
			this.budget = budget;
			this.band = band;
		}

		@Override
		public void close() {
			try {
				tile.close();
				summary.close();
				if(_remaining.decrementAndGet(band) == 0)
					_summaries.clear(band);
			}
			finally {
				budget.close();
			}
		}
	}
}
