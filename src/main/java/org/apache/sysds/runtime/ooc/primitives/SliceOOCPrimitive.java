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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.OrderedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.SelectedAccessPattern;
import org.apache.sysds.runtime.ooc.store.StoreBackedStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public final class SliceOOCPrimitive extends OOCPrimitive {
	private static final int PREFETCH = 4;

	private final OOCStreamable<IndexedMatrixValue> _input;
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final boolean _fromStore;
	private final long _firstRowBlock;
	private final long _lastRowBlock;
	private final long _firstColBlock;
	private final long _lastColBlock;
	private final AtomicBoolean _cleaned;
	private final AtomicBoolean _requested;
	private volatile OOCStream<IndexedMatrixValue> _outputStream;
	private MaterializedStore<IndexedMatrixValue> _store;
	private OrderedMaterializedStoreReader<IndexedMatrixValue> _reader;

	public SliceOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<IndexedMatrixValue> output,
		long firstRowBlock, long lastRowBlock, long firstColBlock, long lastColBlock, StreamContext context) {
		super(context, input);
		_input = input;
		_output = output;
		_firstRowBlock = firstRowBlock;
		_lastRowBlock = lastRowBlock;
		_firstColBlock = firstColBlock;
		_lastColBlock = lastColBlock;
		_fromStore = input.hasMaterializedStore();
		_cleaned = new AtomicBoolean();
		_requested = new AtomicBoolean();
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		if(!_fromStore || !_requested.compareAndSet(false, true))
			return List.of();
		return List.of(new OOCMaterializedInputRequest(0, OOCStoreLayout.ROW_MAJOR, 1));
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
	protected long getMaxTaskReservationBytes() {
		return OOCUtils.estimateOutputTileBytes(_input.getDataCharacteristics());
	}

	@Override
	protected long getAllowanceLimit(GlobalMemoryBroker broker) {
		long taskBytes = getMaxTaskReservationBytes();
		if(taskBytes <= 0)
			return super.getAllowanceLimit(broker);
		return Math.min(super.getAllowanceLimit(broker), 2L * PREFETCH * taskBytes);
	}

	@Override
	protected void startExecution() {
		_outputStream = _output.getWriteStream();
		getContext().addOutStream(_outputStream);
		try {
			if(!_fromStore) {
				OOCStream<IndexedMatrixValue> source = getInputReadStream(0);
				getContext().addInStream(source);
				source.setSubscriber(this::accept);
				return;
			}
			getMaterializedInput(0).whenComplete((store, error) -> {
				if(error != null) {
					failAndClose(error);
					return;
				}
				_store = store;
				store.completion().whenComplete((ignored, completionError) -> {
					if(completionError != null)
						failAndClose(completionError);
					else
						startTargetedRead(store);
				});
			});
		}
		catch(Throwable failure) {
			failAndClose(failure);
		}
	}

	private void startTargetedRead(MaterializedStore<IndexedMatrixValue> store) {
		try {
			int[] selected = selectIndices(store);
			SelectedAccessPattern pattern = new SelectedAccessPattern(store.size(), selected);
			_reader = store.openReader(pattern, _allowance, PREFETCH, false);
			new StoreBackedStream<>(_reader).setSubscriber(this::acceptTargeted);
		}
		catch(Throwable failure) {
			failAndClose(failure);
		}
	}

	private int[] selectIndices(MaterializedStore<IndexedMatrixValue> store) {
		DataCharacteristics dc = store.characteristics();
		if(store.layout() == null || dc == null || !dc.dimsKnown())
			throw new DMLRuntimeException("Targeted slice requires a materialized store with a known layout.");
		long rowBlocks = Math.min(_lastRowBlock, dc.getNumRowBlocks());
		long colBlocks = Math.min(_lastColBlock, dc.getNumColBlocks());
		int count = Math.toIntExact(Math.max(0, rowBlocks - _firstRowBlock + 1) *
			Math.max(0, colBlocks - _firstColBlock + 1));
		int[] selected = new int[count];
		int pos = 0;
		for(long row = _firstRowBlock; row <= rowBlocks; row++)
			for(long col = _firstColBlock; col <= colBlocks; col++)
				selected[pos++] = store.linearize(row, col);
		Arrays.sort(selected);
		return selected;
	}

	private void acceptTargeted(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback.isEos() || callback.isFailure()) {
			finish(callback);
			return;
		}
		try(callback) {
			forward(callback);
		}
		catch(Throwable failure) {
			failAndClose(failure);
		}
	}

	private void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback.isEos() || callback.isFailure()) {
			finish(callback);
			return;
		}
		try(callback) {
			if(selects(callback.get().getIndexes()))
				forward(callback);
		}
		catch(Throwable failure) {
			failAndClose(failure);
		}
	}

	private boolean selects(MatrixIndexes indexes) {
		long row = indexes.getRowIndex();
		long col = indexes.getColumnIndex();
		return row >= _firstRowBlock && row <= _lastRowBlock && col >= _firstColBlock && col <= _lastColBlock;
	}

	private void forward(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		MatrixIndexes source = callback.get().getIndexes();
		MatrixIndexes shifted = new MatrixIndexes(source.getRowIndex() - _firstRowBlock + 1,
			source.getColumnIndex() - _firstColBlock + 1);
		OOCStream.QueueCallback<IndexedMatrixValue> retained = new ReindexedCallback(callback.keepOpen(), shifted);
		try {
			_outputStream.enqueue(retained);
			retained = null;
		}
		finally {
			if(retained != null)
				retained.close();
		}
	}

	private void finish(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		try(callback) {
			if(callback.isFailure())
				callback.get();
			else
				_outputStream.closeInput();
		}
		catch(Throwable failure) {
			fail(failure);
		}
		finally {
			cleanup();
		}
	}

	private void failAndClose(Throwable error) {
		try {
			fail(error);
			_outputStream.propagateFailure(DMLRuntimeException.of(error));
		}
		finally {
			cleanup();
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

	private static final class ReindexedCallback implements OOCStream.QueueCallback<IndexedMatrixValue> {
		private final OOCStream.QueueCallback<IndexedMatrixValue> _delegate;
		private final MatrixIndexes _indexes;
		private IndexedMatrixValue _value;

		private ReindexedCallback(OOCStream.QueueCallback<IndexedMatrixValue> delegate, MatrixIndexes indexes) {
			_delegate = delegate;
			_indexes = indexes;
		}

		@Override
		public synchronized IndexedMatrixValue get() {
			if(_value == null)
				_value = new IndexedMatrixValue(_indexes, _delegate.get().getValue());
			return _value;
		}

		@Override
		public OOCStream.QueueCallback<IndexedMatrixValue> keepOpen() {
			return new ReindexedCallback(_delegate.keepOpen(), _indexes);
		}

		@Override
		public void close() {
			_delegate.close();
		}

		@Override
		public void fail(DMLRuntimeException failure) {
			_delegate.fail(failure);
		}

		@Override
		public boolean isEos() {
			return _delegate.isEos();
		}

		@Override
		public boolean isFailure() {
			return _delegate.isFailure();
		}
	}
}
