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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.OrderedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.SequentialAccessPattern;
import org.apache.sysds.runtime.ooc.store.StoreBackedStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public final class OrderedMappingOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final OOCStoreLayout _layout;
	private final Function<IndexedMatrixValue, IndexedMatrixValue> _operation;
	private final AtomicBoolean _cleaned = new AtomicBoolean();
	private MaterializedStore<IndexedMatrixValue> _store;
	private OrderedMaterializedStoreReader<IndexedMatrixValue> _reader;
	private OOCStream<IndexedMatrixValue> _outputStream;
	private long _outputBytes;

	public OrderedMappingOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<IndexedMatrixValue> output,
		OOCStoreLayout layout, Function<IndexedMatrixValue, IndexedMatrixValue> operation, StreamContext context) {
		super(context, input);
		_output = output;
		_layout = layout;
		_operation = operation;
	}

	@Override
	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		return List.of(new OOCMaterializedInputRequest(0, _layout, 1));
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = _layout == OOCStoreLayout.ROW_MAJOR ? OOCAccessPattern.ROW_MAJOR : OOCAccessPattern.COL_MAJOR;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(_pattern);
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = _layout == OOCStoreLayout.ROW_MAJOR ? OOCAccessPattern.ROW_MAJOR : OOCAccessPattern.COL_MAJOR;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(_pattern);
	}

	@Override
	protected void startExecution() {
		_outputStream = _output.getWriteStream();
		_outputBytes = OOCUtils.estimateOutputTileBytes(_output.getDataCharacteristics());
		getContext().addOutStream(_outputStream);
		getMaterializedInput(0).whenComplete((store, error) -> {
			if(error != null) {
				fail(error);
				cleanup();
				return;
			}
			_store = store;
			store.completion().whenComplete((ignored, completionError) -> {
				if(completionError != null) {
					fail(completionError);
					cleanup();
					return;
				}
				try {
					_reader = store.openReader(new SequentialAccessPattern(store.size()), _allowance, 1, false);
					new StoreBackedStream<>(_reader).setSubscriber(this::accept);
				}
				catch(Throwable failure) {
					fail(failure);
					cleanup();
				}
			});
		});
	}

	private void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback.isEos() || callback.isFailure()) {
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
			return;
		}

		ReservationBudget budget = null;
		try(callback) {
			_allowance.reserveBlocking(_outputBytes);
			budget = new ReservationBudget(_allowance, _outputBytes);
			OOCUtils.enqueueExact(_outputStream, _operation.apply(callback.get()), budget);
			budget = null;
		}
		catch(Throwable failure) {
			fail(failure);
			cleanup();
		}
		finally {
			if(budget != null)
				budget.close();
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
}
