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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.store.OOCStreamMaterializer;
import org.apache.sysds.runtime.ooc.stream.SourceOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

/**
 * Bounded binary source reader. Its output is an internal handoff stream which must be attached directly to a
 * materialization boundary; source-backed callbacks deliberately reject ordinary primitive access.
 */
public final class SourceReadOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final String _path;
	private final long _rows;
	private final long _cols;
	private final int _blocksize;
	private final long _nonZeros;
	private final long _bulkBytes;
	private final long _productionLimit;
	private final SourceOOCStream _ioOutput;
	private final AtomicReference<ReservationBudget> _activeBudget;
	private final AtomicBoolean _finished;
	private OOCIOHandler.SourceReadContinuation _continuation;
	private OOCStream<IndexedMatrixValue> _outputStream;

	public SourceReadOOCPrimitive(OOCStreamable<IndexedMatrixValue> output, String path, long rows, long cols,
		int blocksize, long nonZeros, long bulkBytes, long productionLimit, StreamContext context) {
		super(context);
		_output = output;
		_path = path;
		_rows = rows;
		_cols = cols;
		_blocksize = blocksize;
		_nonZeros = nonZeros;
		_bulkBytes = bulkBytes;
		_productionLimit = productionLimit;
		_ioOutput = new SourceOOCStream(false);
		_activeBudget = new AtomicReference<>();
		_finished = new AtomicBoolean();
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.UNKNOWN;
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = OOCAccessPattern.UNKNOWN;
	}

	@Override
	public long getMaxTaskReservationBytes(IndexedMatrixValue... inputs) {
		return _bulkBytes;
	}

	@Override
	protected void startExecution() {
		_outputStream = _output.getWriteStream();
		getContext().addOutStream(_outputStream);
		_ioOutput.setSubscriber(this::emit);
		produceNext();
	}

	private void produceNext() {
		_allowance.reserveAsync(_bulkBytes).whenComplete((ignored, admissionError) -> {
			if(admissionError != null) {
				failAndFinish(admissionError);
				return;
			}
			ReservationBudget phase = new ReservationBudget(_allowance, _bulkBytes);
			if(!_activeBudget.compareAndSet(null, phase)) {
				phase.close();
				failAndFinish(new IllegalStateException("Overlapping source read phases."));
				return;
			}
			OOCInstructionUtils.submitOOCTask(() -> readPhase(phase),
				new StreamContext(getContext().getCallerId(), getContext().getExtendedOpcode()));
		});
	}

	private void readPhase(ReservationBudget phase) {
		try {
			OOCIOHandler io = OOCCacheManager.getGlobalCache().getIOHandler();
			OOCIOHandler.SourceReadResult result;
			if(_continuation == null) {
				OOCIOHandler.SourceReadRequest request = new OOCIOHandler.SourceReadRequest(_path,
					Types.FileFormat.BINARY, _rows, _cols, _blocksize, _nonZeros, _productionLimit, true, _ioOutput);
				result = io.scheduleSourceRead(request).get();
			}
			else
				result = io.continueSourceRead(_continuation, _productionLimit).get();
			_continuation = result.continuation;
			if(result.eof)
				finish();
		}
		catch(Throwable failure) {
			failAndFinish(failure);
		}
		finally {
			_activeBudget.compareAndSet(phase, null);
			phase.close();
			if(!_finished.get())
				produceNext();
		}
	}

	private void emit(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(callback.isEos()) {
			callback.close();
			return;
		}
		try(callback) {
			ReservationBudget phase = _activeBudget.get();
			if(phase == null)
				throw new IllegalStateException("Source value emitted outside an admitted read phase.");
			List<IndexedMatrixValue> values = new ArrayList<>();
			OOCIOHandler.SourceBlockDescriptor descriptor;
			if(callback instanceof SourceOOCStream.SourceGroupCallback group) {
				descriptor = group.getDescriptor();
				for(int i = 0; i < group.size(); i++)
					try(OOCStream.QueueCallback<IndexedMatrixValue> item = group.getCallback(i)) {
						values.add(item.get());
					}
			}
			else {
				IndexedMatrixValue value = callback.get();
				values.add(value);
				descriptor = _ioOutput.getDescriptor(value.getIndexes());
			}
			long bytes = 0;
			for(IndexedMatrixValue value : values)
				bytes = Math.addExact(bytes, OOCUtils.memoryCharge(value));
			phase.reserveBlocking(bytes);
			ReservationBudget ownership = new ReservationBudget(phase, bytes);
			OOCStream.QueueCallback<IndexedMatrixValue> source = null;
			boolean handedOff = false;
			try {
				source = OOCStreamMaterializer.sourceBackedCallback(values, descriptor, ownership);
				_outputStream.enqueue(source);
				handedOff = true;
			}
			finally {
				if(!handedOff) {
					if(source != null)
						source.close();
					else
						ownership.close();
				}
			}
		}
		catch(Throwable failure) {
			failAndFinish(failure);
			throw DMLRuntimeException.of(failure);
		}
	}

	private void finish() {
		if(_finished.compareAndSet(false, true)) {
			_outputStream.closeInput();
			onComplete();
		}
	}

	private void failAndFinish(Throwable failure) {
		if(fail(DMLRuntimeException.of(failure)))
			finish();
	}
}
