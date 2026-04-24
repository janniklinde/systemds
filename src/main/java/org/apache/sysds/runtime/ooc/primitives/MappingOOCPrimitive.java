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
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.List;
import java.util.function.Function;

public class MappingOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _inputStreamable;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final Function<MatrixBlock, MatrixBlock> _fn;
	private final StreamContext _sc;

	private MappingOOCPrimitive(OOCPrimitive inputPrimitive, OOCStreamable<IndexedMatrixValue> inputStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, Function<MatrixBlock, MatrixBlock> fn, StreamContext sc) {
		super(inputPrimitive == null ? List.of() : List.of(inputPrimitive));
		_inputStreamable = inputStreamable;
		_outputStreamable = outputStreamable;
		_fn = fn;
		_sc = sc;
	}

	public MappingOOCPrimitive(OOCStreamable<IndexedMatrixValue> inputStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, Function<MatrixBlock, MatrixBlock> fn, StreamContext sc) {
		this(inputStreamable.getPrimitive(), inputStreamable, outputStreamable, fn, sc);
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return List.of(_inputStreamable);
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStreamable);
	}

	@Override
	public boolean isTileLocal() {
		return true;
	}

	@Override
	public boolean isOneToOne() {
		return true;
	}

	@Override
	public boolean isIndexPreserving() {
		return true;
	}

	@Override
	public long getDenseTileMemoryFactor() {
		return 2;
	}

	@Override
	public void inferPatterns() {
		_pattern = _pattern.preferred(getPattern(_inputStreamable));
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = _pattern.preferred(accessPattern);
		if(!getChildren().isEmpty())
			getChildren().get(0).requestPattern(accessPattern);
	}

	@Override
	public void startExecution() {
		final OOCStream<IndexedMatrixValue> in = _inputStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();

		if(_crossBoundaries) {
			OOCInstructionUtils.submitOOCTasks(in, cb -> {
				InMemoryQueueCallback cbOut;
				try(cb) {
					var imv = new IndexedMatrixValue(cb.get().getIndexes(),
						_fn.apply((MatrixBlock) cb.get().getValue()));
					cbOut = new InMemoryQueueCallback(imv, null, _allowance,
						_allocFn.applyAsLong(cb.get().getIndexes()));
				}
				out.enqueue(cbOut);
			}, _sc).thenRun(out::closeInput).exceptionally(t -> {
				out.propagateFailure(DMLRuntimeException.of(t));
				return null;
			}).thenRun(() -> out.getPrimitive().onComplete());
		}
		else {
			OOCInstructionUtils.submitOOCTasks(in, cb -> {
				OOCStream.QueueCallback<IndexedMatrixValue> cbOut;
				try(cb) {
					var imv = new IndexedMatrixValue(cb.get().getIndexes(),
						_fn.apply((MatrixBlock) cb.get().getValue()));
					cbOut = new OOCStream.SimpleQueueCallback<>(imv, null);
				}
				out.enqueue(cbOut);
			}, _sc).thenRun(out::closeInput).exceptionally(t -> {
				out.propagateFailure(DMLRuntimeException.of(t));
				return null;
			}).thenRun(() -> out.getPrimitive().onComplete());
		}
	}
}
