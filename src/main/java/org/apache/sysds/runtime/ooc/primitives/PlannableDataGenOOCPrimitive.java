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
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class PlannableDataGenOOCPrimitive extends PlannableOOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _outputStream;
	private final StreamContext _sc;
	private final Function<MatrixIndexes, MatrixBlock> _fn;

	public PlannableDataGenOOCPrimitive(OOCStreamable<IndexedMatrixValue> outputStream, Function<MatrixIndexes, MatrixBlock> fn, StreamContext sc) {
		super(Collections.emptyList());
		_outputStream = outputStream;
		_sc = sc;
		_fn = fn;
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return Collections.emptyList();
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStream);
	}

	@Override
	public boolean isEmissionControlled() {
		return true;
	}

	@Override
	public boolean isTileLocal() {
		return true;
	}

	@Override
	public long getDenseTileMemoryFactor() {
		return 1;
	}

	@Override
	public void inferPatterns() {
		if(_pattern == OOCAccessPattern.UNSET)
			_pattern = OOCAccessPattern.ANY;
		inferPatterns(getParents());
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		_pattern = accessPattern;
	}

	@Override
	public void startExecution() {
		final OOCStream<MatrixIndexes> stream = new SubscribableTaskQueue<>();
		final OOCStream<IndexedMatrixValue> out = _outputStream.getWriteStream();
		new Thread(() -> {
			for(MatrixIndexes ix : OOCUtils.getAccessPattern(_outputStream.getDataCharacteristics(), _pattern)) {
				_allowance.reserveBlocking(_allocFn.applyAsLong(ix));
				stream.enqueue(ix);
			}
			stream.closeInput();
		}).start();

		if(_crossBoundaries) {
			OOCInstructionUtils.submitOOCTasks(stream, cb -> {
				var imv = new IndexedMatrixValue(cb.get(), _fn.apply(cb.get()));
				var cbOut = new InMemoryQueueCallback(imv, null, _allowance, _allocFn.applyAsLong(cb.get()));
				out.enqueue(cbOut);
			}, _sc).thenRun(out::closeInput).exceptionally(t -> {
				out.propagateFailure(DMLRuntimeException.of(t));
				return null;
			}).thenRun(() -> out.getPrimitive().onComplete());
		}
		else {
			OOCInstructionUtils.submitOOCTasks(stream, cb -> {
				var imv = new IndexedMatrixValue(cb.get(), _fn.apply(cb.get()));
				var cbOut = new OOCStream.SimpleQueueCallback<>(imv, null);
				out.enqueue(cbOut);
			}, _sc).thenRun(out::closeInput).exceptionally(t -> {
				out.propagateFailure(DMLRuntimeException.of(t));
				return null;
			}).thenRun(() -> out.getPrimitive().onComplete());
		}
	}
}
