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

import java.util.function.Function;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCTileOperation;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class MappingOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _output;
	private final Function<IndexedMatrixValue, MatrixBlock> _operation;
	private final OOCTileOperation _tileOperation;

	public MappingOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<IndexedMatrixValue> output,
		Function<IndexedMatrixValue, MatrixBlock> operation, StreamContext context) {
		this(input, output, operation,
			new OOCTileOperation(OOCTileOperation.denseOutput(), OOCTileOperation.Relation.EQUI), context);
	}

	public MappingOOCPrimitive(OOCStreamable<IndexedMatrixValue> input, OOCStreamable<IndexedMatrixValue> output,
		Function<IndexedMatrixValue, MatrixBlock> operation, OOCTileOperation tileOperation, StreamContext context) {
		super(context, input);
		_output = output;
		_operation = operation;
		_tileOperation = tileOperation;
		setTileOperation(tileOperation);
	}

	public Function<IndexedMatrixValue, MatrixBlock> getOperation() {
		return _operation;
	}

	public OOCStreamable<IndexedMatrixValue> getOutput() {
		return _output;
	}

	@Override
	public long getMaxTaskReservationBytes(IndexedMatrixValue... inputs) {
		DataCharacteristics dc = _output.getDataCharacteristics();
		int blocksize = dc != null && dc.getBlocksize() > 0 ? dc.getBlocksize() : 1000;
		IndexedMatrixValue input = inputs.length == 0 ? null : inputs[0];
		long rows = input == null ? dc == null || !dc.dimsKnown() ? blocksize : Math.min(dc.getRows(),
			blocksize) : input.getValue().getNumRows();
		long cols = input == null ? dc == null || !dc.dimsKnown() ? blocksize : Math.min(dc.getCols(),
			blocksize) : input.getValue().getNumColumns();
		long cells = rows * cols;
		long inputNnz = input == null ? -1 : input.getValue().getNonZeros();
		long outputNnz = _tileOperation.worstCaseOutputNnz(new long[] {inputNnz}, cells);
		return MatrixBlock.estimateSizeInMemory(rows, cols, outputNnz);
	}

	@Override
	protected void inferPatternsInternal() {
		OOCPrimitive dependency = getInputDependency(0);
		OOCAccessPattern inputPattern = dependency == null ? OOCAccessPattern.ANY : dependency.getAccessPattern();
		_pattern = _pattern.preferred(inputPattern);
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = _pattern.preferred(accessPattern);
		OOCPrimitive dependency = getInputDependency(0);
		if(dependency != null)
			dependency.requestPattern(accessPattern);
	}

	@Override
	protected void startExecution() {
		OOCStream<IndexedMatrixValue> input = getInputReadStream(0);
		OOCStream<IndexedMatrixValue> output = _output.getWriteStream();
		AllocatedOOCStream<IndexedMatrixValue> admitted = new AllocatedOOCStream<>(input, _allowance,
			value -> getMaxTaskReservationBytes(value), true);
		getContext().addOutStream(output);
		OOCInstructionUtils.submitOOCTasks(admitted, callback -> {
			ReservationBudget budget = AllocatedOOCStream.detachBudget(callback);
			try {
				if(budget == null)
					throw new IllegalStateException("Missing admitted mapping output budget");
				IndexedMatrixValue value = callback.get();
				prepareOutput(output, callback, new IndexedMatrixValue(value.getIndexes(), _operation.apply(value)),
					budget);
				budget = null;
			}
			finally {
				if(budget != null)
					budget.close();
			}
		}, getContext()).whenComplete((ignored, error) -> {
			try {
				if(error != null)
					fail(error);
				output.closeInput();
			}
			finally {
				onComplete();
			}
		});
	}
}
