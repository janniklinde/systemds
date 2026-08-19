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

package org.apache.sysds.runtime.instructions.ooc;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.OffsetColumnIndex;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.primitives.RepartitionOOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCDimensions;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class AppendOOCInstruction extends BinaryOOCInstruction {

	public enum AppendType {
		CBIND, RBIND
	}

	protected final AppendType _type;

	protected AppendOOCInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, AppendType type,
		String opcode, String istr) {
		super(OOCType.Append, op, in1, in2, out, opcode, istr);
		_type = type;
	}

	public static AppendOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 5, 4);

		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[parts.length-2]);
		boolean cbind = Boolean.parseBoolean(parts[parts.length-1]);

		if(in1.getDataType() != Types.DataType.MATRIX || in2.getDataType() != Types.DataType.MATRIX) {
			throw new DMLRuntimeException("Only matrix-matrix cbind and rbind are supported");
		}
		AppendType type = cbind ? AppendType.CBIND : AppendType.RBIND;

		Operator op = new ReorgOperator(OffsetColumnIndex.getOffsetColumnIndexFnObject(-1));
		return new AppendOOCInstruction(op, in1, in2, out, type, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		bind(List.of(ec.getMatrixObject(input1), ec.getMatrixObject(input2)), ec.getMatrixObject(output),
			_type == AppendType.CBIND, getContext());
	}


	public static void bind(List<MatrixObject> inputs, MatrixObject output, boolean cbind, StreamContext context) {
		if(inputs.isEmpty())
			throw new IllegalArgumentException("Bind requires at least one input.");
		List<MatrixObject> contributing = new ArrayList<>(inputs.size());
		long concatenated = 0;
		long crossDimension = -1;
		long nonZeros = 0;
		for(MatrixObject input : inputs) {
			OOCDimensions.require("bind", input);
			DataCharacteristics dc = input.getDataCharacteristics();
			if((cbind ? dc.getCols() : dc.getRows()) == 0)
				continue;
			long own = cbind ? dc.getRows() : dc.getCols();
			if(crossDimension < 0)
				crossDimension = own;
			else if(crossDimension != own)
				throw new DMLRuntimeException("Bind is not possible for inputs with different number of "
					+ (cbind ? "rows: " : "columns: ") + crossDimension + " vs " + own);
			concatenated += cbind ? dc.getCols() : dc.getRows();
			nonZeros = nonZeros < 0 || dc.getNonZeros() < 0 ? -1 : nonZeros + dc.getNonZeros();
			contributing.add(input);
		}
		long rows = cbind ? Math.max(crossDimension, 0) : concatenated;
		long cols = cbind ? concatenated : Math.max(crossDimension, 0);

		if(contributing.isEmpty() || rows == 0 || cols == 0) {
			output.getDataCharacteristics().set(rows, cols, inputs.get(0).getBlocksize(), 0);
			OOCStream<IndexedMatrixValue> empty = new SubscribableTaskQueue<>();
			output.setStreamHandle(empty);
			empty.closeInput();
			return;
		}
		if(contributing.size() == 1) {
			output.getDataCharacteristics().set(rows, cols, contributing.get(0).getBlocksize(), nonZeros);
			output.setStreamHandle(contributing.get(0).getStreamHandle());
			return;
		}

		int outputBlocksize = contributing.get(0).getBlocksize();
		output.getDataCharacteristics().set(rows, cols, outputBlocksize, nonZeros);
		OOCStream<IndexedMatrixValue> result = new SubscribableTaskQueue<>();
		output.setStreamHandle(result);

		BindPlacement[] placements = new BindPlacement[contributing.size()];
		List<OOCStreamable<IndexedMatrixValue>> streams = new ArrayList<>(contributing.size());
		List<BiConsumer<IndexedMatrixValue, RepartitionOOCPrimitive.FragmentEmitter>> routers = new ArrayList<>(
			contributing.size());
		long rowOffset = 0;
		long colOffset = 0;
		for(int i = 0; i < placements.length; i++) {
			DataCharacteristics dc = contributing.get(i).getDataCharacteristics();
			BindPlacement placement = new BindPlacement(rowOffset, colOffset, dc.getRows(), dc.getCols(),
				dc.getBlocksize());
			placements[i] = placement;
			streams.add(contributing.get(i).getStreamable());
			routers.add((tile, emit) -> route(tile, placement, outputBlocksize, emit));
			rowOffset += cbind ? 0 : dc.getRows();
			colOffset += cbind ? dc.getCols() : 0;
		}
		OOCInstructionUtils.repartition(streams, result,
			outputIndex -> expectedFragments(outputIndex, placements, rows, cols, outputBlocksize), routers, context);
	}

	private static int expectedFragments(MatrixIndexes outputIndex, BindPlacement[] placements, long outputRows,
		long outputCols, int outputBlocksize) {
		long rowStart = (outputIndex.getRowIndex() - 1) * outputBlocksize;
		long rowEnd = Math.min(outputRows, rowStart + outputBlocksize);
		long colStart = (outputIndex.getColumnIndex() - 1) * outputBlocksize;
		long colEnd = Math.min(outputCols, colStart + outputBlocksize);
		long fragments = 0;
		for(BindPlacement placement : placements)
			fragments += placement.fragments(rowStart, rowEnd, colStart, colEnd);
		return Math.toIntExact(fragments);
	}

	private static void route(IndexedMatrixValue tile, BindPlacement placement, int outputBlocksize,
		RepartitionOOCPrimitive.FragmentEmitter emit) {
		MatrixBlock block = (MatrixBlock) tile.getValue();
		long inputRowStart = placement._rowOffset + (tile.getIndexes().getRowIndex() - 1) * placement._blocksize;
		long inputColStart = placement._colOffset + (tile.getIndexes().getColumnIndex() - 1) * placement._blocksize;
		long inputRowEnd = inputRowStart + block.getNumRows();
		long inputColEnd = inputColStart + block.getNumColumns();
		for(long outputRow = inputRowStart / outputBlocksize;
			outputRow <= (inputRowEnd - 1) / outputBlocksize;
			outputRow++)
			for(long outputCol = inputColStart / outputBlocksize;
				outputCol <= (inputColEnd - 1) / outputBlocksize;
				outputCol++) {
				long outputRowStart = outputRow * outputBlocksize;
				long outputColStart = outputCol * outputBlocksize;
				long rowStart = Math.max(inputRowStart, outputRowStart);
				long colStart = Math.max(inputColStart, outputColStart);
				int rows = (int) (Math.min(inputRowEnd, outputRowStart + outputBlocksize) - rowStart);
				int cols = (int) (Math.min(inputColEnd, outputColStart + outputBlocksize) - colStart);
				emit.copy(new MatrixIndexes(outputRow + 1, outputCol + 1), (int) (rowStart - inputRowStart),
					(int) (colStart - inputColStart), rows, cols, (int) (rowStart - outputRowStart),
					(int) (colStart - outputColStart));
			}
	}

	private static final class BindPlacement {
		private final long _rowOffset;
		private final long _colOffset;
		private final long _rows;
		private final long _cols;
		private final int _blocksize;

		private BindPlacement(long rowOffset, long colOffset, long rows, long cols, int blocksize) {
			_rowOffset = rowOffset;
			_colOffset = colOffset;
			_rows = rows;
			_cols = cols;
			_blocksize = blocksize;
		}

		private long fragments(long rowStart, long rowEnd, long colStart, long colEnd) {
			long localRowStart = Math.max(rowStart, _rowOffset) - _rowOffset;
			long localRowEnd = Math.min(rowEnd, _rowOffset + _rows) - _rowOffset;
			long localColStart = Math.max(colStart, _colOffset) - _colOffset;
			long localColEnd = Math.min(colEnd, _colOffset + _cols) - _colOffset;
			if(localRowStart >= localRowEnd || localColStart >= localColEnd)
				return 0;
			long rowFragments = (localRowEnd - 1) / _blocksize - localRowStart / _blocksize + 1;
			long colFragments = (localColEnd - 1) / _blocksize - localColStart / _blocksize + 1;
			return Math.multiplyExact(rowFragments, colFragments);
		}
	}
}
