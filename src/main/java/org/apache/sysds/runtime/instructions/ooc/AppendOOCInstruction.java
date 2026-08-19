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
import org.apache.sysds.runtime.ooc.primitives.RepartitionOOCPrimitive;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.List;

public class AppendOOCInstruction extends BinaryOOCInstruction {

	public enum AppendType {
		CBIND
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

		if(in1.getDataType() != Types.DataType.MATRIX || in2.getDataType() != Types.DataType.MATRIX || !cbind){
			throw new DMLRuntimeException("Only matrix-matrix cbind is supported");
		}
		AppendType type = AppendType.CBIND;

		Operator op = new ReorgOperator(OffsetColumnIndex.getOffsetColumnIndexFnObject(-1));
		return new AppendOOCInstruction(op, in1, in2, out, type, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject in1 = ec.getMatrixObject(input1);
		MatrixObject in2 = ec.getMatrixObject(input2);
		validateInput(in1, in2);
		if(handleZeroDims(in1, in2, ec))
			return;

		if(!in1.getDataCharacteristics().dimsKnown() || !in2.getDataCharacteristics().dimsKnown() ||
			in1.getBlocksize() <= 0 || in2.getBlocksize() <= 0)
			throw new DMLRuntimeException(
				"Planner-backed OOC append requires known dimensions and positive block sizes.");

		int outputBlocksize = in1.getBlocksize();
		int rightBlocksize = in2.getBlocksize();
		long rows = in1.getNumRows();
		long cols1 = in1.getNumColumns();
		long cols2 = in2.getNumColumns();
		long nonZeros = in1.getNnz() >= 0 && in2.getNnz() >= 0 ? in1.getNnz() + in2.getNnz() : -1;
		ec.getDataCharacteristics(output.getName()).set(rows, cols1 + cols2, outputBlocksize, nonZeros);
		OOCStream<IndexedMatrixValue> result = createWritableStream();
		ec.getMatrixObject(output).setStreamHandle(result);
		OOCInstructionUtils.repartition(List.of(in1.getStreamable(), in2.getStreamable()), result,
			outputIndex -> expectedFragments(outputIndex, rows, cols1, cols2, outputBlocksize, rightBlocksize),
			List.of((tile, emit) -> route(tile, 0, outputBlocksize, outputBlocksize, emit),
				(tile, emit) -> route(tile, cols1, rightBlocksize, outputBlocksize, emit)),
			getContext());
	}

	private void validateInput(MatrixObject m1, MatrixObject m2) {
		if(_type == AppendType.CBIND && m1.getNumRows() >= 0 && m2.getNumRows() >= 0 &&
			m1.getNumRows() != m2.getNumRows()) {
			throw new DMLRuntimeException(
				"Append-cbind is not possible for input matrices " + input1.getName() + " and " + input2.getName()
					+ " with different number of rows: " + m1.getNumRows() + " vs " + m2.getNumRows());
		}
	}

	private boolean handleZeroDims(MatrixObject m1, MatrixObject m2, ExecutionContext ec) {
		long rows = m1.getNumRows();
		long cols1 = m1.getNumColumns();
		long cols2 = m2.getNumColumns();
		if(rows == 0 || (cols1 == 0 && cols2 == 0)) {
			OOCStream<IndexedMatrixValue> empty = createWritableStream();
			empty.closeInput();
			ec.getMatrixObject(output).setStreamHandle(empty);
		}
		else if(cols1 == 0) {
			ec.getMatrixObject(output).setStreamHandle(m2.getStreamHandle());
		}
		else if(cols2 == 0) {
			ec.getMatrixObject(output).setStreamHandle(m1.getStreamHandle());
		}
		else return false;

		return true;
	}

	private static int expectedFragments(MatrixIndexes outputIndex, long rows, long cols1, long cols2,
		int outputBlocksize, int rightBlocksize) {
		long rowStart = (outputIndex.getRowIndex() - 1) * outputBlocksize;
		long rowEnd = Math.min(rows, rowStart + outputBlocksize);
		long colStart = (outputIndex.getColumnIndex() - 1) * outputBlocksize;
		long colEnd = Math.min(cols1 + cols2, colStart + outputBlocksize);
		long fragments = sourceFragments(rowStart, rowEnd, colStart, Math.min(cols1, colEnd), outputBlocksize, 0);
		fragments += sourceFragments(rowStart, rowEnd, Math.max(cols1, colStart), colEnd, rightBlocksize, cols1);
		return Math.toIntExact(fragments);
	}

	private static long sourceFragments(long rowStart, long rowEnd, long colStart, long colEnd, int blocksize,
		long columnOffset) {
		if(rowStart >= rowEnd || colStart >= colEnd)
			return 0;
		long localColStart = colStart - columnOffset;
		long localColEnd = colEnd - columnOffset;
		long rowFragments = (rowEnd - 1) / blocksize - rowStart / blocksize + 1;
		long colFragments = (localColEnd - 1) / blocksize - localColStart / blocksize + 1;
		return Math.multiplyExact(rowFragments, colFragments);
	}

	private static void route(IndexedMatrixValue tile, long columnOffset, int inputBlocksize, int outputBlocksize,
		RepartitionOOCPrimitive.FragmentEmitter emit) {
		MatrixBlock block = (MatrixBlock) tile.getValue();
		long inputRowStart = (tile.getIndexes().getRowIndex() - 1) * inputBlocksize;
		long inputColStart = columnOffset + (tile.getIndexes().getColumnIndex() - 1) * inputBlocksize;
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
}
