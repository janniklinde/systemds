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

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.lops.WeightedDivMM.WDivMMType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.matrix.operators.QuaternaryOperator;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class WDivMMOOCInstruction extends QuaternaryOOCInstruction {

	protected WDivMMOOCInstruction(QuaternaryOperator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand in4,
		CPOperand out, String opcode, String istr) {
		super(op, in1, in2, in3, in4, out, opcode, istr);
	}

	public static WDivMMOOCInstruction parseInstruction(QuaternaryOOCInstruction instr) {
		String instrStr = instr.getInstructionString();
		String opcode = InstructionUtils.getInstructionPartsWithValueType(instr.getInstructionString())[0];
		return new WDivMMOOCInstruction((QuaternaryOperator) instr.getOperator(), instr.input1, instr.input2,
			instr.input3, instr.input4, instr.output, opcode, instrStr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		QuaternaryOperator operator = (QuaternaryOperator) _optr;
		WDivMMType type = operator.wtype3;
		MatrixObject x = ec.getMatrixObject(input1);
		MatrixObject u = ec.getMatrixObject(input2);
		MatrixObject v = ec.getMatrixObject(input3);
		MatrixObject w = type.hasFourInputs() && !type.hasScalar() ? ec.getMatrixObject(input4) : null;
		long rank = u.getDataCharacteristics().colsKnown() ? u.getNumColumns() : v.getNumColumns();
		if(!x.getDataCharacteristics().dimsKnown() || x.getNumRows() <= 0 || x.getNumColumns() <= 0 ||
			x.getBlocksize() <= 0 || rank <= 0)
			throw new DMLRuntimeException("Planner-backed WDivMM requires known, positive matrix dimensions and rank.");
		u.getDataCharacteristics().set(x.getNumRows(), rank, x.getBlocksize(), u.getNnz());
		v.getDataCharacteristics().set(x.getNumColumns(), rank, x.getBlocksize(), v.getNnz());
		if(w != null)
			w.getDataCharacteristics().set(x.getNumRows(), x.getNumColumns(), x.getBlocksize(), w.getNnz());
		processPlannerInstruction(ec, type, x, u, v, w);
	}

	private void processPlannerInstruction(ExecutionContext ec, WDivMMType type, MatrixObject x, MatrixObject u,
		MatrixObject v, MatrixObject w) {
		int blocksize = x.getBlocksize();
		AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator multiply = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate);
		BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		BinaryOperator minus = InstructionUtils.parseBinaryOperator(Opcodes.MINUS.toString());
		BinaryOperator times = InstructionUtils.parseBinaryOperator(Opcodes.MULT.toString());
		BinaryOperator divide = InstructionUtils.parseBinaryOperator(Opcodes.DIV.toString());
		OOCStreamable<IndexedMatrixValue> sharedX = x.getStreamable();
		OOCStreamable<IndexedMatrixValue> sharedU = u.getStreamable();
		OOCStreamable<IndexedMatrixValue> sharedV = v.getStreamable();
		MaterializedStoreStreamable createdX = null;
		MaterializedStoreStreamable createdV = null;
		if(type.isMinus() && !type.hasFourInputs() && !sharedX.hasMaterializedStore()) {
			createdX = new MaterializedStoreStreamable(x.getStreamHandle(), x);
			sharedX = createdX;
		}
		if(type.isRight() && !sharedV.hasMaterializedStore()) {
			createdV = new MaterializedStoreStreamable(v.getStreamHandle(), v, OOCStoreLayout.COL_MAJOR);
			sharedV = createdV;
		}

		OOCStream<IndexedMatrixValue> vt = createWritableStream(v.getNumColumns(), v.getNumRows(), blocksize);
		OOCInstructionUtils.transpose(sharedV, vt, getContext());
		OOCStream<IndexedMatrixValue> product = createWritableStream(x);
		OOCInstructionUtils.matrixMultiply(sharedU, vt, product, multiply, plus, getContext());

		if(type.isBasic()) {
			ec.getDataCharacteristics(output.getName()).set(x.getNumRows(), x.getNumColumns(), blocksize, -1);
			OOCStream<IndexedMatrixValue> out = plannerElement(sharedX, product, x, times);
			ec.getMatrixObject(output).setStreamHandle(out);
			if(createdX != null)
				createdX.scheduleMaterializedStoreDeletion();
			if(createdV != null)
				createdV.scheduleMaterializedStoreDeletion();
			return;
		}

		OOCStream<IndexedMatrixValue> intermediate;
		if(type.hasFourInputs()) {
			if(type.hasScalar()) {
				double epsilon = ec.getScalarInput(input4).getDoubleValue();
				RightScalarOperator add = new RightScalarOperator(Plus.getPlusFnObject(), epsilon);
				OOCStream<IndexedMatrixValue> adjusted = createWritableStream(x);
				OOCInstructionUtils.equiMapBlock(product, adjusted,
					block -> block.scalarOperations(add, new MatrixBlock()), getContext());
				intermediate = plannerElement(sharedX, adjusted, x, divide);
			}
			else {
				OOCStream<IndexedMatrixValue> difference = plannerElement(product, w.getStreamable(), x, minus);
				intermediate = plannerElement(sharedX, difference, x, times);
			}
		}
		else if(type.isMinus()) {
			OOCStream<IndexedMatrixValue> difference = plannerElement(product, sharedX, x, minus);
			OOCStream<IndexedMatrixValue> masked = createWritableStream(x);
			OOCInstructionUtils.equiJoin(sharedX, difference, masked, (mask, block) -> {
				MatrixBlock result = new MatrixBlock(block);
				return mask(mask, result);
			}, getContext());
			intermediate = masked;
		}
		else
			intermediate = plannerElement(sharedX, product, x, type.isMult() ? times : divide);

		long outputRows = type.isLeft() ? x.getNumColumns() : x.getNumRows();
		long outputCols = u.getNumColumns();
		ec.getDataCharacteristics(output.getName()).set(outputRows, outputCols, blocksize, -1);
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		ec.getMatrixObject(output).setStreamHandle(out);
		if(type.isLeft()) {
			OOCStream<IndexedMatrixValue> transposed = createWritableStream(x.getNumColumns(), x.getNumRows(),
				blocksize);
			OOCInstructionUtils.transpose(intermediate, transposed, getContext());
			OOCInstructionUtils.matrixMultiply(transposed, sharedU, out, multiply, plus, getContext());
		}
		else
			OOCInstructionUtils.matrixMultiply(intermediate, sharedV, out, multiply, plus, getContext());
		if(createdX != null)
			createdX.scheduleMaterializedStoreDeletion();
		if(createdV != null)
			createdV.scheduleMaterializedStoreDeletion();
	}

	private OOCStream<IndexedMatrixValue> plannerElement(OOCStreamable<IndexedMatrixValue> left,
		OOCStreamable<IndexedMatrixValue> right, MatrixObject metadata, BinaryOperator operator) {
		OOCStream<IndexedMatrixValue> out = createWritableStream(metadata);
		OOCInstructionUtils.equiJoin(left, right, out,
			(leftBlock, rightBlock) -> leftBlock.binaryOperations(operator, rightBlock, new MatrixBlock()),
			getContext());
		return out;
	}

	private MatrixBlock mask(MatrixBlock mask, MatrixBlock blk) {
		for(int i = 0; i < blk.getNumRows(); i++) {
			for(int j = 0; j < blk.getNumColumns(); j++) {
				if(mask.get(i,j) ==0) blk.set(i, j, 0);
			}
		}
		return blk;
	}
}
