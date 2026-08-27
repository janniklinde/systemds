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
import org.apache.sysds.lops.MapMultChain.ChainType;
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
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.ooc.util.OOCDimensions;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class MapMMChainOOCInstruction extends ComputationOOCInstruction {
	private final ChainType _type;

	protected MapMMChainOOCInstruction(OOCType type, Operator op, CPOperand in1, CPOperand in2, CPOperand in3,
		CPOperand out, ChainType chainType, String opcode, String istr) {
		super(type, op, in1, in2, in3, out, opcode, istr);
		_type = chainType;
	}

	public static MapMMChainOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 4, 5);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);

		if (parts.length == 5) {
			CPOperand out = new CPOperand(parts[3]);
			ChainType type = ChainType.valueOf(parts[4]);
			return new MapMMChainOOCInstruction(OOCType.MAPMMCHAIN, null, in1, in2, null, out, type, opcode, str);
		}
		else { //parts.length==6
			CPOperand in3 = new CPOperand(parts[3]);
			CPOperand out = new CPOperand(parts[4]);
			ChainType type = ChainType.valueOf(parts[5]);
			return new MapMMChainOOCInstruction(OOCType.MAPMMCHAIN, null, in1, in2, in3, out, type, opcode, str);
		}
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject x = ec.getMatrixObject(input1);
		MatrixObject v = ec.getMatrixObject(input2);
		MatrixObject w = _type.isWeighted() ? ec.getMatrixObject(input3) : null;
		boolean hasV = !v.getDataCharacteristics().rowsKnown() || v.getNumRows() > 0;
		if(!hasV && _type != ChainType.XtXvy)
			throw new DMLRuntimeException("MMChain requires non-empty v for chain type " + _type);
		OOCDimensions.require(getOpcode(), x);
		if(x.getNumRows() <= 0 || x.getNumColumns() <= 0)
			throw new DMLRuntimeException("Planner-backed MMChain requires a non-empty matrix.");
		if(hasV)
			v.getDataCharacteristics().set(x.getNumColumns(), 1, x.getBlocksize(), v.getNnz());
		if(w != null)
			w.getDataCharacteristics().set(x.getNumRows(), 1, x.getBlocksize(), w.getNnz());
		processPlannerInstruction(ec, x, v, w, hasV);
	}

	private void processPlannerInstruction(ExecutionContext ec, MatrixObject x, MatrixObject v, MatrixObject w,
		boolean hasV) {
		int blocksize = x.getBlocksize();
		BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator multiply = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate);
		BinaryOperator weight = _type.isWeighted() ? InstructionUtils
			.parseBinaryOperator(_type == ChainType.XtwXv ? Opcodes.MULT.toString() : Opcodes.MINUS.toString()) : null;

		OOCStream<IndexedMatrixValue> xtPartials = createWritableStream(x.getNumColumns(), x.getNumRows(), blocksize);
		OOCInstructionUtils.mmChain(x.getStreamable(), hasV ? v.getStreamable() : null,
			_type.isWeighted() ? w.getStreamable() : null, xtPartials, _type, multiply, plus, weight, getContext());

		ec.getDataCharacteristics(output.getName()).set(x.getNumColumns(), 1, blocksize, -1);
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		ec.getMatrixObject(output).setStreamHandle(out);
		OOCInstructionUtils.rowGroupedReduce(xtPartials, out,
			(left, right) -> left.binaryOperations(plus, right, new MatrixBlock()), getContext());
	}
}
