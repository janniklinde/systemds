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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.ScalarOperator;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.util.OOCDimensions;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class BinaryOOCInstruction extends ComputationOOCInstruction {
	
	protected BinaryOOCInstruction(OOCType type, Operator bop, 
			CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) {
		super(type, bop, in1, in2, out, opcode, istr);
	}

	public static BinaryOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 3);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[3]);
		Operator bop = InstructionUtils.parseExtendedBinaryOrBuiltinOperator(opcode, in1, in2);
		
		return new BinaryOOCInstruction(
			OOCType.Binary, bop, in1, in2, out, opcode, str);
	}
	
	@Override
	public void processInstruction( ExecutionContext ec ) {
		if (input1.isMatrix() && input2.isMatrix())
			processMatrixMatrixInstruction(ec);
		else
			processScalarMatrixInstruction(ec);
	}

	protected void processMatrixMatrixInstruction(ExecutionContext ec) {
		MatrixObject m1 = ec.getMatrixObject(input1);
		MatrixObject m2 = ec.getMatrixObject(input2);

		OOCStream<IndexedMatrixValue> qOut = new SubscribableTaskQueue<>();

		final boolean known1 = OOCDimensions.known(m1);
		final boolean known2 = OOCDimensions.known(m2);
		if(known1 && known2)
			OOCInstructionUtils.propagateDims(ec, output, Math.max(m1.getNumRows(), m2.getNumRows()),
				Math.max(m1.getNumColumns(), m2.getNumColumns()), m1.getBlocksize(), -1);
		ec.getMatrixObject(output).setStreamHandle(qOut);

		// Unknown dimensions cannot distinguish a broadcast from a strict key-wise operation. Preserve the existing
		// key-wise semantics without entering the legacy cache pipeline.
		if(!known1 || !known2) {
			OOCInstructionUtils.equiJoin(m1.getStreamable(), m2.getStreamable(), qOut,
				(left, right) -> left.binaryOperations((BinaryOperator) _optr, right, new MatrixBlock()), getContext());
			return;
		}

		boolean isColBroadcast = m1.getNumColumns() > 1 && m2.getNumColumns() == 1;
		boolean isRowBroadcast = m1.getNumRows() > 1 && m2.getNumRows() == 1;

		if (isColBroadcast && !isRowBroadcast) {
			int broadcastBlocks = Math.toIntExact(m2.getDataCharacteristics().getNumRowBlocks());
			int usesPerBlock = Math.toIntExact(m1.getDataCharacteristics().getNumColBlocks());
			OOCInstructionUtils.indexedBroadcastMap(m1.getStreamable(), m2.getStreamable(), qOut,
				tmp -> tmp.getIndexes().getRowIndex(), tmp -> 1,
				() -> new CountingLiveness(broadcastBlocks, usesPerBlock), (tmp, broadcast) -> {
					IndexedMatrixValue tmpOut = new IndexedMatrixValue();
					tmpOut.set(tmp.getIndexes(), tmp.getValue().binaryOperations((BinaryOperator) _optr,
						broadcast.getValue(), tmpOut.getValue()));
					return tmpOut;
				}, getContext());
		}
		else if (isRowBroadcast && !isColBroadcast) {
			int broadcastBlocks = Math.toIntExact(m2.getDataCharacteristics().getNumColBlocks());
			int usesPerBlock = Math.toIntExact(m1.getDataCharacteristics().getNumRowBlocks());
			OOCInstructionUtils.indexedBroadcastMap(m1.getStreamable(), m2.getStreamable(), qOut, tmp -> 1,
				tmp -> tmp.getIndexes().getColumnIndex(), () -> new CountingLiveness(broadcastBlocks, usesPerBlock),
				(tmp, broadcast) -> {
					IndexedMatrixValue tmpOut = new IndexedMatrixValue();
					tmpOut.set(tmp.getIndexes(), tmp.getValue().binaryOperations((BinaryOperator) _optr,
						broadcast.getValue(), tmpOut.getValue()));
					return tmpOut;
				}, getContext());
		}
		else {
			if (m1.getNumColumns() != m2.getNumColumns() || m1.getNumRows() != m2.getNumRows())
				throw new NotImplementedException("Invalid dimensions for matrix-matrix binary op: "
					+ m1.getNumRows() + "x" + m1.getNumColumns() + " <=> "
					+ m2.getNumRows() + "x" + m2.getNumColumns());

			OOCInstructionUtils.equiJoin(m1.getStreamable(), m2.getStreamable(), qOut,
				(left, right) -> left.binaryOperations((BinaryOperator) _optr, right, new MatrixBlock()), getContext());
		}
	}

	protected void processScalarMatrixInstruction(ExecutionContext ec) {
		//get operator and scalar
		CPOperand scalar = input1.isMatrix() ? input2 : input1;
		ScalarObject constant = ec.getScalarInput(scalar);
		ScalarOperator sc_op = ((ScalarOperator)_optr).setConstant(constant.getDoubleValue());

		//create thread and process binary operation
		MatrixObject min = ec.getMatrixObject(input1.isMatrix() ? input1 : input2);
		OOCStream<IndexedMatrixValue> qOut = createWritableStream();
		OOCInstructionUtils.propagateDims(ec, output, min.getNumRows(), min.getNumColumns(), min.getBlocksize(), -1);
		ec.getMatrixObject(output).setStreamHandle(qOut);
		OOCInstructionUtils.equiMapBlock(min.getStreamable(), qOut,
			block -> block.scalarOperations(sc_op, new MatrixBlock()), getContext());
	}
}
