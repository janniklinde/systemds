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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.cp.CmCovObject;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.CentralMomentCPInstruction;
import org.apache.sysds.runtime.instructions.cp.DoubleObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.CMOperator;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class CentralMomentOOCInstruction extends AggregateUnaryOOCInstruction {

	private CentralMomentOOCInstruction(CMOperator cm, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
		String opcode, String str) {
		super(OOCType.CM, cm, in1, in2, in3, out, opcode, str);
	}

	public static CentralMomentOOCInstruction parseInstruction(String str) {
		CentralMomentCPInstruction cpInst = CentralMomentCPInstruction.parseInstruction(str);
		return parseInstruction(cpInst);
	}

	public static CentralMomentOOCInstruction parseInstruction(CentralMomentCPInstruction inst) {
		return new CentralMomentOOCInstruction((CMOperator) inst.getOperator(), inst.input1, inst.input2, inst.input3,
			inst.output, inst.getOpcode(), inst.getInstructionString());
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		String outputName = output.getName();

		/*
		 * The "order" of the central moment in the instruction can
		 * be set to INVALID when the exact value is unknown at
		 * compilation time. We first need to determine the exact
		 * order and update the CMOperator, if needed.
		 */

		MatrixObject matObj = ec.getMatrixObject(input1.getName());

		CPOperand scalarInput = (input3 == null ? input2 : input3);
		ScalarObject order = ec.getScalarInput(scalarInput);

		CMOperator cm_op = ((CMOperator) _optr);
		if(cm_op.getAggOpType() == CMOperator.AggregateOperationTypes.INVALID)
			cm_op = cm_op.setCMAggOp((int) order.getLongValue());

		CMOperator finalCmOp = cm_op;
		OOCStream<CmCovObject> result = createWritableStream(4, 4, 4);
		if(input3 == null)
			OOCInstructionUtils.reduce(matObj.getStreamable(), result,
				value -> ((MatrixBlock) value.getValue()).cmOperations(new CMOperator(finalCmOp)),
				(left, right) -> (CmCovObject) finalCmOp.fn.execute(left, right), ignored -> 256, getContext());
		else {
			MatrixObject weights = ec.getMatrixObject(input2.getName());
			weights.getDataCharacteristics().set(matObj.getNumRows(), matObj.getNumColumns(), matObj.getBlocksize(),
				weights.getNnz());
			OOCStream<CmCovObject> partials = createWritableStream(matObj);
			OOCInstructionUtils.equiJoinIndexed(matObj.getStreamable(), weights.getStreamable(), partials,
				(value, weight) -> ((MatrixBlock) value.getValue()).cmOperations(new CMOperator(finalCmOp),
					(MatrixBlock) weight.getValue()),
				ignored -> 256, getContext());
			OOCInstructionUtils.reduce(partials, result, value -> value,
				(left, right) -> (CmCovObject) finalCmOp.fn.execute(left, right), ignored -> 256, getContext());
		}

		result.start();
		try(OOCStream.QueueCallback<CmCovObject> callback = result.dequeueCB()) {
			if(callback == null)
				throw new DMLRuntimeException("Central moment cannot reduce an empty OOC stream");
			ec.setScalarOutput(outputName, new DoubleObject(callback.get().getRequiredResult(finalCmOp)));
		}
		try(OOCStream.QueueCallback<CmCovObject> callback = result.dequeueCB()) {
			if(callback != null)
				throw new DMLRuntimeException("Central moment produced multiple aggregate results");
		}
	}
}
