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

import java.util.ArrayList;
import java.util.List;

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;

public class BuiltinNaryOOCInstruction extends ComputationOOCInstruction {
	private final CPOperand[] _inputs;
	private final boolean _cbind;

	private BuiltinNaryOOCInstruction(CPOperand[] inputs, CPOperand out, boolean cbind, String opcode, String istr) {
		super(OOCType.BuiltinNary, null, inputs[0], inputs.length > 1 ? inputs[1] : null, out, opcode, istr);
		_inputs = inputs;
		_cbind = cbind;
	}

	public static BuiltinNaryOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		boolean cbind = Opcodes.CBIND.toString().equals(opcode);
		if(!cbind && !Opcodes.RBIND.toString().equals(opcode))
			throw new DMLRuntimeException("Only n-ary cbind and rbind are supported: " + opcode);
		if(parts.length <= 2)
			throw new DMLRuntimeException("N-ary bind requires at least one input: " + str);

		CPOperand out = new CPOperand(parts[parts.length - 1]);
		CPOperand[] inputs = new CPOperand[parts.length - 2];
		for(int i = 1; i < parts.length - 1; i++) {
			inputs[i - 1] = new CPOperand(parts[i]);
			if(inputs[i - 1].getDataType() != Types.DataType.MATRIX)
				throw new DMLRuntimeException("Only matrix inputs are supported for n-ary bind: " + str);
		}
		return new BuiltinNaryOOCInstruction(inputs, out, cbind, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		List<MatrixObject> inputs = new ArrayList<>(_inputs.length);
		for(CPOperand input : _inputs)
			inputs.add(ec.getMatrixObject(input));
		AppendOOCInstruction.bind(inputs, ec.getMatrixObject(output), _cbind, getContext());
	}
}
