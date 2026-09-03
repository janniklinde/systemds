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

import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;

public final class SharedRowsOOCInstruction extends ComputationOOCInstruction {
	private static final int MAX_OPEN_ROWS = 16;

	private SharedRowsOOCInstruction(CPOperand input, CPOperand output, String opcode, String instruction) {
		super(OOCType.Tee, null, input, output, opcode, instruction);
	}

	public static SharedRowsOOCInstruction parseInstruction(String instruction) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(instruction);
		InstructionUtils.checkNumFields(parts, 2);
		return new SharedRowsOOCInstruction(new CPOperand(parts[1]), new CPOperand(parts[2]), parts[0], instruction);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject input = ec.getMatrixObject(input1);
		OOCStreamable<IndexedMatrixValue> source = input.getStreamable();
		MatrixObject result = ec.getMatrixObject(output);
		result.setStreamHandle(new SharedRowsStreamable(source, result, MAX_OPEN_ROWS, getContext()));
		result.setMetaData(input.getMetaData());
	}
}
