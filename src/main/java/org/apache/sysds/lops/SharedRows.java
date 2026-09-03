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

package org.apache.sysds.lops;

import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.instructions.InstructionUtils;

public final class SharedRows extends Lop {
	public static final String OPCODE = "sharedrows";

	public SharedRows(Lop input, DataType dataType, ValueType valueType) {
		super(Type.SharedRows, dataType, valueType);
		addInput(input);
		input.addOutput(this);
		lps.setProperties(inputs, Types.ExecType.OOC);
	}

	@Override
	public String toString() {
		return "Operation = SharedRows";
	}

	@Override
	public String getInstructions(String input, String output) {
		return InstructionUtils.concatOperands(getExecType().name(), OPCODE, getInputs().get(0).prepInputOperand(input),
			prepOutputOperand(output));
	}
}
