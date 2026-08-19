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
import org.apache.sysds.runtime.functionobjects.Builtin;
import org.apache.sysds.runtime.functionobjects.ValueFunction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.SimpleOperator;
import org.apache.sysds.runtime.ooc.util.OOCDimensions;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class BuiltinNaryOOCInstruction extends ComputationOOCInstruction {
	private static final ScalarObject[] NO_SCALARS = new ScalarObject[0];

	private final CPOperand[] _inputs;
	private final boolean _cbind;
	private final boolean _bind;

	private BuiltinNaryOOCInstruction(Operator op, CPOperand[] inputs, CPOperand out, boolean bind, boolean cbind,
		String opcode, String istr) {
		super(OOCType.BuiltinNary, op, inputs[0], inputs.length > 1 ? inputs[1] : null, out, opcode, istr);
		_inputs = inputs;
		_cbind = cbind;
		_bind = bind;
	}

	public static BuiltinNaryOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		boolean cbind = Opcodes.CBIND.toString().equals(opcode);
		boolean bind = cbind || Opcodes.RBIND.toString().equals(opcode);
		boolean minmax = Opcodes.NMIN.toString().equals(opcode) || Opcodes.NMAX.toString().equals(opcode);
		if(!bind && !minmax)
			throw new DMLRuntimeException("Only n-ary cbind, rbind, nmin, and nmax are supported: " + opcode);
		if(parts.length <= 2)
			throw new DMLRuntimeException("N-ary builtin requires at least one input: " + str);

		CPOperand out = new CPOperand(parts[parts.length - 1]);
		CPOperand[] inputs = new CPOperand[parts.length - 2];
		for(int i = 1; i < parts.length - 1; i++) {
			inputs[i - 1] = new CPOperand(parts[i]);
			if(bind && inputs[i - 1].getDataType() != Types.DataType.MATRIX)
				throw new DMLRuntimeException("Only matrix inputs are supported for n-ary bind: " + str);
		}
		//nmin/nmax fold their scalar operands into an initial value, so only the value function is needed
		Operator op = bind ? null : new SimpleOperator(minmaxFunction(opcode));
		return new BuiltinNaryOOCInstruction(op, inputs, out, bind, cbind, opcode, str);
	}

	private static ValueFunction minmaxFunction(String opcode) {
		return Builtin.getBuiltinFnObject(opcode.substring(1));
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		if(_bind) {
			List<MatrixObject> inputs = new ArrayList<>(_inputs.length);
			for(CPOperand input : _inputs)
				inputs.add(ec.getMatrixObject(input));
			AppendOOCInstruction.bind(inputs, ec.getMatrixObject(output), _cbind, getContext());
			return;
		}
		processMinMax(ec);
	}

	private void processMinMax(ExecutionContext ec) {
		List<MatrixObject> matrices = new ArrayList<>();
		List<ScalarObject> scalars = new ArrayList<>();
		for(CPOperand input : _inputs) {
			if(input.getDataType() == Types.DataType.MATRIX)
				matrices.add(ec.getMatrixObject(input));
			else
				scalars.add(ec.getScalarInput(input));
		}
		if(matrices.isEmpty())
			throw new DMLRuntimeException("N-ary " + getOpcode() + " requires at least one matrix input");

		MatrixObject first = matrices.get(0);
		OOCDimensions.require(getOpcode(), first);
		for(MatrixObject matrix : matrices)
			if(matrix.getNumRows() != first.getNumRows() || matrix.getNumColumns() != first.getNumColumns() ||
				matrix.getBlocksize() != first.getBlocksize())
				throw new DMLRuntimeException("N-ary " + getOpcode() + " requires matching input dimensions");

		ScalarObject[] scalarOperands = scalars.toArray(ScalarObject[]::new);
		OOCInstructionUtils.propagateDims(ec, output, first.getNumRows(), first.getNumColumns(), first.getBlocksize(),
			-1);
		OOCStream<IndexedMatrixValue> qOut = createWritableStream(ec.getMatrixObject(output));
		ec.getMatrixObject(output).setStreamHandle(qOut);

		if(matrices.size() == 1) {
			OOCInstructionUtils.equiMapBlock(first.getStreamable(), qOut,
				block -> MatrixBlock.naryOperations(_optr, new MatrixBlock[] {block}, scalarOperands, new MatrixBlock()),
				getContext());
			return;
		}
		//min and max are associative and commutative, so tiles fold pairwise as soon as two of them share a block
		//index; that keeps one accumulator per index instead of the n-1 unmatched tiles an n-ary join would hold
		List<OOCStreamable<IndexedMatrixValue>> streams = new ArrayList<>(matrices.size());
		for(MatrixObject matrix : matrices)
			streams.add(matrix.getStreamable());
		OOCInstructionUtils.naryEquiReduce(streams, qOut,
			(left, right) -> MatrixBlock.naryOperations(_optr, new MatrixBlock[] {left, right}, NO_SCALARS,
				new MatrixBlock()),
			//the scalar operands fold in once at the end, which is equivalent under idempotent extrema
			block -> scalarOperands.length == 0 ? block :
				MatrixBlock.naryOperations(_optr, new MatrixBlock[] {block}, scalarOperands, new MatrixBlock()),
			getContext());
	}
}
