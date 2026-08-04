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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Opcodes;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.KahanPlus;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.ReduceAll;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.DoubleObject;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.AggregateTernaryOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.primitives.GroupedReduceOOCPrimitive;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class AggregateTernaryOOCInstruction extends ComputationOOCInstruction {
	private static final Log LOG = LogFactory.getLog(AggregateTernaryOOCInstruction.class.getName());

	private AggregateTernaryOOCInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand out,
		String opcode, String istr) {
		super(OOCInstruction.OOCType.AggregateTernary, op, in1, in2, in3, out, opcode, istr);
	}

	public static AggregateTernaryOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		if(opcode.equalsIgnoreCase(Opcodes.TAKPM.toString()) || opcode.equalsIgnoreCase(Opcodes.TACKPM.toString())) {
			InstructionUtils.checkNumFields(parts, 4, 5);
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand in3 = new CPOperand(parts[3]);
			CPOperand out = new CPOperand(parts[4]);
			AggregateTernaryOperator op = InstructionUtils.parseAggregateTernaryOperator(opcode, 1);
			return new AggregateTernaryOOCInstruction(op, in1, in2, in3, out, opcode, str);
		}
		throw new DMLRuntimeException("AggregateTernaryOOCInstruction.parseInstruction():: Unknown opcode " + opcode);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject first = ec.getMatrixObject(input1);
		MatrixObject second = ec.getMatrixObject(input2);
		MatrixObject third = input3.isLiteral() ? null : ec.getMatrixObject(input3);
		AggregateTernaryOperator operator = (AggregateTernaryOperator) _optr;
		validateInput(first, second, third, operator, input1.getName(), input2.getName(), input3.getName());
		second.getDataCharacteristics().set(first.getNumRows(), first.getNumColumns(), first.getBlocksize(),
			second.getNnz());
		if(third != null)
			third.getDataCharacteristics().set(first.getNumRows(), first.getNumColumns(), first.getBlocksize(),
				third.getNnz());

		OOCStream<IndexedMatrixValue> partials = createWritableStream(first);
		if(third == null)
			OOCInstructionUtils
				.equiJoin(
					first.getStreamable(), second.getStreamable(), partials, (left, right) -> MatrixBlock
						.aggregateTernaryOperations(left, right, null, new MatrixBlock(), operator, false),
					getContext());
		else
			OOCInstructionUtils.naryEquiJoin(
				List.of(first.getStreamable(), second.getStreamable(), third.getStreamable()), partials,
				values -> new IndexedMatrixValue(values.get(0).getIndexes(),
					MatrixBlock.aggregateTernaryOperations((MatrixBlock) values.get(0).getValue(),
						(MatrixBlock) values.get(1).getValue(), (MatrixBlock) values.get(2).getValue(),
						new MatrixBlock(), operator, false)),
				getContext());

		if(operator.indexFn instanceof ReduceAll)
			processReduceAll(ec, operator, partials);
		else
			processReduceRows(ec, operator, first, partials);
	}

	private void processReduceAll(ExecutionContext ec, AggregateTernaryOperator operator,
		OOCStream<IndexedMatrixValue> partials) {
		OOCStream<MatrixBlock> result = createWritableStream(4, 4, 4);
		OOCInstructionUtils.reduce(partials, result, value -> new MatrixBlock((MatrixBlock) value.getValue()),
			(left, right) -> merge(left, right, operator), MatrixBlock::getExactSerializedSize, getContext());
		result.start();
		try(OOCStream.QueueCallback<MatrixBlock> callback = result.dequeueCB()) {
			if(callback == null)
				throw new DMLRuntimeException("Aggregate ternary cannot reduce an empty OOC stream");
			MatrixBlock aggregate = callback.get();
			aggregate.dropLastRowsOrColumns(operator.aggOp.correction);
			ec.setScalarOutput(output.getName(), new DoubleObject(aggregate.get(0, 0)));
		}
		try(OOCStream.QueueCallback<MatrixBlock> callback = result.dequeueCB()) {
			if(callback != null)
				throw new DMLRuntimeException("Aggregate ternary produced multiple scalar results");
		}
	}

	private void processReduceRows(ExecutionContext ec, AggregateTernaryOperator operator, MatrixObject input,
		OOCStream<IndexedMatrixValue> partials) {
		MatrixObject target = ec.getMatrixObject(output);
		target.getDataCharacteristics().set(1, input.getNumColumns(), input.getBlocksize(), target.getNnz());
		OOCStream<IndexedMatrixValue> result = createWritableStream(target);
		target.setStreamHandle(result);
		OOCInstructionUtils.groupedReduceIndexed(partials, result, GroupedReduceOOCPrimitive.Grouping.COL_BLOCKS,
			value -> new MatrixBlock((MatrixBlock) value.getValue()), (left, right) -> merge(left, right, operator),
			block -> {
				block.dropLastRowsOrColumns(operator.aggOp.correction);
				return block;
			}, getContext());
	}

	private static MatrixBlock merge(MatrixBlock left, MatrixBlock right, AggregateTernaryOperator operator) {
		MatrixBlock result = new MatrixBlock(left);
		OperationsOnMatrixValues.incrementalAggregation(result, null, right, operator.aggOp, true);
		return result;
	}

	private static void validateInput(MatrixObject first, MatrixObject second, MatrixObject third,
		AggregateTernaryOperator operator, String firstName, String secondName, String thirdName) {
		DataCharacteristics firstDc = first.getDataCharacteristics();
		DataCharacteristics secondDc = second.getDataCharacteristics();
		DataCharacteristics thirdDc = third == null ? null : third.getDataCharacteristics();
		if(!firstDc.dimsKnown() || firstDc.getBlocksize() <= 0)
			throw new DMLRuntimeException("Unknown dimensions for first aggregate ternary input.");
		boolean invalidSecond = secondDc.dimsKnown() &&
			(firstDc.getRows() != secondDc.getRows() || firstDc.getCols() != secondDc.getCols());
		boolean invalidThird = thirdDc != null && thirdDc.dimsKnown() &&
			(firstDc.getRows() != thirdDc.getRows() || firstDc.getCols() != thirdDc.getCols());
		if(invalidSecond || invalidThird) {
			if(LOG.isTraceEnabled()) {
				LOG.trace("matBlock1:" + firstName + " (" + firstDc.getRows() + "x" + firstDc.getCols() + ")");
				LOG.trace("matBlock2:" + secondName + " (" + secondDc.getRows() + "x" + secondDc.getCols() + ")");
				if(thirdDc != null)
					LOG.trace("matBlock3:" + thirdName + " (" + thirdDc.getRows() + "x" + thirdDc.getCols() + ")");
			}
			throw new DMLRuntimeException("Invalid dimensions for aggregate ternary inputs.");
		}
		if(!(operator.aggOp.increOp.fn instanceof KahanPlus && operator.binaryFn instanceof Multiply))
			throw new DMLRuntimeException("Unsupported operator for aggregate ternary operations.");
	}
}
