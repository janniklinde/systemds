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
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class MMultOOCInstruction extends ComputationOOCInstruction {


	protected MMultOOCInstruction(OOCType type, Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) {
		super(type, op, in1, in2, out, opcode, istr);
	}

	public static MMultOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 4);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]); // the larget matrix (streamed)
		CPOperand in2 = new CPOperand(parts[2]); // the small vector (in-memory)
		CPOperand out = new CPOperand(parts[3]);

		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator ba = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);

		return new MMultOOCInstruction(OOCType.MAPMM, ba, in1, in2, out, opcode, str);
	}

	@Override
	public void processInstruction( ExecutionContext ec ) {
		// 1. Identify the inputs
		MatrixObject min = ec.getMatrixObject(input1); // big matrix
		MatrixObject vin = ec.getMatrixObject(input2); // streamed vector

		if(OOC_NEW_SYSTEM && min.getDataCharacteristics().dimsKnown()
			&& vin.getDataCharacteristics().dimsKnown()
			&& min.getDataCharacteristics().getRows() == 1
			&& min.getDataCharacteristics().getCols() == vin.getDataCharacteristics().getRows()) {
			OOCStreamable<IndexedMatrixValue> qBroadcast = min.getStreamable();
			OOCStreamable<IndexedMatrixValue> qStreamed = vin.getStreamable();
			OOCStream<IndexedMatrixValue> qPartial = createWritableStream();
			OOCStream<IndexedMatrixValue> qOut = createWritableStream();
			qPartial.setData(vin);
			ec.getMatrixObject(output).setStreamHandle(qOut);
			qBroadcast.setDownstreamMessageRelay(qPartial::messageDownstream);
			qStreamed.setDownstreamMessageRelay(qPartial::messageDownstream);
			qPartial.setDownstreamMessageRelay(qOut::messageDownstream);
			qOut.setUpstreamMessageRelay(msg -> {
				qBroadcast.messageUpstream(msg.split());
				qStreamed.messageUpstream(msg.split());
			});

			OOCInstructionUtils.broadcast(qBroadcast, qStreamed, qPartial, (left, right) -> {
				MatrixBlock leftBlock = (MatrixBlock) left.getValue();
				MatrixBlock rightBlock = (MatrixBlock) right.getValue();
				return leftBlock.aggregateBinaryOperations(leftBlock, rightBlock,
					new MatrixBlock(), (AggregateBinaryOperator) _optr);
			}, imv -> Math.toIntExact(imv.getIndexes().getColumnIndex() - 1),
				imv -> Math.toIntExact(imv.getIndexes().getRowIndex() - 1),
				Math.toIntExact(min.getDataCharacteristics().getNumColBlocks()),
				Math.toIntExact(vin.getDataCharacteristics().getNumColBlocks()),
				getContext().addOutStream(qPartial));

			BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
			OOCInstructionUtils.colGroupedReduce(qPartial, qOut,
				Math.toIntExact(Math.max(1, Math.min(8L, vin.getDataCharacteristics().getNumRowBlocks()))),
				tmp -> tmp, (left, right) -> left.binaryOperationsInPlace(plus, right),
				java.util.function.Function.identity(), getContext().addOutStream(qOut));
			return;
		}

		if(OOC_NEW_SYSTEM && min.getDataCharacteristics().dimsKnown()
			&& vin.getDataCharacteristics().dimsKnown()
			&& vin.getDataCharacteristics().getNumColBlocks() == 1
			&& min.getDataCharacteristics().getCols() == vin.getDataCharacteristics().getRows()) {
			OOCStreamable<IndexedMatrixValue> qBroadcast = vin.getStreamable();
			OOCStreamable<IndexedMatrixValue> qStreamed = min.getStreamable();
			OOCStream<IndexedMatrixValue> qPartial = createWritableStream();
			OOCStream<IndexedMatrixValue> qOut = createWritableStream();
			qPartial.setData(min);
			ec.getMatrixObject(output).setStreamHandle(qOut);
			qBroadcast.setDownstreamMessageRelay(qPartial::messageDownstream);
			qStreamed.setDownstreamMessageRelay(qPartial::messageDownstream);
			qPartial.setDownstreamMessageRelay(qOut::messageDownstream);
			qOut.setUpstreamMessageRelay(msg -> {
				qBroadcast.messageUpstream(msg.split());
				qStreamed.messageUpstream(msg.split());
			});

			OOCInstructionUtils.broadcast(qBroadcast, qStreamed, qPartial, (left, right) -> {
					MatrixBlock vectorBlock = (MatrixBlock) left.getValue();
					MatrixBlock matrixBlock = (MatrixBlock) right.getValue();
					return matrixBlock.aggregateBinaryOperations(matrixBlock, vectorBlock,
						new MatrixBlock(), (AggregateBinaryOperator) _optr);
				}, imv -> Math.toIntExact(imv.getIndexes().getRowIndex() - 1),
				imv -> Math.toIntExact(imv.getIndexes().getColumnIndex() - 1),
				Math.toIntExact(vin.getDataCharacteristics().getNumRowBlocks()),
				Math.toIntExact(min.getDataCharacteristics().getNumRowBlocks()),
				getContext().addOutStream(qPartial));

			BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
			OOCInstructionUtils.rowGroupedReduce(qPartial, qOut,
				Math.toIntExact(Math.max(1, Math.min(8L, min.getDataCharacteristics().getNumColBlocks()))),
				tmp -> tmp, (left, right) -> left.binaryOperationsInPlace(plus, right),
				java.util.function.Function.identity(), getContext().addOutStream(qOut));
			return;
		}

		int emitLeftThreshold = (int)vin.getDataCharacteristics().getNumColBlocks();
		int emitRightThreshold = (int)min.getDataCharacteristics().getNumRowBlocks();

		OOCStream<IndexedMatrixValue> intermediateStream = createWritableStream();
		OOCStream<IndexedMatrixValue> outStream = createWritableStream();
		ec.getMatrixObject(output).setStreamHandle(outStream);

		joinManyOOC(min.getStreamHandle(), vin.getStreamHandle(), intermediateStream,
			(left, right) -> {
				MatrixBlock leftBlock = (MatrixBlock) left.getValue();
				MatrixBlock rightBlock = (MatrixBlock) right.getValue();
				MatrixBlock partialResult = leftBlock.aggregateBinaryOperations(leftBlock, rightBlock,
					new MatrixBlock(), (AggregateBinaryOperator) _optr);
				return new IndexedMatrixValue(new MatrixIndexes(left.getIndexes().getRowIndex(), right.getIndexes().getColumnIndex()), partialResult);
			},
			tmp -> tmp.getIndexes().getColumnIndex(),
			tmp -> tmp.getIndexes().getRowIndex(),
			emitLeftThreshold, emitRightThreshold);

		BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		int emitAggThreshold = (int)min.getDataCharacteristics().getNumColBlocks();

		groupedReduceOOC(intermediateStream, outStream, (left, right) -> {
			MatrixBlock mb = ((MatrixBlock)left.getValue()).binaryOperations(plus, right.getValue());
			left.setValue(mb);
			return left;
		}, emitAggThreshold);
	}
}
