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
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.List;

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
		OOCStreamable<IndexedMatrixValue> sIn1 = m1.getStreamable();
		OOCStreamable<IndexedMatrixValue> sIn2 = m2.getStreamable();

		OOCStream<IndexedMatrixValue> qOut = new SubscribableTaskQueue<>();
		ec.getMatrixObject(output).setStreamHandle(qOut);
		sIn1.setDownstreamMessageRelay(qOut::messageDownstream);
		sIn2.setDownstreamMessageRelay(qOut::messageDownstream);
		qOut.setUpstreamMessageRelay(msg -> {
			sIn1.messageUpstream(msg.split());
			sIn2.messageUpstream(msg.split());
		});

		final boolean known1 = (m1.getNumRows() >= 0 && m1.getNumColumns() >= 0);
		final boolean known2 = (m2.getNumRows() >= 0 && m2.getNumColumns() >= 0);

		// If dimensions are unknown, we cannot safely detect broadcasting.
		// Fall back to strict key-based join and let downstream operators validate as needed.
		if(!known1 || !known2) {
			if(LOG.isWarnEnabled()) {
				LOG.warn("Falling back to key-wise OOC binary join for opcode '" + getOpcode()
					+ "' due to unknown matrix dimensions: " + input1.getName() + "=" + m1.getNumRows() + "x"
					+ m1.getNumColumns() + ", " + input2.getName() + "=" + m2.getNumRows() + "x"
					+ m2.getNumColumns());
			}
			OOCStream<IndexedMatrixValue> qIn1 = m1.getStreamHandle();
			OOCStream<IndexedMatrixValue> qIn2 = m2.getStreamHandle();
			joinOOC(qIn1, qIn2, qOut, (tmp1, tmp2) -> {
				IndexedMatrixValue tmpOut = new IndexedMatrixValue();
				tmpOut.set(tmp1.getIndexes(),
					tmp1.getValue().binaryOperations((BinaryOperator)_optr, tmp2.getValue(), tmpOut.getValue()));
				return tmpOut;
			}, IndexedMatrixValue::getIndexes);
			return;
		}

		boolean isColBroadcast = m1.getNumColumns() > 1 && m2.getNumColumns() == 1;
		boolean isRowBroadcast = m1.getNumRows() > 1 && m2.getNumRows() == 1;

		if(!isColBroadcast && !isRowBroadcast) {
			if (m1.getNumColumns() != m2.getNumColumns() || m1.getNumRows() != m2.getNumRows())
				throw new NotImplementedException("Invalid dimensions for matrix-matrix binary op: "
					+ m1.getNumRows() + "x" + m1.getNumColumns() + " <=> "
					+ m2.getNumRows() + "x" + m2.getNumColumns());

			if(OOC_NEW_SYSTEM) {
				OOCInstructionUtils.equiJoin(List.of(sIn1, sIn2), qOut, pair -> {
					return pair.get(0).binaryOperations((BinaryOperator)_optr, pair.get(1));
				}, getContext().addOutStream(qOut));
				return;
			}
		}

		if (isColBroadcast && !isRowBroadcast) {
			if(OOC_NEW_SYSTEM) {
				OOCInstructionUtils.broadcast(sIn2, sIn1, qOut, (broadcast, streamed) ->
						(MatrixBlock) streamed.getValue()
							.binaryOperations((BinaryOperator)_optr, broadcast.getValue(), new MatrixBlock()),
					imv -> Math.toIntExact(imv.getIndexes().getRowIndex() - 1),
					imv -> Math.toIntExact(imv.getIndexes().getRowIndex() - 1),
					Math.toIntExact(m2.getDataCharacteristics().getNumRowBlocks()),
					Math.toIntExact(m1.getDataCharacteristics().getNumColBlocks()),
					getContext().addOutStream(qOut));
				return;
			}

			OOCStream<IndexedMatrixValue> qIn1 = m1.getStreamHandle();
			OOCStream<IndexedMatrixValue> qIn2 = m2.getStreamHandle();
			final long maxProcessesPerBroadcast = (m1.getNumColumns() + m1.getBlocksize() - 1) / m1.getBlocksize();
			broadcastJoinOOC(qIn1, qIn2, qOut, (tmp1, b) -> {
				IndexedMatrixValue tmpOut = new IndexedMatrixValue();
				tmpOut.set(tmp1.getIndexes(),
					tmp1.getValue().binaryOperations((BinaryOperator)_optr, b.getValue().getValue(), tmpOut.getValue()));

				if (b.incrProcessCtrAndGet() >= maxProcessesPerBroadcast)
					b.release();

				return tmpOut;
			}, tmp -> tmp.getIndexes().getRowIndex());
		}
		else if (isRowBroadcast && !isColBroadcast) {
			if(OOC_NEW_SYSTEM) {
				OOCInstructionUtils.broadcast(sIn2, sIn1, qOut, (broadcast, streamed) ->
						(MatrixBlock) streamed.getValue()
							.binaryOperations((BinaryOperator)_optr, broadcast.getValue(), new MatrixBlock()),
					imv -> Math.toIntExact(imv.getIndexes().getColumnIndex() - 1),
					imv -> Math.toIntExact(imv.getIndexes().getColumnIndex() - 1),
					Math.toIntExact(m2.getDataCharacteristics().getNumColBlocks()),
					Math.toIntExact(m1.getDataCharacteristics().getNumRowBlocks()),
					getContext().addOutStream(qOut));
				return;
			}

			OOCStream<IndexedMatrixValue> qIn1 = m1.getStreamHandle();
			OOCStream<IndexedMatrixValue> qIn2 = m2.getStreamHandle();
			final long maxProcessesPerBroadcast = (m1.getNumRows() + m1.getBlocksize() - 1) / m1.getBlocksize();
			broadcastJoinOOC(qIn1, qIn2, qOut, (tmp1, b) -> {
				IndexedMatrixValue tmpOut = new IndexedMatrixValue();
				tmpOut.set(tmp1.getIndexes(),
					tmp1.getValue().binaryOperations((BinaryOperator)_optr, b.getValue().getValue(), tmpOut.getValue()));

				if (b.incrProcessCtrAndGet() >= maxProcessesPerBroadcast)
					b.release();

				return tmpOut;
			}, tmp -> tmp.getIndexes().getColumnIndex());
		}
		else {
			OOCStream<IndexedMatrixValue> qIn1 = m1.getStreamHandle();
			OOCStream<IndexedMatrixValue> qIn2 = m2.getStreamHandle();
			joinOOC(qIn1, qIn2, qOut, (tmp1, tmp2) -> {
				IndexedMatrixValue tmpOut = new IndexedMatrixValue();
				tmpOut.set(tmp1.getIndexes(),
					tmp1.getValue().binaryOperations((BinaryOperator)_optr, tmp2.getValue(), tmpOut.getValue()));
				return tmpOut;
			}, IndexedMatrixValue::getIndexes);
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
		ec.getMatrixObject(output).setStreamHandle(qOut);

		if(OOC_NEW_SYSTEM) {
			OOCStreamable<IndexedMatrixValue> sIn = min.getStreamable();
			sIn.setDownstreamMessageRelay(qOut::messageDownstream);
			qOut.setUpstreamMessageRelay(sIn::messageUpstream);
			OOCInstructionUtils.equiMap(sIn, qOut, mb -> mb.scalarOperations(sc_op, new MatrixBlock()),
				getContext().addOutStream(qOut));
			return;
		}

		OOCStream<IndexedMatrixValue> qIn = min.getStreamHandle();
		qIn.setDownstreamMessageRelay(qOut::messageDownstream);
		qOut.setUpstreamMessageRelay(qIn::messageUpstream);

		mapOOC(qIn, qOut, tmp -> {
			IndexedMatrixValue tmpOut = new IndexedMatrixValue();
			tmpOut.set(tmp.getIndexes(),
				tmp.getValue().scalarOperations(sc_op, new MatrixBlock()));
			return tmpOut;
		});
	}
}
