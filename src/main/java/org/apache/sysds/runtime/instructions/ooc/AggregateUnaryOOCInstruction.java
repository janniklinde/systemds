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

import org.apache.sysds.common.Types.CorrectionLocationType;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.LocalTaskQueue;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.DoubleObject;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateUnaryOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.primitives.GroupedReduceOOCPrimitive;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.util.IndexRange;

public class AggregateUnaryOOCInstruction extends ComputationOOCInstruction {
	private AggregateOperator _aop = null;

	protected AggregateUnaryOOCInstruction(OOCType type, AggregateUnaryOperator auop, AggregateOperator aop, 
			CPOperand in, CPOperand out, String opcode, String istr) {
		super(type, auop, in, out, opcode, istr);
		_aop = aop;
	}

	protected AggregateUnaryOOCInstruction(OOCType type, Operator op, CPOperand in1, CPOperand in2, CPOperand in3,
		CPOperand out, String opcode, String istr) {
		super(type, op, in1, in2, in3, out, opcode, istr);
		_aop = null;
	}

	public static AggregateUnaryOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 2);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		
		String aopcode = InstructionUtils.deriveAggregateOperatorOpcode(opcode);
		CorrectionLocationType corrLoc = InstructionUtils.deriveAggregateOperatorCorrectionLocation(opcode);
		AggregateUnaryOperator aggun = InstructionUtils.parseBasicAggregateUnaryOperator(opcode);
		AggregateOperator aop = InstructionUtils.parseAggregateOperator(aopcode, corrLoc.toString());
		return new AggregateUnaryOOCInstruction(
			OOCType.AggregateUnary, aggun, aop, in1, out, opcode, str);
	}
	
	@Override
	public void processInstruction( ExecutionContext ec ) {
		//TODO support all types of aggregations, currently only full aggregation, row aggregation and column aggregation
		
		//setup operators and input queue
		AggregateUnaryOperator aggun = (AggregateUnaryOperator) getOperator(); 
		MatrixObject min = ec.getMatrixObject(input1);
		int blen = ConfigurationManager.getBlocksize();

		if (aggun.isRowAggregate() || aggun.isColAggregate()) {
			DataCharacteristics chars = ec.getDataCharacteristics(input1.getName());
			// number of blocks to process per aggregation idx (row or column dim)
			long emitThreshold = aggun.isRowAggregate()? chars.getNumColBlocks() : chars.getNumRowBlocks();
			OOCStream<IndexedMatrixValue> qOut = createWritableStream();
			ec.getMatrixObject(output).setStreamHandle(qOut);

			OOCStreamable<IndexedMatrixValue> qIn = min.getStreamable();
			qIn.setDownstreamMessageRelay(qOut::messageDownstream);
			qOut.setUpstreamMessageRelay(qIn::messageUpstream);
			qOut.setIXTransform((downstream, range) -> {
				if (downstream) {
					if (aggun.isRowAggregate())
						return new IndexRange(range.rowStart, range.rowEnd, 1, 1);
					else
						return new IndexRange(1, 1, range.colStart, range.colEnd);
				}
				if (aggun.isRowAggregate())
					return new IndexRange(range.rowStart, range.rowEnd, 1, min.getNumColumns() - 1);
				else
					return new IndexRange(1, min.getNumRows() - 1, range.colStart, range.colEnd);
			});

			GroupedReduceOOCPrimitive.Grouping grouping = aggun.isRowAggregate() ?
				GroupedReduceOOCPrimitive.Grouping.ROW_BLOCKS :
				GroupedReduceOOCPrimitive.Grouping.COL_BLOCKS;
			int accumulatorsPerGroup = Math.toIntExact(Math.max(1, Math.min(8L, emitThreshold)));
			OOCInstructionUtils.groupedReduceIndexed(qIn, qOut, grouping, accumulatorsPerGroup,
				tmp -> (MatrixBlock) tmp.getValue()
					.aggregateUnaryOperations(aggun, new MatrixBlock(), blen, tmp.getIndexes()),
				this::mergeAggregate, this::finalizeAggregate, getContext().addOutStream(qOut));
		}
		// full aggregation
		else {
			OOCStream<IndexedMatrixValue> qIn = min.getStreamHandle();
			OOCStream<MatrixBlock> qLocal = createWritableStream();

			mapOOC(qIn, qLocal, tmp -> (MatrixBlock) tmp.getValue()
				.aggregateUnaryOperations(aggun, new MatrixBlock(), blen, tmp.getIndexes()));
			qIn.start();

			MatrixBlock ltmp;
			int extra = _aop.correction.getNumRemovedRowsColumns();
			MatrixBlock ret = new MatrixBlock(1, 1 + extra, _aop.initialValue);
			MatrixBlock corr = new MatrixBlock(1,1+extra,false);
			while((ltmp = qLocal.dequeue()) != LocalTaskQueue.NO_MORE_TASKS) {
				OperationsOnMatrixValues.incrementalAggregation(
					ret, _aop.existsCorrection() ? corr : null, ltmp, _aop, true);
			}

			//create scalar output
			ec.setScalarOutput(output.getName(), new DoubleObject(ret.get(0, 0)));
		}
	}

	private MatrixBlock mergeAggregate(MatrixBlock left, MatrixBlock right) {
		OperationsOnMatrixValues.incrementalAggregation(left, null, right, _aop, true);
		return left;
	}

	private MatrixBlock finalizeAggregate(MatrixBlock block) {
		block.dropLastRowsOrColumns(_aop.correction);
		return block;
	}
}
