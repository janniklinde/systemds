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
import org.apache.sysds.lops.MapMultChain.ChainType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.data.SparseBlock;
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
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;
import org.apache.sysds.runtime.ooc.util.OOCDimensions;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class MapMMChainOOCInstruction extends ComputationOOCInstruction {
	private final ChainType _type;

	protected MapMMChainOOCInstruction(OOCType type, Operator op, CPOperand in1, CPOperand in2, CPOperand in3,
		CPOperand out, ChainType chainType, String opcode, String istr) {
		super(type, op, in1, in2, in3, out, opcode, istr);
		_type = chainType;
	}

	public static MapMMChainOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 4, 5);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);

		if (parts.length == 5) {
			CPOperand out = new CPOperand(parts[3]);
			ChainType type = ChainType.valueOf(parts[4]);
			return new MapMMChainOOCInstruction(OOCType.MAPMMCHAIN, null, in1, in2, null, out, type, opcode, str);
		}
		else { //parts.length==6
			CPOperand in3 = new CPOperand(parts[3]);
			CPOperand out = new CPOperand(parts[4]);
			ChainType type = ChainType.valueOf(parts[5]);
			return new MapMMChainOOCInstruction(OOCType.MAPMMCHAIN, null, in1, in2, in3, out, type, opcode, str);
		}
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject x = ec.getMatrixObject(input1);
		MatrixObject v = ec.getMatrixObject(input2);
		MatrixObject w = _type.isWeighted() ? ec.getMatrixObject(input3) : null;
		boolean hasV = !v.getDataCharacteristics().rowsKnown() || v.getNumRows() > 0;
		if(!hasV && _type != ChainType.XtXvy)
			throw new DMLRuntimeException("MMChain requires non-empty v for chain type " + _type);
		OOCDimensions.require(getOpcode(), x);
		if(x.getNumRows() <= 0 || x.getNumColumns() <= 0)
			throw new DMLRuntimeException("Planner-backed MMChain requires a non-empty matrix.");
		if(hasV)
			v.getDataCharacteristics().set(x.getNumColumns(), 1, x.getBlocksize(), v.getNnz());
		if(w != null)
			w.getDataCharacteristics().set(x.getNumRows(), 1, x.getBlocksize(), w.getNnz());
		processPlannerInstruction(ec, x, v, w, hasV);
	}

	private void processPlannerInstruction(ExecutionContext ec, MatrixObject x, MatrixObject v, MatrixObject w,
		boolean hasV) {
		int rowBlocks = Math.toIntExact(x.getDataCharacteristics().getNumRowBlocks());
		int colBlocks = Math.toIntExact(x.getDataCharacteristics().getNumColBlocks());
		int blocksize = x.getBlocksize();
		OOCStreamable<IndexedMatrixValue> sharedX = x.getStreamable();
		MaterializedStoreStreamable createdX = null;
		if(!sharedX.hasMaterializedStore()) {
			createdX = new MaterializedStoreStreamable(x.getStreamHandle(), x);
			sharedX = createdX;
		}
		BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator multiply = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate);

		OOCStream<IndexedMatrixValue> u;
		if(!hasV) {
			u = createWritableStream(w);
			RightScalarOperator negate = new RightScalarOperator(Multiply.getMultiplyFnObject(), -1);
			OOCInstructionUtils.equiMapBlock(w.getStreamable(), u,
				block -> block.scalarOperations(negate, new MatrixBlock()), getContext());
		}
		else {
			OOCStream<IndexedMatrixValue> xvPartials = createWritableStream(x);
			OOCInstructionUtils.indexedBroadcastMap(sharedX, v.getStreamable(), xvPartials,
				value -> value.getIndexes().getColumnIndex(), value -> 1,
				() -> new CountingLiveness(colBlocks, rowBlocks), (xValue, vValue) -> {
					MatrixBlock xBlock = (MatrixBlock) xValue.getValue();
					MatrixBlock vBlock = (MatrixBlock) vValue.getValue();
					MatrixBlock partial = xBlock.aggregateBinaryOperations(xBlock, vBlock, new MatrixBlock(), multiply);
					return new IndexedMatrixValue(xValue.getIndexes(), partial);
				}, getContext());

			OOCStream<IndexedMatrixValue> xv = createWritableStream(x.getNumRows(), 1, blocksize);
			OOCInstructionUtils.rowGroupedReduce(xvPartials, xv,
				(left, right) -> left.binaryOperations(plus, right, new MatrixBlock()), getContext());
			if(_type.isWeighted()) {
				u = createWritableStream(w);
				BinaryOperator weight = InstructionUtils
					.parseBinaryOperator(_type == ChainType.XtwXv ? Opcodes.MULT.toString() : Opcodes.MINUS.toString());
				OOCInstructionUtils.equiJoin(xv, w.getStreamable(), u,
					(left, right) -> left.binaryOperations(weight, right, new MatrixBlock()), getContext());
			}
			else
				u = xv;
		}

		OOCStream<IndexedMatrixValue> xtPartials = createWritableStream(x.getNumColumns(), x.getNumRows(), blocksize);
		OOCInstructionUtils.indexedBroadcastMap(sharedX, u, xtPartials, value -> value.getIndexes().getRowIndex(),
			value -> 1, () -> new CountingLiveness(rowBlocks, colBlocks), (xValue, uValue) -> {
				MatrixBlock partial = multTransposeVector((MatrixBlock) xValue.getValue(),
					(MatrixBlock) uValue.getValue());
				return new IndexedMatrixValue(
					new MatrixIndexes(xValue.getIndexes().getColumnIndex(), xValue.getIndexes().getRowIndex()),
					partial);
			}, getContext());

		ec.getDataCharacteristics(output.getName()).set(x.getNumColumns(), 1, blocksize, -1);
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		ec.getMatrixObject(output).setStreamHandle(out);
		OOCInstructionUtils.rowGroupedReduce(xtPartials, out,
			(left, right) -> left.binaryOperations(plus, right, new MatrixBlock()), getContext());
		if(createdX != null)
			createdX.scheduleMaterializedStoreDeletion();
	}

	private static MatrixBlock multTransposeVector(MatrixBlock x, MatrixBlock u) {
		int rows = x.getNumRows();
		int cols = x.getNumColumns();
		MatrixBlock out = new MatrixBlock(cols, 1, false);
		out.allocateDenseBlock();
		double[] outVals = out.getDenseBlockValues();

		if(x.isInSparseFormat()) {
			SparseBlock a = x.getSparseBlock();
			if(a != null) {
				if(u.isInSparseFormat()) {
					for(int i = 0; i < rows; i++) {
						if(a.isEmpty(i))
							continue;
						double uval = u.get(i, 0);
						if(uval == 0)
							continue;
						int apos = a.pos(i);
						int alen = a.size(i);
						int[] aix = a.indexes(i);
						double[] avals = a.values(i);
						for(int k = apos; k < apos + alen; k++)
							outVals[aix[k]] += uval * avals[k];
					}
				}
				else {
					double[] uvals = u.getDenseBlockValues();
					for(int i = 0; i < rows; i++) {
						if(a.isEmpty(i))
							continue;
						double uval = uvals[i];
						if(uval == 0)
							continue;
						int apos = a.pos(i);
						int alen = a.size(i);
						int[] aix = a.indexes(i);
						double[] avals = a.values(i);
						for(int k = apos; k < apos + alen; k++)
							outVals[aix[k]] += uval * avals[k];
					}
				}
			}
		}
		else {
			DenseBlock a = x.getDenseBlock();
			if(u.isInSparseFormat()) {
				for(int i = 0; i < rows; i++) {
					double uval = u.get(i, 0);
					if(uval == 0)
						continue;
					double[] avals = a.values(i);
					int apos = a.pos(i);
					for(int j = 0; j < cols; j++)
						outVals[j] += uval * avals[apos + j];
				}
			}
			else {
				double[] uvals = u.getDenseBlockValues();
				for(int i = 0; i < rows; i++) {
					double uval = uvals[i];
					if(uval == 0)
						continue;
					double[] avals = a.values(i);
					int apos = a.pos(i);
					for(int j = 0; j < cols; j++)
						outVals[j] += uval * avals[apos + j];
				}
			}
		}

		out.recomputeNonZeros();
		out.examSparsity();
		return out;
	}
}
