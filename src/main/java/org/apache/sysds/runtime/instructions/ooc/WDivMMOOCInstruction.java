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
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.lops.WeightedDivMM.WDivMMType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.matrix.operators.QuaternaryOperator;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;
import org.apache.sysds.runtime.ooc.util.OOCDimensions;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public class WDivMMOOCInstruction extends QuaternaryOOCInstruction {
	private static final int MIN_CONCURRENT_TASKS = 22;


	protected WDivMMOOCInstruction(QuaternaryOperator op, CPOperand in1, CPOperand in2, CPOperand in3, CPOperand in4,
		CPOperand out, String opcode, String istr) {
		super(op, in1, in2, in3, in4, out, opcode, istr);
	}

	public static WDivMMOOCInstruction parseInstruction(QuaternaryOOCInstruction instr) {
		String instrStr = instr.getInstructionString();
		String opcode = InstructionUtils.getInstructionPartsWithValueType(instr.getInstructionString())[0];
		return new WDivMMOOCInstruction((QuaternaryOperator) instr.getOperator(), instr.input1, instr.input2,
			instr.input3, instr.input4, instr.output, opcode, instrStr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		QuaternaryOperator operator = (QuaternaryOperator) _optr;
		WDivMMType type = operator.wtype3;
		MatrixObject x = ec.getMatrixObject(input1);
		MatrixObject u = ec.getMatrixObject(input2);
		MatrixObject v = ec.getMatrixObject(input3);
		MatrixObject w = type.hasFourInputs() && !type.hasScalar() ? ec.getMatrixObject(input4) : null;
		long rank = u.getDataCharacteristics().colsKnown() ? u.getNumColumns() : v.getNumColumns();
		OOCDimensions.require(getOpcode(), x);
		if(x.getNumRows() <= 0 || x.getNumColumns() <= 0 || rank <= 0)
			throw new DMLRuntimeException("Planner-backed WDivMM requires a non-empty matrix and a positive rank.");
		u.getDataCharacteristics().set(x.getNumRows(), rank, x.getBlocksize(), u.getNnz());
		v.getDataCharacteristics().set(x.getNumColumns(), rank, x.getBlocksize(), v.getNnz());
		if(w != null)
			w.getDataCharacteristics().set(x.getNumRows(), x.getNumColumns(), x.getBlocksize(), w.getNnz());
		if(isFusable(type) && bandsFitBudget(x.getBlocksize(), rank))
			processFusedInstruction(ec, type, x, u, v);
		else
			processPlannerInstruction(ec, type, x, u, v, w);
	}

	private static boolean isFusable(WDivMMType type) {
		return !type.hasFourInputs() || type.hasScalar();
	}

	private static boolean bandsFitBudget(int blocksize, long rank) {
		long bandBytes = MatrixBlock.estimateSizeDenseInMemory(blocksize, rank);
		long budget = GlobalMemoryBroker.get().getAllowedMemory();
		return budget <= 0 || 2 * bandBytes <= budget / MIN_CONCURRENT_TASKS;
	}

	private void processFusedInstruction(ExecutionContext ec, WDivMMType type, MatrixObject x, MatrixObject u,
		MatrixObject v) {
		int blocksize = x.getBlocksize();
		DataCharacteristics dc = x.getDataCharacteristics();
		int rowBlocks = Math.toIntExact(dc.getNumRowBlocks());
		int colBlocks = Math.toIntExact(dc.getNumColBlocks());
		long rank = u.getNumColumns();
		int factorColBlocks = (int) Math.ceil((double) rank / blocksize);

		MatrixBlock scalar = type.hasScalar() ? new MatrixBlock(ec.getScalarInput(input4).getDoubleValue()) : null;
		QuaternaryOperator qop = new QuaternaryOperator(type);
		boolean basic = type.isBasic();
		boolean left = type.isLeft();

		List<OOCStreamable<IndexedMatrixValue>> factors = List.of(u.getStreamable(), v.getStreamable());
		List<ToLongFunction<IndexedMatrixValue>> lookupRows = List.of(
			value -> value.getIndexes().getRowIndex(), value -> value.getIndexes().getColumnIndex());
		List<ToLongFunction<IndexedMatrixValue>> lookupCols = List.of(value -> 1, value -> 1);
		List<Integer> bandWidths = List.of(factorColBlocks, factorColBlocks);
		List<Supplier<MaterializedStore.Liveness>> liveness = List.of(
			() -> new CountingLiveness(rowBlocks * factorColBlocks, colBlocks),
			() -> new CountingLiveness(colBlocks * factorColBlocks, rowBlocks));

		long outputRows = basic ? x.getNumRows() : (left ? x.getNumColumns() : x.getNumRows());
		long outputCols = basic ? x.getNumColumns() : rank;
		ec.getDataCharacteristics(output.getName()).set(outputRows, outputCols, blocksize, -1);

		if(basic) {
			OOCStream<IndexedMatrixValue> out = createWritableStream();
			ec.getMatrixObject(output).setStreamHandle(out);
			OOCInstructionUtils.multiIndexedBroadcastMap(x.getStreamable(), factors, out, lookupRows, lookupCols,
				bandWidths, liveness, blockOperation(qop, scalar, basic, left), getContext());
			return;
		}

		OOCStream<IndexedMatrixValue> partials = createWritableStream(left ? x.getNumColumns() : x.getNumRows(),
			left ? x.getNumRows() : x.getNumColumns(), blocksize);
		OOCInstructionUtils.multiIndexedBroadcastMap(x.getStreamable(), factors, partials, lookupRows, lookupCols,
			bandWidths, liveness, blockOperation(qop, scalar, basic, left), getContext());

		BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		ec.getMatrixObject(output).setStreamHandle(out);
		if(factorColBlocks == 1) {
			OOCInstructionUtils.rowGroupedReduce(partials, out,
				(accumulator, partial) -> accumulator.binaryOperations(plus, partial, new MatrixBlock()), getContext());
			return;
		}

		OOCStream<IndexedMatrixValue> bands = createWritableStream(outputRows, 1, blocksize);
		OOCInstructionUtils.rowGroupedReduce(partials, bands,
			(accumulator, partial) -> accumulator.binaryOperations(plus, partial, new MatrixBlock()), getContext());
		long taskBytes = 3 * MatrixBlock.estimateSizeDenseInMemory(blocksize, outputCols);
		OOCInstructionUtils.flatMap(bands, out, value -> splitBand(value, blocksize, outputCols),
			OOCUtils::memoryCharge, taskBytes, getContext());
	}

	private static Collection<IndexedMatrixValue> splitBand(IndexedMatrixValue value, int blocksize, long cols) {
		MatrixBlock band = (MatrixBlock) value.getValue();
		int tiles = (int) Math.ceil((double) cols / blocksize);
		List<IndexedMatrixValue> split = new ArrayList<>(tiles);
		long row = value.getIndexes().getRowIndex();
		for(int tile = 0; tile < tiles; tile++) {
			int from = tile * blocksize;
			int to = (int) Math.min((long) (tile + 1) * blocksize, cols) - 1;
			MatrixBlock cut = band.slice(0, band.getNumRows() - 1, from, to, new MatrixBlock());
			split.add(new IndexedMatrixValue(new MatrixIndexes(row, tile + 1L), cut));
		}
		return split;
	}

	private static BiFunction<IndexedMatrixValue, IndexedMatrixValue[][], IndexedMatrixValue>
		blockOperation(QuaternaryOperator qop, MatrixBlock scalar, boolean basic, boolean left) {
		return (xValue, factors) -> {
			MatrixBlock xBlock = (MatrixBlock) xValue.getValue();
			MatrixBlock uBlock = band(factors[0]);
			MatrixBlock vBlock = band(factors[1]);
			MatrixBlock result = xBlock.quaternaryOperations(qop, uBlock, vBlock, scalar, new MatrixBlock());
			MatrixIndexes indexes = xValue.getIndexes();
			MatrixIndexes outputIndexes = basic ? indexes : left ?
				new MatrixIndexes(indexes.getColumnIndex(), indexes.getRowIndex()) :
				new MatrixIndexes(indexes.getRowIndex(), indexes.getColumnIndex());
			return new IndexedMatrixValue(outputIndexes, result);
		};
	}

	private static MatrixBlock band(IndexedMatrixValue[] tiles) {
		if(tiles.length == 1)
			return (MatrixBlock) tiles[0].getValue();
		MatrixBlock[] rest = new MatrixBlock[tiles.length - 1];
		for(int i = 1; i < tiles.length; i++)
			rest[i - 1] = (MatrixBlock) tiles[i].getValue();
		return ((MatrixBlock) tiles[0].getValue()).append(rest, new MatrixBlock(), true);
	}

	private void processPlannerInstruction(ExecutionContext ec, WDivMMType type, MatrixObject x, MatrixObject u,
		MatrixObject v, MatrixObject w) {
		int blocksize = x.getBlocksize();
		AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator multiply = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate);
		BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());
		BinaryOperator minus = InstructionUtils.parseBinaryOperator(Opcodes.MINUS.toString());
		BinaryOperator times = InstructionUtils.parseBinaryOperator(Opcodes.MULT.toString());
		BinaryOperator divide = InstructionUtils.parseBinaryOperator(Opcodes.DIV.toString());
		OOCStreamable<IndexedMatrixValue> sharedX = x.getStreamable();
		OOCStreamable<IndexedMatrixValue> sharedU = u.getStreamable();
		OOCStreamable<IndexedMatrixValue> sharedV = v.getStreamable();
		MaterializedStoreStreamable createdX = null;
		MaterializedStoreStreamable createdV = null;
		if(type.isMinus() && !type.hasFourInputs() && !sharedX.hasMaterializedStore()) {
			createdX = new MaterializedStoreStreamable(x.getStreamHandle(), x);
			sharedX = createdX;
		}
		if(type.isRight() && !sharedV.hasMaterializedStore()) {
			createdV = new MaterializedStoreStreamable(v.getStreamHandle(), v, OOCStoreLayout.COL_MAJOR);
			sharedV = createdV;
		}

		OOCStream<IndexedMatrixValue> vt = createWritableStream(v.getNumColumns(), v.getNumRows(), blocksize);
		OOCInstructionUtils.transpose(sharedV, vt, getContext());
		OOCStream<IndexedMatrixValue> product = createWritableStream(x);
		OOCInstructionUtils.matrixMultiply(sharedU, vt, product, multiply, plus, getContext());

		if(type.isBasic()) {
			ec.getDataCharacteristics(output.getName()).set(x.getNumRows(), x.getNumColumns(), blocksize, -1);
			OOCStream<IndexedMatrixValue> out = plannerElement(sharedX, product, x, times);
			ec.getMatrixObject(output).setStreamHandle(out);
			if(createdX != null)
				createdX.scheduleMaterializedStoreDeletion();
			if(createdV != null)
				createdV.scheduleMaterializedStoreDeletion();
			return;
		}

		OOCStream<IndexedMatrixValue> intermediate;
		if(type.hasFourInputs()) {
			if(type.hasScalar()) {
				double epsilon = ec.getScalarInput(input4).getDoubleValue();
				RightScalarOperator add = new RightScalarOperator(Plus.getPlusFnObject(), epsilon);
				OOCStream<IndexedMatrixValue> adjusted = createWritableStream(x);
				OOCInstructionUtils.equiMapBlock(product, adjusted,
					block -> block.scalarOperations(add, new MatrixBlock()), getContext());
				intermediate = plannerElement(sharedX, adjusted, x, divide);
			}
			else {
				OOCStream<IndexedMatrixValue> difference = plannerElement(product, w.getStreamable(), x, minus);
				intermediate = plannerElement(sharedX, difference, x, times);
			}
		}
		else if(type.isMinus()) {
			OOCStream<IndexedMatrixValue> difference = plannerElement(product, sharedX, x, minus);
			OOCStream<IndexedMatrixValue> masked = createWritableStream(x);
			OOCInstructionUtils.equiJoin(sharedX, difference, masked, (mask, block) -> {
				MatrixBlock result = new MatrixBlock(block);
				return mask(mask, result);
			}, getContext());
			intermediate = masked;
		}
		else
			intermediate = plannerElement(sharedX, product, x, type.isMult() ? times : divide);

		long outputRows = type.isLeft() ? x.getNumColumns() : x.getNumRows();
		long outputCols = u.getNumColumns();
		ec.getDataCharacteristics(output.getName()).set(outputRows, outputCols, blocksize, -1);
		OOCStream<IndexedMatrixValue> out = createWritableStream();
		ec.getMatrixObject(output).setStreamHandle(out);
		if(type.isLeft()) {
			OOCStream<IndexedMatrixValue> transposed = createWritableStream(x.getNumColumns(), x.getNumRows(),
				blocksize);
			OOCInstructionUtils.transpose(intermediate, transposed, getContext());
			OOCInstructionUtils.matrixMultiply(transposed, sharedU, out, multiply, plus, getContext());
		}
		else
			OOCInstructionUtils.matrixMultiply(intermediate, sharedV, out, multiply, plus, getContext());
		if(createdX != null)
			createdX.scheduleMaterializedStoreDeletion();
		if(createdV != null)
			createdV.scheduleMaterializedStoreDeletion();
	}

	private OOCStream<IndexedMatrixValue> plannerElement(OOCStreamable<IndexedMatrixValue> left,
		OOCStreamable<IndexedMatrixValue> right, MatrixObject metadata, BinaryOperator operator) {
		OOCStream<IndexedMatrixValue> out = createWritableStream(metadata);
		OOCInstructionUtils.equiJoin(left, right, out,
			(leftBlock, rightBlock) -> leftBlock.binaryOperations(operator, rightBlock, new MatrixBlock()),
			getContext());
		return out;
	}

	private MatrixBlock mask(MatrixBlock mask, MatrixBlock blk) {
		for(int i = 0; i < blk.getNumRows(); i++) {
			for(int j = 0; j < blk.getNumColumns(); j++) {
				if(mask.get(i,j) ==0) blk.set(i, j, 0);
			}
		}
		return blk;
	}
}
