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
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.DiagIndex;
import org.apache.sysds.runtime.functionobjects.RevIndex;
import org.apache.sysds.runtime.functionobjects.SortIndex;
import org.apache.sysds.runtime.functionobjects.SwapIndex;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.ReorgOperator;
import org.apache.sysds.runtime.ooc.primitives.RepartitionOOCPrimitive;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.util.DataConverter;
import org.apache.sysds.runtime.util.UtilFunctions;

public class ReorgOOCInstruction extends ComputationOOCInstruction {
	// sort-specific attributes (to enable variable attributes)
	private final CPOperand _col;
	private final CPOperand _desc;
	private final CPOperand _ixret;

	protected ReorgOOCInstruction(ReorgOperator op, CPOperand in1, CPOperand out, String opcode, String istr) {
		this(op, in1, out, null, null, null, opcode, istr);
	}

	private ReorgOOCInstruction(Operator op, CPOperand in, CPOperand out, CPOperand col, CPOperand desc, CPOperand ixret,
		String opcode, String istr) {
		super(OOCType.Reorg, op, in, out, opcode, istr);
		_col = col;
		_desc = desc;
		_ixret = ixret;
	}

	public static ReorgOOCInstruction parseInstruction(String str) {
		CPOperand in = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);
		CPOperand out = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);

		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if(opcode.equalsIgnoreCase(Opcodes.TRANSPOSE.toString())) {
			InstructionUtils.checkNumFields(str, 2, 3);
			in.split(parts[1]);
			out.split(parts[2]);

			ReorgOperator reorg = new ReorgOperator(SwapIndex.getSwapIndexFnObject());
			return new ReorgOOCInstruction(reorg, in, out, opcode, str);
		}
		else if(opcode.equalsIgnoreCase(Opcodes.SORT.toString())) {
			InstructionUtils.checkNumFields(str, 5, 6);
			in.split(parts[1]);
			out.split(parts[5]);
			CPOperand col = new CPOperand(parts[2]);
			CPOperand desc = new CPOperand(parts[3]);
			CPOperand ixret = new CPOperand(parts[4]);
			int k = Integer.parseInt(parts[6]);
			return new ReorgOOCInstruction(new ReorgOperator(new SortIndex(1, false, false), k),
				in, out, col, desc, ixret, opcode, str);
		}
		else if(opcode.equalsIgnoreCase(Opcodes.REV.toString())) {
			InstructionUtils.checkNumFields(str, 2, 3);
			in.split(parts[1]);
			out.split(parts[2]);
			//the emitted thread count belongs to CP, OOC parallelizes over the tiles of the stream instead
			return new ReorgOOCInstruction(new ReorgOperator(RevIndex.getRevIndexFnObject()), in, out, opcode, str);
		}
		else if(opcode.equalsIgnoreCase(Opcodes.DIAG.toString())) {
			InstructionUtils.checkNumFields(str, 2);
			in.split(parts[1]);
			out.split(parts[2]);
			return new ReorgOOCInstruction(new ReorgOperator(DiagIndex.getDiagIndexFnObject()), in, out, opcode, str);
		}
		else
			throw new NotImplementedException("Unsupported OOC reorg opcode: " + opcode);
	}

	public void processInstruction( ExecutionContext ec ) {
		// Create thread and process the transpose/sort operation
		MatrixObject min = ec.getMatrixObject(input1);
		ReorgOperator r_op = (ReorgOperator) _optr;

		if(r_op.fn instanceof SortIndex) {
			//additional attributes for sort
			int[] cols = _col.getDataType().isMatrix() ? DataConverter.convertToIntVector(ec.getMatrixInput(_col.getName())) :
				new int[]{(int)ec.getScalarInput(_col).getLongValue()};
			boolean desc = ec.getScalarInput(_desc).getBooleanValue();
			boolean ixret = ec.getScalarInput(_ixret).getBooleanValue();
			r_op = r_op.setFn(new SortIndex(cols, desc, ixret));

			// For now, we reuse the CP instruction
			// In future, we could optimize by building the permutation and streaming blocks column by column
			MatrixBlock matBlock = min.acquireRead();
			MatrixBlock soresBlock = matBlock.reorgOperations(r_op, new MatrixBlock(), 0, 0, 0);
			if (_col.getDataType().isMatrix())
				ec.releaseMatrixInput(_col.getName());
			ec.releaseMatrixInput(input1.getName());
			ec.setMatrixOutput(output.getName(), soresBlock);
		} else if(r_op.fn instanceof SwapIndex) {
			OOCStreamable<IndexedMatrixValue> qIn = min.getStreamable();
			OOCStream<IndexedMatrixValue> qOut = createWritableStream();
			OOCInstructionUtils.propagateDims(ec, output, min.getNumColumns(), min.getNumRows(), min.getBlocksize(),
				min.getDataCharacteristics().getNonZeros());
			ec.getMatrixObject(output).setStreamHandle(qOut);

			OOCInstructionUtils.transposedMap(qIn, qOut,
				block -> block.reorgOperations((ReorgOperator) _optr, new MatrixBlock(), -1, -1, -1), getContext());
		}
		else if(r_op.fn instanceof DiagIndex) {
			processDiag(ec, min);
		}
		else if(r_op.fn instanceof RevIndex) {
			processRev(ec, min);
		}
		else
			throw new NotImplementedException("Unsupported OOC reorg operation: " + r_op.fn.getClass().getSimpleName());
	}

	/**
	 * Out-of-core {@code diag}. Both variants are block-local: {@code diagV2M} turns the input column block i into the
	 * diagonal output block (i,i) and pads its block row with empty blocks, {@code diagM2V} keeps only the diagonal
	 * blocks and extracts their diagonal. Neither variant needs to see more than one block at a time.
	 */
	private void processDiag(ExecutionContext ec, MatrixObject min) {
		final long rlen = min.getNumRows();
		final long clen = min.getNumColumns();
		final int blen = min.getBlocksize();
		if(rlen < 0 || clen < 0 || blen <= 0)
			throw new DMLRuntimeException("OOC diag requires known input dimensions and block size, got " + rlen + "x"
				+ clen + " with blocksize " + blen);
		final boolean v2m = (clen == 1);
		if(!v2m && rlen != clen)
			throw new DMLRuntimeException("OOC diag requires a square input matrix, got " + rlen + "x" + clen);

		OOCInstructionUtils.propagateDims(ec, output, rlen, v2m ? rlen : 1, blen, v2m ? min.getNnz() : -1);
		MatrixObject mout = ec.getMatrixObject(output);
		OOCStream<IndexedMatrixValue> qOut = createWritableStream(mout);
		mout.setStreamHandle(qOut);

		final ReorgOperator op = (ReorgOperator) _optr;
		final long taskBytes = 3 * OOCUtils.estimateOutputTileBytes(mout.getDataCharacteristics());
		if(v2m) {
			final int numBlocks = (int) Math.ceil((double) rlen / blen);
			OOCInstructionUtils.flatMap(min.getStreamable(), qOut, value -> {
				long rix = value.getIndexes().getRowIndex();
				ArrayList<IndexedMatrixValue> blocks = new ArrayList<>(numBlocks);
				blocks.add(new IndexedMatrixValue(new MatrixIndexes(rix, rix),
					((MatrixBlock) value.getValue()).reorgOperations(op, new MatrixBlock(), -1, -1, -1)));
				//the remainder of the block row is empty but has to be materialized so that consumers observe a
				//complete block geometry, mirroring the Spark diagV2M mapping
				int lrlen = UtilFunctions.computeBlockSize(rlen, rix, blen);
				for(int i = 1; i <= numBlocks; i++)
					if(i != rix)
						blocks.add(new IndexedMatrixValue(new MatrixIndexes(rix, i),
							new MatrixBlock(lrlen, UtilFunctions.computeBlockSize(rlen, i, blen), true)));
				return blocks;
			}, OOCUtils::memoryCharge, taskBytes, getContext());
		}
		else {
			OOCInstructionUtils.flatMap(min.getStreamable(), qOut, value -> {
				MatrixIndexes ix = value.getIndexes();
				if(ix.getRowIndex() != ix.getColumnIndex())
					return Collections.emptyList();
				return List.of(new IndexedMatrixValue(new MatrixIndexes(ix.getRowIndex(), 1),
					((MatrixBlock) value.getValue()).reorgOperations(op, new MatrixBlock(), -1, -1, -1)));
			}, OOCUtils::memoryCharge, taskBytes, getContext());
		}
	}

	/**
	 * Out-of-core {@code rev}. For a row count that is a multiple of the block size the reversal is block-local and
	 * only the block row index flips. Otherwise every reversed block straddles two output blocks, so the reversed
	 * blocks are shifted into place by a repartition instead of being merged after the fact.
	 */
	private void processRev(ExecutionContext ec, MatrixObject min) {
		final long rlen = min.getNumRows();
		final long clen = min.getNumColumns();
		final int blen = min.getBlocksize();
		if(rlen < 0 || clen < 0 || blen <= 0)
			throw new DMLRuntimeException("OOC rev requires known input dimensions and block size, got " + rlen + "x"
				+ clen + " with blocksize " + blen);

		OOCInstructionUtils.propagateDims(ec, output, rlen, clen, blen, min.getNnz());
		MatrixObject mout = ec.getMatrixObject(output);
		OOCStream<IndexedMatrixValue> qOut = createWritableStream(mout);
		mout.setStreamHandle(qOut);

		final ReorgOperator op = (ReorgOperator) _optr;
		if(rlen % blen == 0 || rlen <= blen) {
			final long rowBlocks = (long) Math.ceil((double) rlen / blen);
			OOCInstructionUtils.flatMap(min.getStreamable(), qOut,
				value -> List.of(new IndexedMatrixValue(
					new MatrixIndexes(rowBlocks - value.getIndexes().getRowIndex() + 1,
						value.getIndexes().getColumnIndex()),
					((MatrixBlock) value.getValue()).reorgOperations(op, new MatrixBlock(), -1, -1, -1))),
				OOCUtils::memoryCharge, 3 * OOCUtils.estimateOutputTileBytes(mout.getDataCharacteristics()),
				getContext());
			return;
		}

		//reverse each block in place first, so that its rows map onto a contiguous ascending output row range and the
		//repartition only has to shift them by the block misalignment
		OOCStream<IndexedMatrixValue> reversed = createWritableStream(rlen, clen, blen);
		OOCInstructionUtils.equiMapBlock(min.getStreamable(), reversed,
			block -> block.reorgOperations(op, new MatrixBlock(), -1, -1, -1), getContext());
		OOCInstructionUtils.repartition(reversed, qOut, index -> countRevFragments(index, rlen, blen),
			(tile, emit) -> routeRev(tile, rlen, blen, emit), getContext());
	}

	/**
	 * Number of input block rows that overlap the given output block. Output row r reads input row {@code rlen-r+1}, so
	 * the output block covers one contiguous input row range that spans at most two input block rows.
	 */
	private static int countRevFragments(MatrixIndexes outputIndex, long rlen, int blen) {
		long begin = (outputIndex.getRowIndex() - 1) * blen + 1;
		long end = Math.min(rlen, outputIndex.getRowIndex() * blen);
		long inputBegin = rlen - end + 1;
		long inputEnd = rlen - begin + 1;
		return Math.toIntExact((inputEnd - 1) / blen - (inputBegin - 1) / blen + 1);
	}

	private static void routeRev(IndexedMatrixValue tile, long rlen, int blen,
		RepartitionOOCPrimitive.FragmentEmitter emit) {
		MatrixBlock block = (MatrixBlock) tile.getValue();
		int rows = block.getNumRows();
		int cols = block.getNumColumns();
		long colIndex = tile.getIndexes().getColumnIndex();
		//row 0 of the already reversed block is the last row of the input block, i.e. the smallest output row
		long position = rlen - ((tile.getIndexes().getRowIndex() - 1) * blen + rows) + 1;
		int row = 0;
		while(row < rows) {
			int destinationRow = (int) ((position - 1) % blen);
			int length = (int) Math.min(rows - row, blen - destinationRow);
			emit.copy(new MatrixIndexes((position - 1) / blen + 1, colIndex), row, 0, length, cols, destinationRow, 0);
			row += length;
			position += length;
		}
	}
}
