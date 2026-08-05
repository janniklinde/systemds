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
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.LocalTaskQueue;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.DoubleObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.ooc.primitives.RepartitionOOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.SubOOCStream;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.util.IndexRange;
import org.apache.sysds.runtime.util.UtilFunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MatrixIndexingOOCInstruction extends IndexingOOCInstruction {

	public MatrixIndexingOOCInstruction(CPOperand in, CPOperand rl, CPOperand ru, CPOperand cl, CPOperand cu,
		CPOperand out, String opcode, String istr) {
		super(in, rl, ru, cl, cu, out, opcode, istr);
	}

	public MatrixIndexingOOCInstruction(CPOperand lhsInput, CPOperand rhsInput, CPOperand rl, CPOperand ru,
		CPOperand cl, CPOperand cu, CPOperand out, String opcode, String istr) {
		super(lhsInput, rhsInput, rl, ru, cl, cu, out, opcode, istr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		String opcode = getOpcode();
		IndexRange ix = getIndexRange(ec);

		MatrixObject mo = ec.getMatrixObject(input1.getName());
		int blocksize = mo.getBlocksize();
		if(blocksize <= 0)
			throw new DMLRuntimeException("Planner-backed OOC indexing requires a positive block size.");
		long firstBlockRow = ix.rowStart / blocksize;
		long firstBlockCol = ix.colStart / blocksize;

		boolean inRange = (mo.getNumRows() < 0 || ix.rowStart < mo.getNumRows()) &&
			(mo.getNumColumns() < 0 || ix.colStart < mo.getNumColumns());

		//right indexing
		if(opcode.equalsIgnoreCase(Opcodes.RIGHT_INDEX.toString())) {
			OOCStream<IndexedMatrixValue> qIn = mo.getStreamHandle();
			addInStream(qIn);

			if(output.isScalar()) {
				if(!inRange)
					throw new DMLRuntimeException(
						"Invalid values for matrix indexing: [" + (ix.rowStart + 1) + ":" + (ix.rowEnd + 1) + "," +
							(ix.colStart + 1) + ":" + (ix.colEnd + 1) + "] must be within matrix dimensions [" +
							mo.getNumRows() + "x" + mo.getNumColumns() + "].");

				Double scalarOut = null;
				IndexedMatrixValue tmp;

				qIn.start();
				while((tmp = qIn.dequeue()) != LocalTaskQueue.NO_MORE_TASKS) {
					if(tmp.getIndexes().getRowIndex() == firstBlockRow + 1 &&
						tmp.getIndexes().getColumnIndex() == firstBlockCol + 1) {
						scalarOut = ((MatrixBlock) tmp.getValue()).get((int) (ix.rowStart % blocksize),
							(int) (ix.colStart % blocksize));
					}
				}
				if(scalarOut == null)
					throw new DMLRuntimeException("Desired block not found");
				ec.setScalarOutput(output.getName(), new DoubleObject(scalarOut));
				return;
			}

			if(ix.rowStart < 0 || ix.rowEnd < ix.rowStart || ix.colStart < 0 || ix.colEnd < ix.colStart ||
				(mo.getNumRows() >= 0 && ix.rowEnd >= mo.getNumRows()) ||
				(mo.getNumColumns() >= 0 && ix.colEnd >= mo.getNumColumns())) {
				String dbg = "inst=\"" + instString + "\", input=" + input1.getName() + ", output=" + output.getName() +
					", rowLower=" + debugScalarOperand(rowLower, ec) + ", rowUpper=" +
					debugScalarOperand(rowUpper, ec) + ", colLower=" + debugScalarOperand(colLower, ec) +
					", colUpper=" + debugScalarOperand(colUpper, ec) + ", resolvedRange=[" + (ix.rowStart + 1) + ":" +
					(ix.rowEnd + 1) + "," + (ix.colStart + 1) + ":" + (ix.colEnd + 1) + "]" + ", matrixDims=[" +
					mo.getNumRows() + "x" + mo.getNumColumns() + "]" + ", blocksize=" + blocksize;
				System.out.println("[WARN] OOC rightIndex bounds violation: " + dbg);
				throw new DMLRuntimeException(
					"Invalid values for matrix indexing: [" + (ix.rowStart + 1) + ":" + (ix.rowEnd + 1) + "," +
						(ix.colStart + 1) + ":" + (ix.colEnd + 1) + "] must be within matrix dimensions [" +
						mo.getNumRows() + "x" + mo.getNumColumns() + "]. " + dbg);
			}

			MatrixObject mOut = ec.getMatrixObject(output);
			ec.getDataCharacteristics(output.getName()).set(ix.rowSpan() + 1, ix.colSpan() + 1, blocksize, -1);
			OOCStream<IndexedMatrixValue> qOut = createWritableStream();
			addOutStream(qOut);
			mOut.setStreamHandle(qOut);

			OOCInstructionUtils.repartition(qIn, qOut, outputIndex -> {
				long sourceRowStart = ix.rowStart + (outputIndex.getRowIndex() - 1) * blocksize;
				long sourceColStart = ix.colStart + (outputIndex.getColumnIndex() - 1) * blocksize;
				long sourceRowEnd = Math.min(ix.rowEnd, sourceRowStart + blocksize - 1);
				long sourceColEnd = Math.min(ix.colEnd, sourceColStart + blocksize - 1);
				int rowFragments = (int) (sourceRowEnd / blocksize - sourceRowStart / blocksize + 1);
				int colFragments = (int) (sourceColEnd / blocksize - sourceColStart / blocksize + 1);
				return rowFragments * colFragments;
			}, (tile, emit) -> {
				MatrixBlock block = (MatrixBlock) tile.getValue();
				long inputRowStart = (tile.getIndexes().getRowIndex() - 1) * blocksize;
				long inputColStart = (tile.getIndexes().getColumnIndex() - 1) * blocksize;
				long rowStart = Math.max(ix.rowStart, inputRowStart);
				long colStart = Math.max(ix.colStart, inputColStart);
				long rowEnd = Math.min(ix.rowEnd + 1, inputRowStart + block.getNumRows());
				long colEnd = Math.min(ix.colEnd + 1, inputColStart + block.getNumColumns());
				if(rowStart >= rowEnd || colStart >= colEnd)
					return;

				long outputRowStart = (rowStart - ix.rowStart) / blocksize;
				long outputRowEnd = (rowEnd - ix.rowStart - 1) / blocksize;
				long outputColStart = (colStart - ix.colStart) / blocksize;
				long outputColEnd = (colEnd - ix.colStart - 1) / blocksize;
				for(long outputRow = outputRowStart; outputRow <= outputRowEnd; outputRow++)
					for(long outputCol = outputColStart; outputCol <= outputColEnd; outputCol++) {
						long targetRowStart = ix.rowStart + outputRow * blocksize;
						long targetColStart = ix.colStart + outputCol * blocksize;
						long copyRowStart = Math.max(rowStart, targetRowStart);
						long copyColStart = Math.max(colStart, targetColStart);
						int rows = (int) (Math.min(rowEnd, targetRowStart + blocksize) - copyRowStart);
						int cols = (int) (Math.min(colEnd, targetColStart + blocksize) - copyColStart);
						emit.copy(new MatrixIndexes(outputRow + 1, outputCol + 1), (int) (copyRowStart - inputRowStart),
							(int) (copyColStart - inputColStart), rows, cols, (int) (copyRowStart - targetRowStart),
							(int) (copyColStart - targetColStart));
					}
			}, getContext());
			return;
		}
		else if(opcode.equalsIgnoreCase(Opcodes.LEFT_INDEX.toString())) {
			MatrixObject mOut = ec.getMatrixObject(output);
			ec.getDataCharacteristics(output.getName()).set(mo.getNumRows(), mo.getNumColumns(), blocksize, -1);
			if(input2.getDataType().isScalar()) {
				if(!ix.isScalar())
					throw new DMLRuntimeException("Invalid index range of scalar leftindexing: " + ix + ".");
				if(ix.rowStart < 0 || ix.rowStart >= mo.getNumRows() || ix.colStart < 0 ||
					ix.colStart >= mo.getNumColumns()) {
					throw new DMLRuntimeException(
						"Invalid values for matrix indexing: [" + (ix.rowStart + 1) + ":" + (ix.rowEnd + 1) + "," +
							(ix.colStart + 1) + ":" + (ix.colEnd + 1) + "] must be within matrix dimensions [" +
							mo.getNumRows() + "x" + mo.getNumColumns() + "].");
				}

				final ScalarObject scalar = ec.getScalarInput(input2.getName(), ValueType.FP64, input2.isLiteral());
				final double scalarValue = scalar.getDoubleValue();
				final long targetBlockRow = ix.rowStart / blocksize + 1;
				final long targetBlockCol = ix.colStart / blocksize + 1;
				final int targetLocalRow = (int) (ix.rowStart % blocksize);
				final int targetLocalCol = (int) (ix.colStart % blocksize);

				if(mo.getDataCharacteristics().dimsKnown() && blocksize > 0) {
					OOCStream<IndexedMatrixValue> result = createWritableStream();
					mOut.setStreamHandle(result);
					OOCInstructionUtils.equiMap(mo.getStreamable(), result, value -> {
						MatrixBlock source = (MatrixBlock) value.getValue();
						MatrixIndexes index = value.getIndexes();
						if(index.getRowIndex() != targetBlockRow || index.getColumnIndex() != targetBlockCol)
							return source;
						MatrixBlock updated = new MatrixBlock(source);
						updated.set(targetLocalRow, targetLocalCol, scalarValue);
						updated.examSparsity();
						return updated;
					}, getContext());
					return;
				}

				OOCStream<IndexedMatrixValue> qLhs = mo.getStreamHandle();
				OOCStream<IndexedMatrixValue> qOutRaw = createWritableStream();
				SubOOCStream<IndexedMatrixValue> qOut = new SubOOCStream<>(qOutRaw);
				addInStream(qLhs);
				addOutStream(qOut);
				mOut.setStreamHandle(qOut);

				submitOOCTasks(qLhs, cb -> {
					IndexedMatrixValue lhs = cb.get();
					MatrixIndexes idx = lhs.getIndexes();
					if(idx.getRowIndex() != targetBlockRow || idx.getColumnIndex() != targetBlockCol) {
						qOut.enqueue(cb.keepOpen());
						return;
					}

					MatrixBlock src = (MatrixBlock) lhs.getValue();
					MatrixBlock updated = new MatrixBlock(src);
					updated.set(targetLocalRow, targetLocalCol, scalarValue);
					updated.examSparsity();
					qOut.enqueue(new IndexedMatrixValue(new MatrixIndexes(idx), updated));
				}).thenRun(() -> {
					qOut.closeInput();
					qOutRaw.closeInput();
				}).exceptionally(err -> {
					DMLRuntimeException dmlErr = DMLRuntimeException.of(err);
					qOut.propagateFailure(dmlErr);
					qOutRaw.propagateFailure(dmlErr);
					qOutRaw.closeInput();
					return null;
				});
				qLhs.start();
				return;
			}

			final MatrixObject rhsMo = ec.getMatrixObject(input2.getName());
			final long lhsRows = mo.getNumRows();
			final long lhsCols = mo.getNumColumns();
			final long rhsRows = rhsMo.getNumRows();
			final long rhsCols = rhsMo.getNumColumns();

			if(ix.rowSpan() + 1 != rhsRows || ix.colSpan() + 1 != rhsCols) {
				throw new DMLRuntimeException(
					"Invalid index range of leftindexing: [" + (ix.rowStart + 1) + ":" + (ix.rowEnd + 1) + "," +
						(ix.colStart + 1) + ":" + (ix.colEnd + 1) + "] vs [" + rhsRows + "x" + rhsCols + "].");
			}
			if(ix.rowStart < 0 || ix.rowStart >= lhsRows || ix.rowEnd < ix.rowStart || ix.rowEnd >= lhsRows ||
				ix.colStart < 0 || ix.colStart >= lhsCols || ix.colEnd < ix.colStart || ix.colEnd >= lhsCols) {
				throw new DMLRuntimeException(
					"Invalid values for matrix indexing: [" + (ix.rowStart + 1) + ":" + (ix.rowEnd + 1) + "," +
						(ix.colStart + 1) + ":" + (ix.colEnd + 1) + "] must be within matrix dimensions [" + lhsRows +
						"x" + lhsCols + "].");
			}

			if(mo.getDataCharacteristics().dimsKnown() && rhsMo.getDataCharacteristics().dimsKnown() && blocksize > 0 &&
				rhsMo.getBlocksize() > 0) {
				int rhsBlocksize = rhsMo.getBlocksize();
				OOCStream<IndexedMatrixValue> result = createWritableStream();
				mOut.setStreamHandle(result);
				OOCInstructionUtils.repartition(List.of(mo.getStreamable(), rhsMo.getStreamable()), result,
					outputIndex -> expectedLeftIndexFragments(outputIndex, ix, blocksize, rhsBlocksize, lhsRows,
						lhsCols),
					List.of((tile, emit) -> routeLeftIndexLhs(tile, ix, blocksize, emit),
						(tile, emit) -> routeLeftIndexRhs(tile, ix, blocksize, rhsBlocksize, emit)),
					getContext());
				return;
			}

			final IndexRange shiftRange = new IndexRange(ix.rowStart + 1, ix.rowEnd + 1, ix.colStart + 1,
				ix.colEnd + 1);
			final BinaryOperator plus = InstructionUtils.parseBinaryOperator(Opcodes.PLUS.toString());

			OOCStream<IndexedMatrixValue> qLhs = mo.getStreamHandle();
			OOCStream<IndexedMatrixValue> qRhs = rhsMo.getStreamHandle();
			OOCStream<IndexedMatrixValue> qOutRaw = createWritableStream();
			SubOOCStream<IndexedMatrixValue> qOut = new SubOOCStream<>(qOutRaw);

			addInStream(qLhs, qRhs);
			addOutStream(qOut);
			mOut.setStreamHandle(qOut);

			final Map<MatrixIndexes, LeftIndexAccumulator> aggregators = new ConcurrentHashMap<>();
			submitOOCTasks(List.of(qLhs, qRhs), (streamIdx, cb) -> {
				if(streamIdx == 0) {
					IndexedMatrixValue lhs = cb.get();
					MatrixIndexes lhsIx = lhs.getIndexes();
					if(!UtilFunctions.isInBlockRange(lhsIx, blocksize, shiftRange)) {
						qOut.enqueue(cb.keepOpen());
						return;
					}

					MatrixIndexes key = new MatrixIndexes(lhsIx);
					int expectedRhsContribs = getExpectedRhsContribs(key, shiftRange, blocksize, lhsRows, lhsCols);
					LeftIndexAccumulator acc = aggregators.computeIfAbsent(key,
						k -> new LeftIndexAccumulator(expectedRhsContribs));

					IndexRange zeroRange = UtilFunctions.getSelectedRangeForZeroOut(lhs, blocksize, shiftRange);
					MatrixBlock lhsZeroed = ((MatrixBlock) lhs.getValue()).zeroOutOperations(new MatrixBlock(),
						zeroRange);

					MatrixBlock out = acc.addLhs(lhsZeroed, plus);
					if(out != null) {
						if(!aggregators.remove(key, acc))
							throw new DMLRuntimeException(
								"Failed to remove completed LEFT_INDEX accumulator for " + key);
						out.examSparsity();
						qOut.enqueue(new IndexedMatrixValue(new MatrixIndexes(key), out));
					}
				}
				else {
					IndexedMatrixValue rhs = cb.get();
					ArrayList<IndexedMatrixValue> shifted = new ArrayList<>();
					OperationsOnMatrixValues.performShift(rhs, shiftRange, blocksize, lhsRows, lhsCols, shifted);

					for(IndexedMatrixValue part : shifted) {
						MatrixIndexes key = new MatrixIndexes(part.getIndexes());
						LeftIndexAccumulator acc = aggregators.computeIfAbsent(key, k -> new LeftIndexAccumulator(
							getExpectedRhsContribs(k, shiftRange, blocksize, lhsRows, lhsCols)));

						MatrixBlock out = acc.addRhs((MatrixBlock) part.getValue(), plus);
						if(out != null) {
							if(!aggregators.remove(key, acc))
								throw new DMLRuntimeException(
									"Failed to remove completed LEFT_INDEX accumulator for " + key);
							out.examSparsity();
							qOut.enqueue(new IndexedMatrixValue(new MatrixIndexes(key), out));
						}
					}
				}
			}).thenRun(() -> {
				if(!aggregators.isEmpty())
					throw new DMLRuntimeException(
						"LEFT_INDEX finished with unfinished aggregators: " + aggregators.size());
				qOut.closeInput();
				qOutRaw.closeInput();
			}).exceptionally(err -> {
				DMLRuntimeException dmlErr = DMLRuntimeException.of(err);
				qOut.propagateFailure(dmlErr);
				qOutRaw.propagateFailure(dmlErr);
				qOutRaw.closeInput();
				return null;
			});
			qLhs.start();
			qRhs.start();
		}
		else
			throw new DMLRuntimeException(
				"Invalid opcode (" + opcode + ") encountered in MatrixIndexingOOCInstruction.");
	}

	private static int expectedLeftIndexFragments(MatrixIndexes outputIndex, IndexRange overwrite, int blocksize,
		int rhsBlocksize, long rows, long cols) {
		long rowStart = (outputIndex.getRowIndex() - 1) * blocksize;
		long rowEnd = Math.min(rows, rowStart + blocksize);
		long colStart = (outputIndex.getColumnIndex() - 1) * blocksize;
		long colEnd = Math.min(cols, colStart + blocksize);
		long overwriteRowStart = Math.max(rowStart, overwrite.rowStart);
		long overwriteRowEnd = Math.min(rowEnd, overwrite.rowEnd + 1);
		long overwriteColStart = Math.max(colStart, overwrite.colStart);
		long overwriteColEnd = Math.min(colEnd, overwrite.colEnd + 1);
		if(overwriteRowStart >= overwriteRowEnd || overwriteColStart >= overwriteColEnd)
			return 1;

		int fragments = 0;
		if(rowStart < overwriteRowStart)
			fragments++;
		if(overwriteRowEnd < rowEnd)
			fragments++;
		if(colStart < overwriteColStart)
			fragments++;
		if(overwriteColEnd < colEnd)
			fragments++;

		long rhsRowStart = overwriteRowStart - overwrite.rowStart;
		long rhsRowEnd = overwriteRowEnd - overwrite.rowStart;
		long rhsColStart = overwriteColStart - overwrite.colStart;
		long rhsColEnd = overwriteColEnd - overwrite.colStart;
		long rhsRowBlocks = (rhsRowEnd - 1) / rhsBlocksize - rhsRowStart / rhsBlocksize + 1;
		long rhsColBlocks = (rhsColEnd - 1) / rhsBlocksize - rhsColStart / rhsBlocksize + 1;
		return Math.toIntExact(fragments + rhsRowBlocks * rhsColBlocks);
	}

	private static void routeLeftIndexLhs(IndexedMatrixValue tile, IndexRange overwrite, int blocksize,
		RepartitionOOCPrimitive.FragmentEmitter emit) {
		MatrixBlock block = (MatrixBlock) tile.getValue();
		long rowStart = (tile.getIndexes().getRowIndex() - 1) * blocksize;
		long rowEnd = rowStart + block.getNumRows();
		long colStart = (tile.getIndexes().getColumnIndex() - 1) * blocksize;
		long colEnd = colStart + block.getNumColumns();
		long overwriteRowStart = Math.max(rowStart, overwrite.rowStart);
		long overwriteRowEnd = Math.min(rowEnd, overwrite.rowEnd + 1);
		long overwriteColStart = Math.max(colStart, overwrite.colStart);
		long overwriteColEnd = Math.min(colEnd, overwrite.colEnd + 1);
		MatrixIndexes outputIndex = new MatrixIndexes(tile.getIndexes());
		if(overwriteRowStart >= overwriteRowEnd || overwriteColStart >= overwriteColEnd) {
			emit.copy(outputIndex, 0, 0, block.getNumRows(), block.getNumColumns(), 0, 0);
			return;
		}

		if(rowStart < overwriteRowStart)
			emit.copy(outputIndex, 0, 0, (int) (overwriteRowStart - rowStart), block.getNumColumns(), 0, 0);
		if(overwriteRowEnd < rowEnd) {
			int localRow = (int) (overwriteRowEnd - rowStart);
			emit.copy(outputIndex, localRow, 0, (int) (rowEnd - overwriteRowEnd), block.getNumColumns(), localRow, 0);
		}
		int middleRow = (int) (overwriteRowStart - rowStart);
		int middleRows = (int) (overwriteRowEnd - overwriteRowStart);
		if(colStart < overwriteColStart)
			emit.copy(outputIndex, middleRow, 0, middleRows, (int) (overwriteColStart - colStart), middleRow, 0);
		if(overwriteColEnd < colEnd) {
			int localCol = (int) (overwriteColEnd - colStart);
			emit.copy(outputIndex, middleRow, localCol, middleRows, (int) (colEnd - overwriteColEnd), middleRow,
				localCol);
		}
	}

	private static void routeLeftIndexRhs(IndexedMatrixValue tile, IndexRange overwrite, int blocksize,
		int rhsBlocksize, RepartitionOOCPrimitive.FragmentEmitter emit) {
		MatrixBlock block = (MatrixBlock) tile.getValue();
		long sourceRowStart = (tile.getIndexes().getRowIndex() - 1) * rhsBlocksize;
		long sourceRowEnd = sourceRowStart + block.getNumRows();
		long sourceColStart = (tile.getIndexes().getColumnIndex() - 1) * rhsBlocksize;
		long sourceColEnd = sourceColStart + block.getNumColumns();
		long targetRowStart = overwrite.rowStart + sourceRowStart;
		long targetRowEnd = overwrite.rowStart + sourceRowEnd;
		long targetColStart = overwrite.colStart + sourceColStart;
		long targetColEnd = overwrite.colStart + sourceColEnd;
		long outputRowStart = targetRowStart / blocksize;
		long outputRowEnd = (targetRowEnd - 1) / blocksize;
		long outputColStart = targetColStart / blocksize;
		long outputColEnd = (targetColEnd - 1) / blocksize;
		for(long outputRow = outputRowStart; outputRow <= outputRowEnd; outputRow++)
			for(long outputCol = outputColStart; outputCol <= outputColEnd; outputCol++) {
				long outputGlobalRow = outputRow * blocksize;
				long outputGlobalCol = outputCol * blocksize;
				long copyRowStart = Math.max(targetRowStart, outputGlobalRow);
				long copyRowEnd = Math.min(targetRowEnd, outputGlobalRow + blocksize);
				long copyColStart = Math.max(targetColStart, outputGlobalCol);
				long copyColEnd = Math.min(targetColEnd, outputGlobalCol + blocksize);
				emit.copy(new MatrixIndexes(outputRow + 1, outputCol + 1), (int) (copyRowStart - targetRowStart),
					(int) (copyColStart - targetColStart), (int) (copyRowEnd - copyRowStart),
					(int) (copyColEnd - copyColStart), (int) (copyRowStart - outputGlobalRow),
					(int) (copyColStart - outputGlobalCol));
			}
	}

	private static String debugScalarOperand(CPOperand op, ExecutionContext ec) {
		try {
			return op.getName() + "=" + ec.getScalarInput(op).getStringValue() + (op.isLiteral() ? " [lit]" : " [var]");
		}
		catch(Exception ex) {
			return op.getName() + "=<unavailable:" + ex.getClass().getSimpleName() + ">";
		}
	}

	private static int getExpectedRhsContribs(MatrixIndexes lhsIx, IndexRange shift, int bs, long lhsRows,
		long lhsCols) {

		long lrs = UtilFunctions.computeCellIndex(lhsIx.getRowIndex(), bs, 0);
		long lcs = UtilFunctions.computeCellIndex(lhsIx.getColumnIndex(), bs, 0);
		long lre = lrs + UtilFunctions.computeBlockSize(lhsRows, lhsIx.getRowIndex(), bs) - 1;
		long lce = lcs + UtilFunctions.computeBlockSize(lhsCols, lhsIx.getColumnIndex(), bs) - 1;

		long ors = Math.max(lrs, shift.rowStart), ore = Math.min(lre, shift.rowEnd);
		long ocs = Math.max(lcs, shift.colStart), oce = Math.min(lce, shift.colEnd);
		if(ors > ore || ocs > oce)
			return 0;

		long rhsRowStart = ors - shift.rowStart + 1;
		long rhsColStart = ocs - shift.colStart + 1;
		long rowLen = ore - ors + 1;
		long colLen = oce - ocs + 1;

		long rBlocks = UtilFunctions.computeBlockIndex(rhsRowStart + rowLen - 1, bs) -
			UtilFunctions.computeBlockIndex(rhsRowStart, bs) + 1;
		long cBlocks = UtilFunctions.computeBlockIndex(rhsColStart + colLen - 1, bs) -
			UtilFunctions.computeBlockIndex(rhsColStart, bs) + 1;

		return Math.toIntExact(rBlocks * cBlocks);
	}

	private static class LeftIndexAccumulator {
		private final int _expectedRhsContribs;
		private MatrixBlock _lhs;
		private MatrixBlock _rhsAgg;
		private int _rhsCtr;
		private boolean _lhsSeen;
		private boolean _emitted;

		private LeftIndexAccumulator(int expectedRhsContribs) {
			_expectedRhsContribs = expectedRhsContribs;
			_rhsCtr = 0;
			_lhsSeen = false;
			_emitted = false;
		}

		public synchronized MatrixBlock addLhs(MatrixBlock lhs, BinaryOperator plus) {
			if(_lhsSeen)
				throw new DMLRuntimeException("Duplicate LEFT_INDEX lhs contribution encountered");
			_lhs = lhs;
			_lhsSeen = true;
			return emitIfReady(plus);
		}

		public synchronized MatrixBlock addRhs(MatrixBlock rhs, BinaryOperator plus) {
			if(_emitted)
				throw new DMLRuntimeException("LEFT_INDEX accumulator received rhs after completion");
			_rhsCtr++;
			if(_rhsCtr > _expectedRhsContribs)
				throw new DMLRuntimeException(
					"LEFT_INDEX accumulator rhs overflow: " + _rhsCtr + " > " + _expectedRhsContribs);
			if(_rhsAgg == null)
				_rhsAgg = rhs;
			else
				_rhsAgg = _rhsAgg.binaryOperationsInPlace(plus, rhs);
			return emitIfReady(plus);
		}

		private MatrixBlock emitIfReady(BinaryOperator plus) {
			if(_emitted || !_lhsSeen || _rhsCtr < _expectedRhsContribs)
				return null;
			if(_rhsCtr > _expectedRhsContribs)
				throw new DMLRuntimeException("LEFT_INDEX accumulator encountered invalid rhs contribution count");
			_emitted = true;
			if(_rhsAgg != null)
				_lhs = _lhs.binaryOperationsInPlace(plus, _rhsAgg);
			return _lhs;
		}
	}
}
