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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.SourceOOCStream;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public class ReblockOOCInstruction extends ComputationOOCInstruction {
	private static final long SOURCE_BULK_BYTES = 100_000_000L;
	private final int blen;

	private ReblockOOCInstruction(Operator op, CPOperand in, CPOperand out, int blocksize, String opcode,
		String instr) {
		super(OOCType.Reblock, op, in, out, opcode, instr);
		blen = blocksize;
	}

	public static ReblockOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		if(!opcode.equals(Opcodes.RBLK.toString()))
			throw new DMLRuntimeException("Incorrect opcode for ReblockOOCInstruction:" + opcode);

		CPOperand in = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		int blen = Integer.parseInt(parts[3]);
		return new ReblockOOCInstruction(null, in, out, blen, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject min = ec.getMatrixObject(input1);
		DataCharacteristics mc = ec.getDataCharacteristics(input1.getName());
		DataCharacteristics mcOut = ec.getDataCharacteristics(output.getName());
		mcOut.set(mc.getRows(), mc.getCols(), blen, mc.getNonZeros());

		MatrixObject mout = ec.getMatrixObject(output);
		if(!mc.dimsKnown() || mc.getRows() <= 0 || mc.getCols() <= 0 || mc.getBlocksize() <= 0 || blen <= 0) {
			SourceOOCStream source = new SourceOOCStream();
			source.setData(min);
			OOCIOHandler.SourceReadRequest request = new OOCIOHandler.SourceReadRequest(min.getFileName(),
				Types.FileFormat.BINARY, mc.getRows(), mc.getCols(), mc.getBlocksize(), mc.getNonZeros(),
				Long.MAX_VALUE, true, source);
			OOCCacheManager.getIOHandler().scheduleSourceRead(request).whenComplete((res, error) -> {
				if(error != null)
					source.propagateFailure(DMLRuntimeException.of(error));
			});
			mout.setStreamHandle(source);
			return;
		}

		OOCStream<IndexedMatrixValue> source = createWritableStream();
		source.setData(min);
		OOCStream<IndexedMatrixValue> untracked = createWritableStream();
		AtomicReference<Consumer<IndexedMatrixValue>> emitter = new AtomicReference<>();
		AtomicReference<OOCIOHandler.SourceReadContinuation> continuation = new AtomicReference<>();
		untracked.setSubscriber(callback -> {
			if(callback.isEos()) {
				callback.close();
				return;
			}
			try(callback) {
				emitter.get().accept(callback.get());
			}
		});
		long tileBytes = OOCUtils.estimateFullTileBytes(mc);
		long numBlocks = OOCUtils.getNumBlocks(mc);
		long totalBytes = numBlocks > Long.MAX_VALUE / tileBytes ? Long.MAX_VALUE : numBlocks * tileBytes;
		long productionLimit = Math.min(SOURCE_BULK_BYTES, totalBytes);
		long batchBytes = productionLimit > Long.MAX_VALUE - tileBytes ? Long.MAX_VALUE : productionLimit + tileBytes;
		long bulkBytes = Math.min(totalBytes, batchBytes);
		OOCIOHandler io = OOCCacheManager.getIOHandler();
		OOCInstructionUtils.uncoordinatedDataGen(source, bulkBytes, productionLimit, OOCAccessPattern.UNKNOWN,
			(byteLimit, active) -> {
				emitter.set(active);
				try {
					OOCIOHandler.SourceReadContinuation current = continuation.get();
					OOCIOHandler.SourceReadResult result;
					if(current == null) {
						OOCIOHandler.SourceReadRequest request = new OOCIOHandler.SourceReadRequest(min.getFileName(),
							Types.FileFormat.BINARY, mc.getRows(), mc.getCols(), mc.getBlocksize(), mc.getNonZeros(),
							byteLimit, true, untracked);
						result = io.scheduleSourceRead(request).get();
					}
					else
						result = io.continueSourceRead(current, byteLimit).get();
					continuation.set(result.continuation);
					return result.eof;
				}
				catch(Exception error) {
					throw DMLRuntimeException.of(error);
				}
				finally {
					emitter.set(null);
				}
			}, () -> {
			}, getContext());

		if(mc.getBlocksize() == blen) {
			mout.setStreamHandle(source);
			return;
		}

		OOCStream<IndexedMatrixValue> result = createWritableStream();
		mout.setStreamHandle(result);
		int inputBlen = mc.getBlocksize();
		OOCInstructionUtils.repartition(source, result, outputIndex -> {
			long outputRowStart = (outputIndex.getRowIndex() - 1) * blen;
			long outputColStart = (outputIndex.getColumnIndex() - 1) * blen;
			long outputRowEnd = Math.min(mc.getRows(), outputRowStart + blen) - 1;
			long outputColEnd = Math.min(mc.getCols(), outputColStart + blen) - 1;
			int rowFragments = (int) (outputRowEnd / inputBlen - outputRowStart / inputBlen + 1);
			int colFragments = (int) (outputColEnd / inputBlen - outputColStart / inputBlen + 1);
			return rowFragments * colFragments;
		}, (tile, emit) -> {
			MatrixBlock block = (MatrixBlock) tile.getValue();
			long inputRowStart = (tile.getIndexes().getRowIndex() - 1) * inputBlen;
			long inputColStart = (tile.getIndexes().getColumnIndex() - 1) * inputBlen;
			long inputRowEnd = inputRowStart + block.getNumRows();
			long inputColEnd = inputColStart + block.getNumColumns();
			for(long outputRow = inputRowStart / blen; outputRow <= (inputRowEnd - 1) / blen; outputRow++)
				for(long outputCol = inputColStart / blen; outputCol <= (inputColEnd - 1) / blen; outputCol++) {
					long outputRowStart = outputRow * blen;
					long outputColStart = outputCol * blen;
					long rowStart = Math.max(inputRowStart, outputRowStart);
					long colStart = Math.max(inputColStart, outputColStart);
					int rows = (int) (Math.min(inputRowEnd, outputRowStart + blen) - rowStart);
					int cols = (int) (Math.min(inputColEnd, outputColStart + blen) - colStart);
					emit.copy(new MatrixIndexes(outputRow + 1, outputCol + 1), (int) (rowStart - inputRowStart),
						(int) (colStart - inputColStart), rows, cols, (int) (rowStart - outputRowStart),
						(int) (colStart - outputColStart));
				}
		}, getContext());
	}
}
