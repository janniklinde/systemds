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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.io.FileFormatProperties;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.io.ReaderTextCSVParallel;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public class CSVReblockOOCInstruction extends ComputationOOCInstruction {
	private static final long MAX_ROW_BULK_BYTES = 100_000_000L;
	private static final long CSV_SCAN_BULK_BYTES = 1_000_000L;
	private final int blen;

	private CSVReblockOOCInstruction(Operator op, CPOperand in, CPOperand out, int blocklength, String opcode,
		String instr) {
		super(OOCType.Reblock, op, in, out, opcode, instr);
		blen = blocklength;
	}

	public static CSVReblockOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		if(!opcode.equals(Opcodes.CSVRBLK.toString()))
			throw new DMLRuntimeException("Incorrect opcode for CSVReblockOOCInstruction:" + opcode);

		CPOperand in = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		int blen = Integer.parseInt(parts[3]);
		return new CSVReblockOOCInstruction(null, in, out, blen, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		MatrixObject min = ec.getMatrixObject(input1);
		DataCharacteristics mc = ec.getDataCharacteristics(input1.getName());
		DataCharacteristics mcOut = ec.getDataCharacteristics(output.getName());
		mcOut.set(mc.getRows(), mc.getCols(), blen, mc.getNonZeros());

		OOCStream<IndexedMatrixValue> qOut = createWritableStream();
		MatrixObject mout = ec.getMatrixObject(output);
		mout.setStreamHandle(qOut);
		FileFormatProperties props = min.getFileFormatProperties();
		FileFormatPropertiesCSV csvProps = props instanceof FileFormatPropertiesCSV ? (FileFormatPropertiesCSV) props : new FileFormatPropertiesCSV();
		long maxBulkBytes = Math.min(MAX_ROW_BULK_BYTES, Runtime.getRuntime().maxMemory() / 3);
		AtomicLong bulkBytes = new AtomicLong(Math.min(CSV_SCAN_BULK_BYTES, maxBulkBytes));
		AtomicLong rowBytes = new AtomicLong();
		AtomicLong blockRows = new AtomicLong();
		AtomicLong columnBlocks = new AtomicLong();
		AtomicInteger readWorkers = new AtomicInteger(OptimizerUtils.getParallelTextReadParallelism());
		AtomicBoolean prepared = new AtomicBoolean();
		AtomicBoolean legacy = new AtomicBoolean();
		AtomicReference<Consumer<IndexedMatrixValue>> emitter = new AtomicReference<>();
		OOCStream<IndexedMatrixValue> untracked = createWritableStream();
		untracked.setSubscriber(callback -> {
			if(callback.isEos()) {
				callback.close();
				return;
			}
			try(callback) {
				if(legacy.get())
					qOut.enqueue(callback.keepOpen());
				else {
					Consumer<IndexedMatrixValue> active = emitter.get();
					if(active == null)
						throw new DMLRuntimeException("CSV reader emitted outside an admitted bulk read");
					active.accept(callback.get());
				}
			}
		});
		ReaderTextCSVParallel reader = new ReaderTextCSVParallel(csvProps);
		//the size pass has to run before this instruction returns: it is the only thing that knows the dimensions of
		//an unsized file, and every consumer downstream reads them while wiring its own plan
		try {
			prepared.set(true);
			reader.prepareStreamRead(min.getFileName(), mc.getRows(), mc.getCols(), blen, mc.getNonZeros(),
				(rows, cols) -> configureParallelRead(rows, cols, mc, mcOut, maxBulkBytes, bulkBytes, rowBytes,
					blockRows, columnBlocks, readWorkers, legacy));
		}
		catch(Exception error) {
			throw DMLRuntimeException.of(error);
		}
		OOCInstructionUtils.uncoordinatedDataGen(qOut, bulkBytes::get, maxBulkBytes, OOCAccessPattern.UNKNOWN,
			(ignored, active) -> {
				try {
					emitter.set(active);
					reader.readPreparedMatrixAsStream(untracked, readWorkers.get());
					return true;
				}
				catch(Exception error) {
					throw DMLRuntimeException.of(error);
				}
				finally {
					emitter.set(null);
				}
			}, () -> {
			}, rowBytes::get, value -> value.getIndexes().getColumnIndex() == 1,
			value -> value.getIndexes().getColumnIndex() == columnBlocks.get(),
			value -> value.getIndexes().getRowIndex() < blockRows.get(), getContext());
	}

	private void configureParallelRead(int rows, int cols, DataCharacteristics input, DataCharacteristics output,
		long maxBulkBytes, AtomicLong bulkBytes, AtomicLong rowBytes, AtomicLong blockRows, AtomicLong columnBlocks,
		AtomicInteger readWorkers, AtomicBoolean legacy) {
		input.setRows(rows).setCols(cols);
		output.set(rows, cols, blen, input.getNonZeros());
		long bytes = estimateDenseBlockRowBytes(rows, cols);
		long rowTiles = (rows + blen - 1) / blen;
		int configuredWorkers = readWorkers.get();
		int workers = configuredWorkers;
		if(rowTiles > maxBulkBytes / bytes)
			workers = (int) Math.min(configuredWorkers, maxBulkBytes / bytes / 2);
		if(workers > 0 && workers < configuredWorkers)
			System.out.println("[WARN] Reducing parallel CSV read workers from " + configuredWorkers + " to " + workers
				+ " to keep the OOC reader working allocation within " + maxBulkBytes + " bytes.");
		else if(workers == 0) {
			System.out.println(
				"[WARN] Falling back to the legacy parallel CSV reader because one OOC read worker requires more than "
					+ maxBulkBytes + " bytes.");
			legacy.set(true);
			workers = configuredWorkers;
			bytes = 0;
		}
		long workingRows = Math.min(rowTiles, 2L * workers);
		bulkBytes.set(bytes > 0 ? workingRows * bytes : Math.min(CSV_SCAN_BULK_BYTES, maxBulkBytes));
		rowBytes.set(bytes);
		blockRows.set(rowTiles);
		columnBlocks.set((cols + blen - 1) / blen);
		readWorkers.set(workers);
	}

	private long estimateDenseBlockRowBytes(long numRows, long numCols) {
		long rows = Math.min(numRows, blen);
		long bytes = 0;
		for(long col = 0; col < numCols; col += blen) {
			long blockBytes = MatrixBlock.estimateSizeDenseInMemory(rows, Math.min(blen, numCols - col));
			if(bytes > Long.MAX_VALUE - blockBytes)
				return -1;
			bytes += blockBytes;
		}
		return bytes;
	}
}
