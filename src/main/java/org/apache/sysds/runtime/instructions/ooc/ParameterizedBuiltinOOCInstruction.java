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
import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types;
import org.apache.sysds.lops.Lop;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.ParameterizedBuiltin;
import org.apache.sysds.runtime.functionobjects.ValueFunction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.parser.Statement;
import org.apache.sysds.runtime.instructions.cp.BooleanObject;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.ParameterizedBuiltinCPInstruction;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.cp.ScalarObjectFactory;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.LibMatrixReorg;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.SimpleOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.util.OOCGroupedAggregate;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCRemoveEmptyMap;
import org.apache.sysds.runtime.util.UtilFunctions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public class ParameterizedBuiltinOOCInstruction extends ComputationOOCInstruction {
	/** Guard for the removeEmpty position map, which holds one bit per margin entry. 64M entries are 8 MiB. */
	private static final long MAX_SELECT_ENTRIES = 64L * 1024 * 1024;

	protected final LinkedHashMap<String, String> params;

	protected ParameterizedBuiltinOOCInstruction(Operator op, LinkedHashMap<String, String> paramsMap, CPOperand out,
		String opcode, String istr) {
		super(OOCInstruction.OOCType.ParameterizedBuiltin, op, null, null, out, opcode, istr);
		params = paramsMap;
	}

	public static ParameterizedBuiltinOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		// first part is always the opcode
		String opcode = parts[0];
		// last part is always the output
		CPOperand out = new CPOperand(parts[parts.length - 1]);

		// process remaining parts and build a hash map
		LinkedHashMap<String, String> paramsMap = ParameterizedBuiltinCPInstruction.constructParameterMap(parts);

		// determine the appropriate value function
		ValueFunction func = null;

		if(opcode.equalsIgnoreCase(Opcodes.REPLACE.toString())) {
			func = ParameterizedBuiltin.getParameterizedBuiltinFnObject(opcode);
			return new ParameterizedBuiltinOOCInstruction(new SimpleOperator(func), paramsMap, out, opcode, str);
		}
		else if(opcode.equalsIgnoreCase(Opcodes.CONTAINS.toString())) {
			return new ParameterizedBuiltinOOCInstruction(null, paramsMap, out, opcode, str);
		}
		else if(opcode.equalsIgnoreCase(Opcodes.REXPAND.toString())) {
			func = ParameterizedBuiltin.getParameterizedBuiltinFnObject(opcode);
			return new ParameterizedBuiltinOOCInstruction(new SimpleOperator(func), paramsMap, out, opcode, str);
		}
		else if(opcode.equalsIgnoreCase(Opcodes.RMEMPTY.toString())) {
			return new ParameterizedBuiltinOOCInstruction(null, paramsMap, out, opcode, str);
		}
		else if(opcode.equalsIgnoreCase(Opcodes.GROUPEDAGG.toString())) {
			String fn = paramsMap.get(Statement.GAGG_FN);
			if(fn == null)
				throw new DMLRuntimeException("Function parameter is missing in groupedAggregate.");
			if(fn.equalsIgnoreCase("centralmoment") && paramsMap.get(Statement.GAGG_FN_CM_ORDER) == null)
				throw new DMLRuntimeException(
					"Mandatory \"order\" must be specified when fn=\"centralmoment\" in groupedAggregate.");
			Operator op = InstructionUtils.parseGroupedAggOperator(fn, paramsMap.get(Statement.GAGG_FN_CM_ORDER));
			return new ParameterizedBuiltinOOCInstruction(op, paramsMap, out, opcode, str);
		}
		else
			throw new NotImplementedException(); // TODO
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		if(instOpcode.equalsIgnoreCase(Opcodes.REPLACE.toString())) {
			if(ec.isFrameObject(params.get("target"))){
				throw new NotImplementedException();
			} else{
				MatrixObject targetObj = ec.getMatrixObject(params.get("target"));
				OOCStream<IndexedMatrixValue> qOut = createWritableStream();

				double pattern = Double.parseDouble(params.get("pattern"));
				double replacement = Double.parseDouble(params.get("replacement"));

				OOCInstructionUtils.equiMapBlock(targetObj.getStreamable(), qOut,
					block -> block.replaceOperations(new MatrixBlock(), pattern, replacement), getContext());

				OOCInstructionUtils.propagateDims(ec, output, targetObj.getNumRows(), targetObj.getNumColumns(),
					targetObj.getBlocksize(), -1);
				ec.getMatrixObject(output).setStreamHandle(qOut);
			}
		}
		else if(instOpcode.equalsIgnoreCase(Opcodes.CONTAINS.toString())) {
			MatrixObject targetObj = ec.getMatrixObject(params.get("target"));
			Data pattern = ec.getVariable(params.get("pattern"));
			if(pattern == null)
				pattern = ScalarObjectFactory.createScalarObject(Types.ValueType.FP64, params.get("pattern"));
			if(!pattern.getDataType().isScalar())
				throw new NotImplementedException();

			double value = ((ScalarObject) pattern).getDoubleValue();
			OOCStream<Boolean> result = createWritableStream(4, 4, 4);
			OOCInstructionUtils.reduce(targetObj.getStreamable(), result,
				block -> ((MatrixBlock) block.getValue()).containsValue(value), (left, right) -> left || right,
				ignored -> 1, getContext());
			result.start();
			try(OOCStream.QueueCallback<Boolean> callback = result.dequeueCB()) {
				if(callback == null)
					throw new IllegalStateException("Contains cannot reduce an empty OOC stream");
				ec.setScalarOutput(output.getName(), new BooleanObject(callback.get()));
			}
			try(OOCStream.QueueCallback<Boolean> callback = result.dequeueCB()) {
				if(callback != null)
					throw new IllegalStateException("Contains produced multiple results");
			}
		}
		else if(instOpcode.equalsIgnoreCase(Opcodes.REXPAND.toString())) {
			MatrixObject targetObj = ec.getMatrixObject(params.get("target"));
			String maxValName = params.get("max");
			long lmaxVal = maxValName.startsWith(Lop.SCALAR_VAR_NAME_PREFIX) ? ec
				.getScalarInput(maxValName, Types.ValueType.FP64, false)
				.getLongValue() : UtilFunctions.toLong(Double.parseDouble(maxValName));
			boolean dirRows = params.get("dir").equals("rows");
			boolean cast = Boolean.parseBoolean(params.get("cast"));
			boolean ignore = Boolean.parseBoolean(params.get("ignore"));
			long blen = targetObj.getBlocksize();
			MatrixObject outputObj = ec.getMatrixObject(output);
			outputObj.getDataCharacteristics().set(dirRows ? lmaxVal : targetObj.getNumRows(),
				dirRows ? targetObj.getNumRows() : lmaxVal, targetObj.getBlocksize(), -1);
			OOCStream<IndexedMatrixValue> result = createWritableStream(outputObj);
			outputObj.setStreamHandle(result);

			long tileLength = Math.min(blen, targetObj.getNumRows());
			long expandedBytes = MatrixBlock.estimateSizeInMemory(dirRows ? lmaxVal : tileLength,
				dirRows ? tileLength : lmaxVal, tileLength);
			OOCInstructionUtils.flatMap(targetObj.getStreamable(), result, value -> {
				ArrayList<IndexedMatrixValue> blocks = new ArrayList<>();
				LibMatrixReorg.rexpand(value, lmaxVal, dirRows, cast, ignore, blen, blocks);
				return blocks;
			}, OOCUtils::memoryCharge, 3 * expandedBytes, getContext());
		}
		else if(instOpcode.equalsIgnoreCase(Opcodes.RMEMPTY.toString())) {
			processRemoveEmpty(ec);
		}
		else if(instOpcode.equalsIgnoreCase(Opcodes.GROUPEDAGG.toString())) {
			processGroupedAggregate(ec);
		}
		else
			throw new NotImplementedException();
	}

	/**
	 * Out-of-core {@code removeEmpty} for an explicit select vector. The select vector is a vector along the compacted
	 * margin, so it is collected into a position map whose size follows a dimension rather than the data volume; the
	 * target itself stays streamed and is compacted by a repartition.
	 */
	private void processRemoveEmpty(ExecutionContext ec) {
		if(ec.isFrameObject(params.get("target")))
			throw new NotImplementedException();
		String selectName = params.get("select");
		if(selectName == null)
			throw new DMLRuntimeException(
				"Planner-backed OOC removeEmpty requires an explicit select vector, the data-dependent variant is CP");

		MatrixObject target = ec.getMatrixObject(params.get("target"));
		MatrixObject select = ec.getMatrixObject(selectName);
		boolean rows = "rows".equals(params.get("margin"));
		if(!rows && !"cols".equals(params.get("margin")))
			throw new DMLRuntimeException("Invalid margin for removeEmpty: " + params.get("margin"));
		boolean emptyReturn = Boolean.parseBoolean(params.get("empty.return").toLowerCase());

		final int blen = target.getBlocksize();
		if(blen <= 0)
			throw new DMLRuntimeException("Planner-backed OOC removeEmpty requires a positive block size");
		final long otherLength = rows ? target.getNumColumns() : target.getNumRows();
		if(otherLength < 0)
			throw new DMLRuntimeException("Planner-backed OOC removeEmpty requires a known " + (rows ? "column" : "row")
				+ " count, got " + target.getNumRows() + "x" + target.getNumColumns());
		// the select vector holds one entry per margin position, so it carries the margin length even when neither it
		// nor the target published one
		OOCRemoveEmptyMap map = collectSelect(select, rows ? target.getNumRows() : target.getNumColumns(), blen);
		long kept = map.getKeptCount();

		MatrixObject out = ec.getMatrixObject(output);
		if(kept == 0) {
			// CP returns a single zero row/column for a fully removed matrix, and an empty one otherwise
			long emptyMargin = emptyReturn ? 1 : 0;
			long rlen = rows ? emptyMargin : otherLength;
			long clen = rows ? otherLength : emptyMargin;
			OOCInstructionUtils.propagateDims(ec, output, rlen, clen, blen, 0);
			OOCStream<IndexedMatrixValue> empty = createWritableStream(out);
			out.setStreamHandle(empty);
			target.getStreamable().discardHandle();
			for(long row = 0; row < (rlen + blen - 1) / blen; row++)
				for(long col = 0; col < (clen + blen - 1) / blen; col++)
					empty.enqueue(new IndexedMatrixValue(new MatrixIndexes(row + 1, col + 1), new MatrixBlock(
						(int) Math.min(blen, rlen - row * blen), (int) Math.min(blen, clen - col * blen), true)));
			empty.closeInput();
			return;
		}

		OOCInstructionUtils.propagateDims(ec, output, rows ? kept : otherLength, rows ? otherLength : kept, blen, -1);
		OOCStream<IndexedMatrixValue> qOut = createWritableStream(out);
		addOutStream(qOut);
		out.setStreamHandle(qOut);

		int[] fragments = map.fragmentCounts();
		OOCInstructionUtils.repartition(target.getStreamable(), qOut,
			outputIndex -> fragments[(int) ((rows ? outputIndex.getRowIndex() : outputIndex.getColumnIndex()) - 1)],
			(tile, emit) -> {
				MatrixBlock block = (MatrixBlock) tile.getValue();
				long marginBlock = (rows ? tile.getIndexes().getRowIndex() : tile.getIndexes().getColumnIndex()) - 1;
				long otherBlock = rows ? tile.getIndexes().getColumnIndex() : tile.getIndexes().getRowIndex();
				int marginEntries = rows ? block.getNumRows() : block.getNumColumns();
				int otherEntries = rows ? block.getNumColumns() : block.getNumRows();
				map.forEachRun(marginBlock, marginEntries, (srcOffset, length, outputBlock, dstOffset) -> {
					MatrixIndexes outputIndex = rows ? new MatrixIndexes(outputBlock + 1,
						otherBlock) : new MatrixIndexes(otherBlock, outputBlock + 1);
					if(rows)
						emit.copy(outputIndex, srcOffset, 0, length, otherEntries, dstOffset, 0);
					else
						emit.copy(outputIndex, 0, srcOffset, otherEntries, length, 0, dstOffset);
				});
			}, getContext());
	}

	private void processGroupedAggregate(ExecutionContext ec) {
		MatrixObject target = ec.getMatrixObject(params.get(Statement.GAGG_TARGET));
		MatrixObject groups = ec.getMatrixObject(params.get(Statement.GAGG_GROUPS));
		if(params.get(Statement.GAGG_WEIGHTS) != null)
			throw new DMLRuntimeException("Planner-backed OOC groupedAggregate does not support weights");
		if(target.getNumRows() != groups.getNumRows() || groups.getNumColumns() != 1)
			throw new DMLRuntimeException("Grouped aggregate dimension mismatch between target " + target.getNumRows()
				+ "x" + target.getNumColumns() + " and groups " + groups.getNumRows() + "x" + groups.getNumColumns());

		final int blen = target.getBlocksize();
		if(blen <= 0)
			throw new DMLRuntimeException("Planner-backed OOC groupedAggregate requires a positive block size");
		final int cols = Math.toIntExact(target.getNumColumns());
		final int ngroups = numGroups();
		final Operator operator = _optr;

		DataCharacteristics targetDc = target.getDataCharacteristics();
		int broadcastBlocks = Math.toIntExact(groups.getDataCharacteristics().getNumRowBlocks());
		int usesPerBlock = Math.toIntExact(targetDc.getNumColBlocks());
		OOCStream<IndexedMatrixValue> paired = createWritableStream(new MatrixCharacteristics(-1, -1, blen, -1));
		OOCInstructionUtils.indexedBroadcastMap(target.getStreamable(), groups.getStreamable(), paired,
			tmp -> tmp.getIndexes().getRowIndex(), tmp -> 1, () -> new CountingLiveness(broadcastBlocks, usesPerBlock),
			(tile, groupIds) -> new IndexedMatrixValue(tile.getIndexes(),
				((MatrixBlock) groupIds.getValue()).append((MatrixBlock) tile.getValue(), new MatrixBlock(), true)),
			getContext());

		OOCStream<OOCGroupedAggregate> reduced = createWritableStream(4, 4, 4);
		OOCInstructionUtils.reduce(paired, reduced, value -> {
			MatrixBlock pairedBlock = (MatrixBlock) value.getValue();
			int colOffset = Math.toIntExact((value.getIndexes().getColumnIndex() - 1) * blen);
			MatrixBlock groupIds = pairedBlock.slice(0, pairedBlock.getNumRows() - 1, 0, 0, new MatrixBlock());
			MatrixBlock tile = pairedBlock.slice(0, pairedBlock.getNumRows() - 1, 1, pairedBlock.getNumColumns() - 1,
				new MatrixBlock());
			OOCGroupedAggregate partial = new OOCGroupedAggregate(operator, ngroups, colOffset,
				colOffset + tile.getNumColumns());
			partial.add(groupIds, tile, colOffset);
			return partial;
		}, OOCGroupedAggregate::merge, OOCGroupedAggregate::estimateBytes,
			() -> new OOCGroupedAggregate(operator, ngroups, 0, cols), getContext());

		MatrixObject out = ec.getMatrixObject(output);
		OOCInstructionUtils.propagateDims(ec, output, ngroups, cols, blen, -1);
		OOCStream<IndexedMatrixValue> qOut = createWritableStream(out);
		out.setStreamHandle(qOut);

		reduced.start();
		OOCGroupedAggregate aggregate;
		try(OOCStream.QueueCallback<OOCGroupedAggregate> callback = reduced.dequeueCB()) {
			if(callback == null)
				throw new IllegalStateException("Grouped aggregate cannot reduce an empty OOC stream");
			aggregate = callback.get();
		}
		try(OOCStream.QueueCallback<OOCGroupedAggregate> callback = reduced.dequeueCB()) {
			if(callback != null)
				throw new IllegalStateException("Grouped aggregate produced multiple results");
		}

		for(int rowLow = 0; rowLow < ngroups; rowLow += blen)
			for(int colLow = 0; colLow < cols; colLow += blen) {
				int rowHigh = Math.min(rowLow + blen, ngroups);
				int colHigh = Math.min(colLow + blen, cols);
				qOut.enqueue(new IndexedMatrixValue(new MatrixIndexes(rowLow / blen + 1L, colLow / blen + 1L),
					aggregate.toMatrixBlock(rowLow, rowHigh, colLow, colHigh)));
			}
		qOut.closeInput();
	}

	private int numGroups() {
		String declared = params.get(Statement.GAGG_NUM_GROUPS);
		if(declared == null)
			throw new DMLRuntimeException(
				"Planner-backed OOC groupedAggregate requires ngroups, the data-dependent variant is CP");
		double value = declared.startsWith(Lop.SCALAR_VAR_NAME_PREFIX) ? Double.NaN : Double.parseDouble(declared);
		if(Double.isNaN(value) || value < 1)
			throw new DMLRuntimeException("Invalid ngroups for OOC groupedAggregate: " + declared);
		return (int) value;
	}

	private OOCRemoveEmptyMap collectSelect(MatrixObject select, long marginLength, int blen) {
		OOCRemoveEmptyMap.Builder builder = OOCRemoveEmptyMap.builder(blen, MAX_SELECT_ENTRIES);
		int selectBlen = select.getBlocksize() > 0 ? select.getBlocksize() : blen;
		OOCStream<IndexedMatrixValue> stream = select.getStreamHandle();
		stream.start();
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = stream.dequeueCB()) != null)
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				IndexedMatrixValue value = current.get();
				MatrixIndexes indexes = value.getIndexes();
				long block = OOCRemoveEmptyMap.Builder.marginBlock(indexes.getRowIndex(), indexes.getColumnIndex());
				builder.add(block * selectBlen, (MatrixBlock) value.getValue());
			}
		return builder.build(marginLength);
	}
}
