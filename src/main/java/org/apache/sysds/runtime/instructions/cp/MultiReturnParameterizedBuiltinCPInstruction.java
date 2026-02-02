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

package org.apache.sysds.runtime.instructions.cp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.data.DenseBlockFactory;
import org.apache.sysds.runtime.data.DenseBlockFP32;
import org.apache.sysds.runtime.frame.data.FrameBlock;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.lineage.LineageItemUtils;
import org.apache.sysds.runtime.matrix.data.LibMatrixMult;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.transform.encode.EncoderFactory;
import org.apache.sysds.runtime.transform.encode.MultiColumnEncoder;

public class MultiReturnParameterizedBuiltinCPInstruction extends ComputationCPInstruction {
	protected final ArrayList<CPOperand> _inputs;
	protected final ArrayList<CPOperand> _outputs;
	protected final boolean _metaReturn;
	
	private MultiReturnParameterizedBuiltinCPInstruction(Operator op, ArrayList<CPOperand> inputs,
		boolean metaReturn, ArrayList<CPOperand> outputs, String opcode, String istr) {
		super(CPType.MultiReturnBuiltin, op, inputs.size() > 0 ? inputs.get(0) : null,
			inputs.size() > 1 ? inputs.get(1) : null, outputs.get(0), opcode, istr);
		_inputs = inputs;
		_outputs = outputs;
		_metaReturn = metaReturn;
	}

	public CPOperand getOutput(int i) {
		return _outputs.get(i);
	}

	public List<CPOperand> getOutputs() {
		return _outputs;
	}

	public String[] getOutputNames() {
		return _outputs.stream().map(CPOperand::getName).toArray(String[]::new);
	}

	public static MultiReturnParameterizedBuiltinCPInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		ArrayList<CPOperand> outputs = new ArrayList<>();
		String opcode = parts[0];

		if(opcode.equalsIgnoreCase(Opcodes.TRANSFORMENCODE.toString())) {
			// one input and two outputs
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			ArrayList<CPOperand> inputs = new ArrayList<>();
			inputs.add(in1);
			inputs.add(in2);
			int pos = 3;
			boolean metaReturn = true;
			if( parts.length == 7 ) //no need for meta data
				metaReturn = new CPOperand(parts[pos++]).getLiteral().getBooleanValue();
			outputs.add(new CPOperand(parts[pos], ValueType.FP64, DataType.MATRIX));
			outputs.add(new CPOperand(parts[pos+1], ValueType.STRING, DataType.FRAME));
			return new MultiReturnParameterizedBuiltinCPInstruction(
				null, inputs, metaReturn, outputs, opcode, str);
		}
		else if(opcode.equalsIgnoreCase(Opcodes.MESSAGE_PASSING_BIPARTITE.toString())) {
			final int baseInputs = 12;
			final int extendedInputs = 14;
			final int baseOutputs = 2;
			final int extendedOutputs = 4;
			final int baseParts = 1 + baseInputs + baseOutputs;
			final int extendedParts = 1 + extendedInputs + extendedOutputs;
			final int basePartsWithK = baseParts + 1;
			final int extendedPartsWithK = extendedParts + 1;
			if(parts.length != baseParts && parts.length != extendedParts
				&& parts.length != basePartsWithK && parts.length != extendedPartsWithK)
				throw new DMLRuntimeException("Invalid number of operands in MultiReturnParameterizedBuiltin instruction: " + opcode);

			final int numInputs = (parts.length == baseParts || parts.length == basePartsWithK) ? baseInputs : extendedInputs;
			final int numOutputs = (parts.length == baseParts || parts.length == basePartsWithK) ? baseOutputs : extendedOutputs;
			int pos = 1;
			ArrayList<CPOperand> inputs = new ArrayList<>(numInputs);
			for(int i = 0; i < numInputs; i++)
				inputs.add(new CPOperand(parts[pos++]));

			for(int i = 0; i < numOutputs; i++)
				outputs.add(new CPOperand(parts[pos++], ValueType.FP64, DataType.MATRIX));

			return new MultiReturnParameterizedBuiltinCPInstruction(
				null, inputs, false, outputs, opcode, str);
		}
		else {
			throw new DMLRuntimeException("Invalid opcode in MultiReturnBuiltin instruction: " + opcode);
		}

	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		if(getOpcode().equalsIgnoreCase(Opcodes.MESSAGE_PASSING_BIPARTITE.toString())) {
			processMessagePassingBipartite(ec);
			return;
		}

		// obtain and pin input frame
		FrameBlock fin = ec.getFrameInput(input1.getName());
		String spec = ec.getScalarInput(input2).getStringValue();
		String[] colnames = fin.getColumnNames();
		
		// execute block transform encode
		MultiColumnEncoder encoder = EncoderFactory.createEncoder(spec, colnames, fin.getNumColumns(), null);
		// TODO: Assign #threads in compiler and pass via the instruction string
		int k = OptimizerUtils.getTransformNumThreads();
		MatrixBlock data = encoder.encode(fin, OptimizerUtils.getTransformNumThreads()); // build and apply
		FrameBlock meta = !_metaReturn ? new FrameBlock() :
			encoder.getMetaData(new FrameBlock(fin.getNumColumns(), ValueType.STRING), k);
		meta.setColumnNames(colnames);

		// release input and outputs
		ec.releaseFrameInput(input1.getName());
		ec.setMatrixOutput(getOutput(0).getName(), data);
		ec.setFrameOutput(getOutput(1).getName(), meta);
		
		if(LOG.isDebugEnabled())
			// debug the size of the output metadata.
			LOG.debug("Memory size of metadata: " + meta.getInMemorySize());
	}

	@Override
	public boolean hasSingleLineage() {
		return false;
	}


	@Override
	@SuppressWarnings("unchecked")
	public Pair<String, LineageItem>[] getLineageItems(ExecutionContext ec) {
		LineageItem[] inputLineage = LineageItemUtils.getLineage(ec, _inputs.toArray(new CPOperand[0]));
		final Pair<String, LineageItem>[] ret = new Pair[_outputs.size()];
		for(int i = 0; i < _outputs.size(); i++){
			CPOperand out = _outputs.get(i);
			ret[i] = Pair.of(out.getName(), new LineageItem(getOpcode(), inputLineage));
		}
		return ret; 
	}

	public boolean getMetaReturn() {
		return _metaReturn;
	}

	private static float[] toFloatArray(MatrixBlock mb) {
		DenseBlock db = mb.getDenseBlock();
		if(db instanceof DenseBlockFP32)
			return ((DenseBlockFP32) db).getData();
		double[] vals = mb.getDenseBlockValues();
		if(vals == null)
			return null;
		float[] ret = new float[vals.length];
		for(int i = 0; i < vals.length; i++)
			ret[i] = (float) vals[i];
		return ret;
	}

	private static float[] getFloatDenseBlockValues(MatrixBlock mb) {
		DenseBlock db = mb.getDenseBlock();
		if(db instanceof DenseBlockFP32)
			return ((DenseBlockFP32) db).getData();
		throw new DMLRuntimeException("Expected FP32 dense block for output but found: " +
			(db != null ? db.getClass().getSimpleName() : "null"));
	}

	@SuppressWarnings("unused")
	private void processMessagePassingBipartite(ExecutionContext ec) {
		// Input order (see DMLTranslator):
		// base: W_v, W_c, b_v, b_c, W_v_vccv, W_c_vccv, W_e_vccv, b_vccv, v, c, e, Ex2
		// extended: W_v, W_c, b_v, b_c, W_v_in, W_c_in, W_v_vccv, W_c_vccv, W_e_vccv, b_vccv, v, c, e, Ex2
		int k = OptimizerUtils.getTransformNumThreads();
		//k = 1;
		boolean extended = (_inputs.size() == 14);
		if(_outputs.size() > 2 && !extended)
			throw new DMLRuntimeException("message_passing_bipartite: W_v_in and W_c_in are required when returning v_out/c_out.");
		int pos = 0;
		MatrixBlock W_v = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock W_c = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock b_v = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock b_c = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock W_v_in = null;
		MatrixBlock W_c_in = null;
		if(extended) {
			W_v_in = ec.getMatrixInput(_inputs.get(pos++).getName());
			W_c_in = ec.getMatrixInput(_inputs.get(pos++).getName());
		}
		MatrixBlock W_v_vccv = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock W_c_vccv = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock W_e_vccv = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock b_vccv = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock v = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock c = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock e = ec.getMatrixInput(_inputs.get(pos++).getName());
		MatrixBlock Ex2 = ec.getMatrixInput(_inputs.get(pos++).getName());

		MatrixBlock vW = LibMatrixMult.matrixMultFP32(v, W_v_vccv);
		MatrixBlock cW = LibMatrixMult.matrixMultFP32(c, W_c_vccv);
		MatrixBlock eW = LibMatrixMult.matrixMultFP32(e, W_e_vccv);

		final int nV = v.getNumRows();
		final int nC = c.getNumRows();
		final int nE = Ex2.getNumRows();
		final int ex2Cols = Ex2.getNumColumns();
		if(ex2Cols != 2)
			throw new DMLRuntimeException("message_passing_bipartite: Ex2 must have 2 columns [c_idx, v_idx] but has " + ex2Cols);
		final int twoD = W_v_vccv.getNumColumns();
		final int d = twoD / 2;

		MatrixBlock vAct = new MatrixBlock(nV, d, false);
		MatrixBlock cAct = new MatrixBlock(nC, d, false);
		vAct.setDenseBlock(DenseBlockFactory.createDenseBlock(ValueType.FP32, new int[]{nV, d}));
		cAct.setDenseBlock(DenseBlockFactory.createDenseBlock(ValueType.FP32, new int[]{nC, d}));

		if(vW.isInSparseFormat()) vW.sparseToDense();
		if(cW.isInSparseFormat()) cW.sparseToDense();
		if(eW.isInSparseFormat()) eW.sparseToDense();
		if(b_vccv.isInSparseFormat()) b_vccv.sparseToDense();
		if(Ex2.isInSparseFormat()) Ex2.sparseToDense();

		float[] vWArr = toFloatArray(vW);
		float[] cWArr = toFloatArray(cW);
		float[] eWArr = toFloatArray(eW);
		float[] bArr = toFloatArray(b_vccv);
		float[] ex2Arr = toFloatArray(Ex2);
		if(bArr == null)
			bArr = new float[twoD];
		float[] vActArr = getFloatDenseBlockValues(vAct);
		float[] cActArr = getFloatDenseBlockValues(cAct);

		int[] countV = new int[nV];
		float[] sumC = new float[d];
		int countC = 0;
		int currentC = -1;

		for(int ei = 0; ei < nE; ei++) {
			int base = ei * 2;
			int cIdx = (int) ex2Arr[base] - 1;
			int vIdx = (int) ex2Arr[base + 1] - 1;

			if(currentC != -1 && cIdx != currentC) {
				int cOff = currentC * d;
				float inv = 1.0f / countC;
				for(int k2 = 0; k2 < d; k2++) {
					float mean = sumC[k2] * inv;
					cActArr[cOff + k2] = mean;
				}
				Arrays.fill(sumC, 0.0f);
				countC = 0;
			}
			currentC = cIdx;

			int vOff = vIdx * twoD;
			int cOff2 = cIdx * twoD;
			int eOff = ei * twoD;
			for(int k2 = 0; k2 < d; k2++) {
				float msgC = vWArr[vOff + k2] + cWArr[cOff2 + k2] + eWArr[eOff + k2] + bArr[k2];
				sumC[k2] += msgC;
				float msgV = vWArr[vOff + d + k2] + cWArr[cOff2 + d + k2] + eWArr[eOff + d + k2] + bArr[d + k2];
				vActArr[vIdx * d + k2] += msgV;
			}
			countC++;
			countV[vIdx]++;
		}

		if(currentC != -1 && countC > 0) {
			int cOff = currentC * d;
			float inv = 1.0f / countC;
			for(int k2 = 0; k2 < d; k2++) {
				float mean = sumC[k2] * inv;
				cActArr[cOff + k2] = mean;
			}
		}

		for(int vi = 0; vi < nV; vi++) {
			int cnt = countV[vi];
			if(cnt == 0)
				continue;
			float inv = 1.0f / cnt;
			int off = vi * d;
			for(int k2 = 0; k2 < d; k2++) {
				float mean = vActArr[off + k2] * inv;
				vActArr[off + k2] = mean;
			}
		}

		vAct.setNonZeros((long) nV * d);
		cAct.setNonZeros((long) nC * d);

		ec.setMatrixOutput(getOutput(0).getName(), vAct);
		ec.setMatrixOutput(getOutput(1).getName(), cAct);

		if(_outputs.size() > 2) {
			if(b_v.isInSparseFormat()) b_v.sparseToDense();
			if(b_c.isInSparseFormat()) b_c.sparseToDense();
			MatrixBlock vActRelu = new MatrixBlock(nV, d, false);
			MatrixBlock cActRelu = new MatrixBlock(nC, d, false);
			vActRelu.setDenseBlock(DenseBlockFactory.createDenseBlock(ValueType.FP32, new int[]{nV, d}));
			cActRelu.setDenseBlock(DenseBlockFactory.createDenseBlock(ValueType.FP32, new int[]{nC, d}));
			float[] vActReluArr = getFloatDenseBlockValues(vActRelu);
			float[] cActReluArr = getFloatDenseBlockValues(cActRelu);
			for(int i = 0; i < vActArr.length; i++)
				vActReluArr[i] = (vActArr[i] > 0.0f) ? vActArr[i] : 0.0f;
			for(int i = 0; i < cActArr.length; i++)
				cActReluArr[i] = (cActArr[i] > 0.0f) ? cActArr[i] : 0.0f;

			MatrixBlock vActW = LibMatrixMult.matrixMultFP32(vActRelu, W_v);
			MatrixBlock vInW = LibMatrixMult.matrixMultFP32(v, W_v_in);
			MatrixBlock cActW = LibMatrixMult.matrixMultFP32(cActRelu, W_c);
			MatrixBlock cInW = LibMatrixMult.matrixMultFP32(c, W_c_in);

			if(vActW.isInSparseFormat()) vActW.sparseToDense();
			if(vInW.isInSparseFormat()) vInW.sparseToDense();
			if(cActW.isInSparseFormat()) cActW.sparseToDense();
			if(cInW.isInSparseFormat()) cInW.sparseToDense();

			float[] vActWArr = toFloatArray(vActW);
			float[] vInWArr = toFloatArray(vInW);
			float[] cActWArr = toFloatArray(cActW);
			float[] cInWArr = toFloatArray(cInW);
			float[] bVArr = toFloatArray(b_v);
			float[] bCArr = toFloatArray(b_c);
			if(bVArr == null)
				bVArr = new float[d];
			if(bCArr == null)
				bCArr = new float[d];

			MatrixBlock vOut = new MatrixBlock(nV, d, false);
			MatrixBlock cOut = new MatrixBlock(nC, d, false);
			vOut.setDenseBlock(DenseBlockFactory.createDenseBlock(ValueType.FP32, new int[]{nV, d}));
			cOut.setDenseBlock(DenseBlockFactory.createDenseBlock(ValueType.FP32, new int[]{nC, d}));
			float[] vOutArr = getFloatDenseBlockValues(vOut);
			float[] cOutArr = getFloatDenseBlockValues(cOut);

			for(int i = 0; i < nV; i++) {
				int off = i * d;
				for(int k2 = 0; k2 < d; k2++) {
					float val = vActWArr[off + k2] + vInWArr[off + k2] + bVArr[k2];
					vOutArr[off + k2] = (val > 0.0f) ? val : 0.0f;
				}
			}
			for(int i = 0; i < nC; i++) {
				int off = i * d;
				for(int k2 = 0; k2 < d; k2++) {
					float val = cActWArr[off + k2] + cInWArr[off + k2] + bCArr[k2];
					cOutArr[off + k2] = (val > 0.0f) ? val : 0.0f;
				}
			}

			vOut.setNonZeros((long) nV * d);
			cOut.setNonZeros((long) nC * d);

			ec.setMatrixOutput(getOutput(2).getName(), vOut);
			ec.setMatrixOutput(getOutput(3).getName(), cOut);
		}

		for(CPOperand input : _inputs)
			ec.releaseMatrixInput(input.getName());
	}
}
