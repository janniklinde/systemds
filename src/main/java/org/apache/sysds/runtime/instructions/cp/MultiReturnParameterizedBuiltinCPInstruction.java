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
import org.apache.sysds.runtime.frame.data.FrameBlock;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.lineage.LineageItemUtils;
import org.apache.sysds.runtime.matrix.data.LibMatrixMult;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.LibMatrixReorg;
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
			final int numInputs = 12;
			final int numOutputs = 4;
			final int minParts = 1 + numInputs + numOutputs;
			if(parts.length < minParts)
				throw new DMLRuntimeException("Invalid number of operands in MultiReturnParameterizedBuiltin instruction: " + opcode);

			int pos = 1;
			ArrayList<CPOperand> inputs = new ArrayList<>(numInputs);
			for(int i = 0; i < numInputs; i++)
				inputs.add(new CPOperand(parts[pos++]));

			outputs.add(new CPOperand(parts[pos++], ValueType.FP64, DataType.MATRIX));
			outputs.add(new CPOperand(parts[pos++], ValueType.FP64, DataType.MATRIX));
			outputs.add(new CPOperand(parts[pos++], ValueType.FP64, DataType.MATRIX));
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
	@SuppressWarnings("unused")
	private void processMessagePassingBipartite(ExecutionContext ec) {
		// Input order (see DMLTranslator): W_v, W_c, b_v, b_c, W_v_vccv, W_c_vccv, W_e_vccv, b_vccv, v, c, e, Ex2
		int k = OptimizerUtils.getTransformNumThreads();
		MatrixBlock W_v = ec.getMatrixInput(_inputs.get(0).getName());
		MatrixBlock W_c = ec.getMatrixInput(_inputs.get(1).getName());
		MatrixBlock b_v = ec.getMatrixInput(_inputs.get(2).getName());
		MatrixBlock b_c = ec.getMatrixInput(_inputs.get(3).getName());
		MatrixBlock W_v_vccv = ec.getMatrixInput(_inputs.get(4).getName());
		MatrixBlock W_c_vccv = ec.getMatrixInput(_inputs.get(5).getName());
		MatrixBlock W_e_vccv = ec.getMatrixInput(_inputs.get(6).getName());
		MatrixBlock b_vccv = ec.getMatrixInput(_inputs.get(7).getName());
		MatrixBlock v = ec.getMatrixInput(_inputs.get(8).getName());
		MatrixBlock c = ec.getMatrixInput(_inputs.get(9).getName());
		MatrixBlock e = ec.getMatrixInput(_inputs.get(10).getName());
		MatrixBlock Ex2 = ec.getMatrixInput(_inputs.get(11).getName());

		MatrixBlock W_v_vccv_t = LibMatrixReorg.transpose(W_v_vccv, k);
		MatrixBlock W_c_vccv_t = LibMatrixReorg.transpose(W_c_vccv, k);
		MatrixBlock W_e_vccv_t = LibMatrixReorg.transpose(W_e_vccv, k);

		MatrixBlock vW = LibMatrixMult.matrixMult(v, W_v_vccv_t, k);
		MatrixBlock cW = LibMatrixMult.matrixMult(c, W_c_vccv_t, k);
		MatrixBlock eW = LibMatrixMult.matrixMult(e, W_e_vccv_t, k);

		final int nV = v.getNumRows();
		final int nC = c.getNumRows();
		final int nE = Ex2.getNumRows();
		final int twoD = W_v_vccv.getNumRows();
		final int d = twoD / 2;

		MatrixBlock vAct = new MatrixBlock(nV, d, false);
		MatrixBlock vOut = new MatrixBlock(nV, d, false);
		MatrixBlock cAct = new MatrixBlock(nC, d, false);
		MatrixBlock cOut = new MatrixBlock(nC, d, false);
		vAct.allocateDenseBlock();
		vOut.allocateDenseBlock();
		cAct.allocateDenseBlock();
		cOut.allocateDenseBlock();

		if(vW.isInSparseFormat()) vW.sparseToDense();
		if(cW.isInSparseFormat()) cW.sparseToDense();
		if(eW.isInSparseFormat()) eW.sparseToDense();
		if(b_vccv.isInSparseFormat()) b_vccv.sparseToDense();
		if(Ex2.isInSparseFormat()) Ex2.sparseToDense();

		double[] vWArr = vW.getDenseBlockValues();
		double[] cWArr = cW.getDenseBlockValues();
		double[] eWArr = eW.getDenseBlockValues();
		double[] bArr = b_vccv.getDenseBlockValues();
		double[] ex2Arr = Ex2.getDenseBlockValues();
		if(bArr == null)
			bArr = new double[twoD];
		double[] vActArr = vAct.getDenseBlockValues();
		double[] vOutArr = vOut.getDenseBlockValues();
		double[] cActArr = cAct.getDenseBlockValues();
		double[] cOutArr = cOut.getDenseBlockValues();

		double[] sumV = new double[nV * d];
		int[] countV = new int[nV];
		double[] sumC = new double[d];
		int countC = 0;
		int currentC = -1;

		for(int ei = 0; ei < nE; ei++) {
			int base = ei * 2;
			int cIdx = (int) ex2Arr[base] - 1;
			int vIdx = (int) ex2Arr[base + 1] - 1;

			if(currentC != -1 && cIdx != currentC) {
				int cOff = currentC * d;
				double inv = 1.0 / countC;
				for(int k2 = 0; k2 < d; k2++) {
					double mean = sumC[k2] * inv;
					cActArr[cOff + k2] = mean;
					cOutArr[cOff + k2] = (mean > 0) ? mean : 0.0;
				}
				Arrays.fill(sumC, 0.0);
				countC = 0;
			}
			if(currentC != cIdx) {
				currentC = cIdx;
			}

			int vOff = vIdx * twoD;
			int cOff2 = cIdx * twoD;
			int eOff = ei * twoD;
			for(int k2 = 0; k2 < d; k2++) {
				double msgC = vWArr[vOff + k2] + cWArr[cOff2 + k2] + eWArr[eOff + k2] + bArr[k2];
				sumC[k2] += msgC;
				double msgV = vWArr[vOff + d + k2] + cWArr[cOff2 + d + k2] + eWArr[eOff + d + k2] + bArr[d + k2];
				sumV[vIdx * d + k2] += msgV;
			}
			countC++;
			countV[vIdx]++;
		}

		if(currentC != -1 && countC > 0) {
			int cOff = currentC * d;
			double inv = 1.0 / countC;
			for(int k2 = 0; k2 < d; k2++) {
				double mean = sumC[k2] * inv;
				cActArr[cOff + k2] = mean;
				cOutArr[cOff + k2] = (mean > 0) ? mean : 0.0;
			}
		}

		for(int vi = 0; vi < nV; vi++) {
			int cnt = countV[vi];
			if(cnt == 0)
				continue;
			double inv = 1.0 / cnt;
			int off = vi * d;
			for(int k2 = 0; k2 < d; k2++) {
				double mean = sumV[off + k2] * inv;
				vActArr[off + k2] = mean;
				vOutArr[off + k2] = (mean > 0) ? mean : 0.0;
			}
		}

		ec.setMatrixOutput(getOutput(0).getName(), vOut);
		ec.setMatrixOutput(getOutput(1).getName(), cOut);
		ec.setMatrixOutput(getOutput(2).getName(), vAct);
		ec.setMatrixOutput(getOutput(3).getName(), cAct);

		ec.releaseMatrixInput(_inputs.get(0).getName());
		ec.releaseMatrixInput(_inputs.get(1).getName());
		ec.releaseMatrixInput(_inputs.get(2).getName());
		ec.releaseMatrixInput(_inputs.get(3).getName());
		ec.releaseMatrixInput(_inputs.get(4).getName());
		ec.releaseMatrixInput(_inputs.get(5).getName());
		ec.releaseMatrixInput(_inputs.get(6).getName());
		ec.releaseMatrixInput(_inputs.get(7).getName());
		ec.releaseMatrixInput(_inputs.get(8).getName());
		ec.releaseMatrixInput(_inputs.get(9).getName());
		ec.releaseMatrixInput(_inputs.get(10).getName());
		ec.releaseMatrixInput(_inputs.get(11).getName());
	}
}
