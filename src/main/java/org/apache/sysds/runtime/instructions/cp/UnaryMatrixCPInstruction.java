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

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.data.DenseBlockFP32;
import org.apache.sysds.runtime.data.DenseBlockFactory;
import org.apache.sysds.runtime.lineage.LineageItem;
import org.apache.sysds.runtime.matrix.data.LibCommonsMath;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.matrix.operators.UnaryOperator;

public class UnaryMatrixCPInstruction extends UnaryCPInstruction {
	protected UnaryMatrixCPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String instr) {
		super(CPType.Unary, op, in, out, opcode, instr);
	}

	@Override 
	public void processInstruction(ExecutionContext ec) {
		MatrixObject inObj = ec.getMatrixObject(input1);
		MatrixBlock inBlock = inObj.acquireRead();
		MatrixBlock retBlock = null;
		
		if(getOpcode().equals(Opcodes.CAST_AS_FP32.toString())) {
			MatrixBlock denseIn = inBlock;
			if(denseIn.isInSparseFormat()) {
				denseIn = new MatrixBlock(inBlock);
				denseIn.sparseToDense();
			}
			int rlen = denseIn.getNumRows();
			int clen = denseIn.getNumColumns();
			MatrixBlock out = new MatrixBlock(rlen, clen, false);
			out.setDenseBlock(DenseBlockFactory.createDenseBlock(ValueType.FP32, new int[]{rlen, clen}));
			double[] inVals = denseIn.getDenseBlockValues();
			float[] outVals = ((DenseBlockFP32) out.getDenseBlock()).getData();
			if(inVals != null) {
				for(int i = 0; i < inVals.length; i++)
					outVals[i] = (float) inVals[i];
			}
			long nnz = denseIn.getNonZeros();
			out.setNonZeros(nnz >= 0 ? nnz : out.recomputeNonZeros());
			retBlock = out;
			ec.releaseMatrixInput(input1.getName());
		}
		else if(getOpcode().equals(Opcodes.CAST_AS_FP64.toString())) {
			retBlock = new MatrixBlock(inBlock);
			ec.releaseMatrixInput(input1.getName());
		}
		else if(LibCommonsMath.isSupportedUnaryOperation(getOpcode())) {
			retBlock = LibCommonsMath.unaryOperations(inBlock, getOpcode());
			ec.releaseMatrixInput(input1.getName());
		}
		else {
			UnaryOperator u_op = (UnaryOperator) _optr;
			retBlock = inBlock.unaryOperations(u_op, new MatrixBlock());
			ec.releaseMatrixInput(input1.getName());
			// Ensure right dense/sparse output representation (guarded by released input memory)
			if( checkGuardedRepresentationChange(inBlock, retBlock) )
	 			retBlock.examSparsity();
		}
		
		//avoid bufferpool pollution and unnecessary writes by leveraging lineage
		//but only if short lineage (here the lineage of datagen ops)
		LineageItem lin = (!inObj.hasValidLineage() || !inObj.getCacheLineage().isLeaf() ||
			CacheableData.isBelowCachingThreshold(retBlock)) ? null : 
			getCacheLineageItem(inObj.getCacheLineage());
		if (getOpcode().equals(Opcodes.DET.toString())){
			var temp = ScalarObjectFactory.createScalarObject(ValueType.FP64, retBlock.get(0,0));
			ec.setVariable(output.getName(), temp);
		}
		else {
			ec.setMatrixOutputAndLineage(output, retBlock, lin);
		}
	}
	
	public LineageItem getCacheLineageItem(LineageItem input) {
		return new LineageItem(getOpcode(), new LineageItem[]{input});
	}
}
