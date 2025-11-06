package org.apache.sysds.runtime.instructions.ooc;

import org.apache.sysds.common.Opcodes;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.meta.DataCharacteristics;

public class CSVReblockOOCInstruction extends ComputationOOCInstruction {
	private int blen;

	private CSVReblockOOCInstruction(Operator op, CPOperand in, CPOperand out,
		int blocklength, String opcode, String instr) {
		super(OOCType.Reblock, op, in, out, opcode, instr);
		blen = blocklength;
	}

	public static CSVReblockOOCInstruction parseInstruction(String str) {
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		if(!opcode.equals(Opcodes.CSVRBLK.toString()))
			throw new DMLRuntimeException("Incorrect opcode for CSVReblockOOCInstruction:" + opcode);

		CPOperand in = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		int blen=Integer.parseInt(parts[3]);
		return new CSVReblockOOCInstruction(null, in, out, blen, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		//set the output characteristics
		MatrixObject min = ec.getMatrixObject(input1);
		DataCharacteristics mc = ec.getDataCharacteristics(input1.getName());
		DataCharacteristics mcOut = ec.getDataCharacteristics(output.getName());
		mcOut.set(mc.getRows(), mc.getCols(), blen, mc.getNonZeros());

		// Rows and cols may be -1 if unknown dimensions
		long rows = mc.getRows();
		long cols = mc.getCols();

		//get the source format from the meta data
		//MetaDataFormat iimd = (MetaDataFormat) min.getMetaData();
		//TODO support other formats than binary

		//create queue, spawn thread for asynchronous reading, and return
		OOCStream<IndexedMatrixValue> q = createWritableStream();

		// TODO

		MatrixObject mout = ec.getMatrixObject(output);
		mout.setStreamHandle(q);
	}

	private void readCSVBlock(OOCStream<IndexedMatrixValue> q, String fname) {

	}
}
