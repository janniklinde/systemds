package org.apache.sysds.runtime.instructions.ooc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sysds.common.Opcodes;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.io.FileFormatProperties;
import org.apache.sysds.runtime.io.FileFormatPropertiesCSV;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.io.MatrixReader;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.util.UtilFunctions;

public class CSVReblockOOCInstruction extends ComputationOOCInstruction {
	private final int blen;

	private CSVReblockOOCInstruction(Operator op, CPOperand in, CPOperand out,
		int blocklength, String opcode, String instr) {
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

		OOCStream<IndexedMatrixValue> q = createWritableStream();

		FileFormatProperties props = min.getFileFormatProperties();
		FileFormatPropertiesCSV csvProps = props instanceof FileFormatPropertiesCSV ?
			(FileFormatPropertiesCSV) props : new FileFormatPropertiesCSV();

		submitOOCTask(() -> readCSVBlock(q, min, mcOut, csvProps), q);

		MatrixObject mout = ec.getMatrixObject(output);
		mout.setStreamHandle(q);
	}

	private void readCSVBlock(OOCStream<IndexedMatrixValue> q, MatrixObject min,
		DataCharacteristics mcOut, FileFormatPropertiesCSV props) {
		try {
			JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
			Path path = new Path(min.getFileName());
			FileSystem fs = IOUtilFunctions.getFileSystem(path, job);

			MatrixReader.checkValidInputFile(fs, path);

			List<Path> files = collectCSVPaths(fs, path);

			boolean hasHeader = props.hasHeader();
			String delim = props.getDelim();
			boolean fill = props.isFill();
			double fillValue = props.getFillValue();
			HashSet<String> naStrings = props.getNAStrings();

			long knownCols = mcOut.getCols();
			long expectedRows = mcOut.getRows();
			int numCols = (knownCols > 0 && knownCols <= Integer.MAX_VALUE) ?
				(int) knownCols : -1;
			double[][] rowBuffer = null;
			int bufferRowCount = 0;
			long globalRowIdx = 0;
			long totalNonZeros = 0;

			for(int fileIdx = 0; fileIdx < files.size(); fileIdx++) {
				Path file = files.get(fileIdx);
				try( BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file))) ) {
					if(fileIdx == 0 && hasHeader)
						br.readLine();

					String value;
					while((value = br.readLine()) != null) {
						String cellStr = value.trim();
						String[] parts = IOUtilFunctions.split(cellStr, delim);
						if(numCols < 0) {
							numCols = parts.length;
							if(numCols <= 0)
								throw new DMLRuntimeException("Unable to determine number of columns for CSV reblock.");
							rowBuffer = new double[blen][numCols];
							mcOut.setCols(numCols);
						}
						else if(rowBuffer == null) {
							rowBuffer = new double[blen][numCols];
						}
						IOUtilFunctions.checkAndRaiseErrorCSVNumColumns(file.toString(), cellStr, parts, numCols);

						boolean emptyValuesFound = false;
						for(int j = 0; j < numCols; j++) {
							String part = parts[j].trim();
							double cellValue;
							if(part.isEmpty()) {
								emptyValuesFound = true;
								cellValue = fillValue;
							}
							else {
								cellValue = UtilFunctions.parseToDouble(part, naStrings);
							}
							rowBuffer[bufferRowCount][j] = cellValue;
						}
						IOUtilFunctions.checkAndRaiseErrorCSVEmptyField(cellStr, fill, emptyValuesFound);

						bufferRowCount++;
						globalRowIdx++;

						if(bufferRowCount == blen) {
							totalNonZeros += flushBuffer(q, rowBuffer, bufferRowCount, numCols, globalRowIdx);
							bufferRowCount = 0;
						}
					}
				}
			}

			if(bufferRowCount > 0)
				totalNonZeros += flushBuffer(q, rowBuffer, bufferRowCount, numCols, globalRowIdx);

			q.closeInput();

			if(numCols > 0)
				mcOut.setCols(numCols);
			if(globalRowIdx > 0) {
				if(expectedRows > 0 && expectedRows != globalRowIdx)
					throw new DMLRuntimeException("Read matrix inconsistent with given meta data: expected "
						+ expectedRows + " rows but found " + globalRowIdx + ".");
				mcOut.setRows(globalRowIdx);
			}
			else if(expectedRows > 0) {
				throw new DMLRuntimeException("Read matrix inconsistent with given meta data: expected "
					+ expectedRows + " rows but found 0.");
			}
			mcOut.setNonZeros(totalNonZeros);
		}
		catch(IOException ex) {
			throw new DMLRuntimeException("Failed to read CSV for out-of-core reblock.", ex);
		}
	}

	private List<Path> collectCSVPaths(FileSystem fs, Path path) throws IOException {
		List<Path> files = new ArrayList<>();
		if(fs.getFileStatus(path).isDirectory()) {
			for(FileStatus stat : fs.listStatus(path, IOUtilFunctions.hiddenFileFilter))
				files.add(stat.getPath());
			Collections.sort(files);
		}
		else {
			files.add(path);
		}
		return files;
	}

	private long flushBuffer(OOCStream<IndexedMatrixValue> q, double[][] rowBuffer,
		int rowsInBuffer, int numCols, long rowsProcessed) {
		if(rowBuffer == null)
			return 0;

		long blockRowIndex = ((rowsProcessed - 1) / blen) + 1;
		int numColBlocks = (numCols + blen - 1) / blen;
		long totalNonZeros = 0;

		for(int cb = 0; cb < numColBlocks; cb++) {
			int colStart = cb * blen;
			int colEnd = Math.min(numCols, colStart + blen);
			int colBlockSize = colEnd - colStart;

			MatrixBlock block = new MatrixBlock(rowsInBuffer, colBlockSize, false);
			block.allocateDenseBlock();
			DenseBlock db = block.getDenseBlock();
			long blockNonZeros = 0;

			for(int r = 0; r < rowsInBuffer; r++) {
				double[] dest = db.values(r);
				int dpos = db.pos(r);
				double[] src = rowBuffer[r];
				for(int c = 0; c < colBlockSize; c++) {
					double v = src[colStart + c];
					dest[dpos + c] = v;
					if(v != 0)
						blockNonZeros++;
				}
			}

			block.setNonZeros(blockNonZeros);
			block.examSparsity();

			MatrixIndexes idx = new MatrixIndexes(blockRowIndex, cb + 1);
			q.enqueue(new IndexedMatrixValue(idx, block));
			totalNonZeros += blockNonZeros;
		}

		return totalNonZeros;
	}
}
