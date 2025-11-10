package org.apache.sysds.runtime.instructions.ooc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
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
	private static final int MAX_BLOCKS_IN_CACHE = 10;

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

		OOCStream<IndexedMatrixValue> q = createWritableStream();

		FileFormatProperties props = min.getFileFormatProperties();
		FileFormatPropertiesCSV csvProps = props instanceof FileFormatPropertiesCSV ? (FileFormatPropertiesCSV) props : new FileFormatPropertiesCSV();

		submitOOCTask(() -> readCSVBlock(q, min, mcOut, csvProps), q);

		MatrixObject mout = ec.getMatrixObject(output);
		mout.setStreamHandle(q);
	}

	private void readCSVBlock(OOCStream<IndexedMatrixValue> qOut, MatrixObject min,
		DataCharacteristics mcOut, FileFormatPropertiesCSV props) {
		final Path path = new Path(min.getFileName());
		final JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());

		try {
			final FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
			MatrixReader.checkValidInputFile(fs, path);

			try(FSDataInputStream in = fs.open(path)) {
				final int delim = getDelimiter(props);
				final boolean hasCRLF = true;
				// pseudocode: PRESCAN
				final PrescanResult scan = prescan(in, delim, hasCRLF);
				final long[] lineStarts = scan.lineStarts;
				final int headerOffset = props.hasHeader() ? 1 : 0;
				final int nrows = lineStarts.length - headerOffset;
				final int ncols = scan.ncols;
				if(nrows <= 0 || ncols <= 0) {
					qOut.closeInput();
					return;
				}

				final int segColsMax = Math.min(MAX_BLOCKS_IN_CACHE * blen, ncols);
				final boolean fill = props.isFill();
				final double fillValue = props.getFillValue();
				final Set<String> naStrings = props.getNAStrings();
				final int nBrow = (nrows + blen - 1) / blen;

				// pseudocode: sweep rows by bands
				for(int brow = 0; brow < nBrow; brow++) {
					final int r0 = brow * blen;
					final int rowsInBand = Math.min(blen, nrows - r0);
					final long[] rowPos = new long[rowsInBand];
					for(int i = 0; i < rowsInBand; i++)
						rowPos[i] = lineStarts[headerOffset + r0 + i];

					// pseudocode: sweep columns in segments of ≤ K blocks
					for(int c0 = 0; c0 < ncols; c0 += segColsMax) {
						final int segLen = Math.min(segColsMax, ncols - c0);
						final int firstBlockCol = c0 / blen;
						final int lastBlockCol = (c0 + segLen - 1) / blen;
						final int numBlocks = lastBlockCol - firstBlockCol + 1;

						final MatrixBlock[] blocks = new MatrixBlock[numBlocks];
						final DenseBlock[] denseBlocks = new DenseBlock[numBlocks];
						final int[] blockColStarts = new int[numBlocks];
						for(int bci = 0; bci < numBlocks; bci++) {
							final int bcol = firstBlockCol + bci;
							final int cStart = bcol * blen;
							final int cEnd = Math.min(ncols, cStart + blen);
							blockColStarts[bci] = cStart;
							final MatrixBlock block = new MatrixBlock(rowsInBand, cEnd - cStart, false);
							block.allocateDenseBlock();
							final DenseBlock dense = block.getDenseBlock();
							if(dense == null)
								throw new DMLRuntimeException("Failed to allocate dense block");
							blocks[bci] = block;
							denseBlocks[bci] = dense;
						}

						// pseudocode: fill rows for this segment
						for(int r = 0; r < rowsInBand; r++) {
							in.seek(rowPos[r]);
							int readCount = 0;
							int colIdx = c0;
							while(readCount < segLen) {
								final double v = parseNextDouble(in, delim, fill, fillValue, naStrings);
								final int bci = (colIdx / blen) - firstBlockCol;
								final int within = colIdx - blockColStarts[bci];
								denseBlocks[bci].set(r, within, v);

								final int ch = peek(in);
								if(ch == delim)
									in.read();

								readCount++;
								colIdx++;
							}
							rowPos[r] = in.getPos();
							// pseudocode: SKIP_TO_EOL
							skipToEOL(in, hasCRLF);
						}

						// pseudocode: emit ready blocks
						for(int bci = 0; bci < numBlocks; bci++) {
							final MatrixBlock block = blocks[bci];
							block.recomputeNonZeros();
							block.examSparsity();
							final MatrixIndexes idx = new MatrixIndexes(brow + 1,
								firstBlockCol + bci + 1);
							qOut.enqueue(new IndexedMatrixValue(idx, block));
						}
					}
				}
				qOut.closeInput();
			}
		}
		catch(IOException ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	private static int getDelimiter(FileFormatPropertiesCSV props) {
		final String delim = props.getDelim();
		return (delim == null || delim.isEmpty()) ? ',' : delim.charAt(0);
	}

	private static PrescanResult prescan(FSDataInputStream in, int delim, boolean hasCRLF) throws IOException {
		in.seek(0);
		final List<Long> starts = new ArrayList<>();
		starts.add(0L);
		long pos = 0;
		int ncols = 0;
		boolean countingFirst = true;
		boolean seenToken = false;

		int ch;
		while((ch = in.read()) != -1) {
			pos++;
			if(countingFirst) {
				if(ch == delim) {
					ncols++;
					seenToken = false;
				}
				else if(ch == '\n') {
					if(seenToken || ncols > 0)
						ncols++;
					countingFirst = false;
					starts.add(pos);
					seenToken = false;
				}
				else if(ch == '\r') {
					if(hasCRLF && consumeLF(in))
						pos++;
					if(seenToken || ncols > 0)
						ncols++;
					countingFirst = false;
					starts.add(pos);
					seenToken = false;
				}
				else {
					seenToken = true;
				}
			}
			else {
				if(ch == '\n') {
					starts.add(pos);
				}
				else if(ch == '\r') {
					if(hasCRLF && consumeLF(in))
						pos++;
					starts.add(pos);
				}
			}
		}

		if(countingFirst && (seenToken || ncols > 0))
			ncols++;
		if(!starts.isEmpty() && starts.get(starts.size() - 1) == pos)
			starts.remove(starts.size() - 1);

		return new PrescanResult(toArray(starts), ncols);
	}

	private static boolean consumeLF(FSDataInputStream in) throws IOException {
		final long pos = in.getPos();
		final int next = in.read();
		if(next == '\n')
			return true;
		if(next != -1)
			in.seek(pos);
		return false;
	}

	private static long[] toArray(List<Long> offsets) {
		final long[] ret = new long[offsets.size()];
		for(int i = 0; i < offsets.size(); i++)
			ret[i] = offsets.get(i);
		return ret;
	}

	private static double parseNextDouble(FSDataInputStream in, int delim, boolean fill,
		double fillValue, Set<String> naStrings) throws IOException {
		int ch;
		do {
			ch = in.read();
			if(ch == -1)
				throw new DMLRuntimeException("Unexpected EOF in CSV token");
		}
		while(ch == ' ' || ch == '\t');

		final StringBuilder buf = new StringBuilder();
		while(ch != -1 && ch != delim && ch != '\n' && ch != '\r') {
			buf.append((char) ch);
			ch = in.read();
		}
		if(ch != -1)
			in.seek(in.getPos() - 1);

		int len = buf.length();
		while(len > 0 && (buf.charAt(len - 1) == ' ' || buf.charAt(len - 1) == '\t'))
			buf.setLength(--len);

		if(len == 0) {
			if(fill)
				return fillValue;
			throw new DMLRuntimeException("Empty value in CSV input");
		}

		return UtilFunctions.parseToDouble(buf.toString(), naStrings);
	}

	private static void skipToEOL(FSDataInputStream in, boolean hasCRLF) throws IOException {
		int ch;
		while((ch = in.read()) != -1) {
			if(ch == '\n')
				return;
			if(ch == '\r') {
				if(hasCRLF)
					consumeLF(in);
				return;
			}
		}
	}

	private static int peek(FSDataInputStream in) throws IOException {
		final long pos = in.getPos();
		final int ch = in.read();
		if(ch != -1)
			in.seek(pos);
		return ch;
	}

	private static final class PrescanResult {
		private final long[] lineStarts;
		private final int ncols;

		private PrescanResult(long[] starts, int cols) {
			lineStarts = starts;
			ncols = cols;
		}
	}
}
