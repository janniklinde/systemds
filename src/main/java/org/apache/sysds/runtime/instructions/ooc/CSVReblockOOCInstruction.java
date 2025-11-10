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

	private void readCSVBlock(OOCStream<IndexedMatrixValue> qOut, MatrixObject min, DataCharacteristics mcOut,
		FileFormatPropertiesCSV props) {
		final Path path = new Path(min.getFileName());
		final JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());

		try {
			final FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
			MatrixReader.checkValidInputFile(fs, path);

			try(FSDataInputStream rawIn = fs.open(path); BufferedSeekableInput in = new BufferedSeekableInput(rawIn)) {
				final int delim = getDelimiter(props);
				final boolean hasCRLF = true;
				final ColumnInfo columnInfo = detectColumnInfo(in, delim, hasCRLF, props.hasHeader());
				final int ncols = columnInfo.ncols;
				if(ncols <= 0) {
					qOut.closeInput();
					return;
				}
				in.seek(columnInfo.dataStart);

				final int segLenMax = Math.min(MAX_BLOCKS_IN_CACHE * blen, ncols);
				final int segLen = Math.min(segLenMax, ncols);
				final boolean fill = props.isFill();
				final double fillValue = props.getFillValue();
				final Set<String> naStrings = props.getNAStrings();

				final List<Long> segStartList = new ArrayList<>();
				MatrixBlock[] firstBlocks = null;
				DenseBlock[] firstDense = null;
				int rowsInBand = 0;
				int brow = 0;

				// pseudocode: fused prescan + seg0 fill
				while(true) {
					int next = peek(in);
					if(next == -1)
						break;

					if(rowsInBand == 0) {
						firstBlocks = allocateBlocks(blen, 0, segLen, ncols);
						firstDense = extractDense(firstBlocks);
					}

					final long nextSegPos = fillFirstSegmentRow(in, firstDense, rowsInBand, segLen, ncols, delim, fill,
						fillValue, naStrings, hasCRLF);
					segStartList.add(nextSegPos);

					rowsInBand++;
					if(rowsInBand == blen) {
						emitBlocks(qOut, firstBlocks, rowsInBand, brow, 0);
						rowsInBand = 0;
						firstBlocks = null;
						firstDense = null;
						brow++;
					}
				}

				if(rowsInBand > 0 && firstBlocks != null) {
					emitBlocks(qOut, firstBlocks, rowsInBand, brow, 0);
					brow++;
				}

				final long[] segStarts = toArray(segStartList);
				final int nrows = segStarts.length;
				if(nrows == 0) {
					qOut.closeInput();
					return;
				}

				final int nBrow = (nrows + blen - 1) / blen;
				// pseudocode: process segments c0 ≥ segLen
				for(int c0 = segLen; c0 < ncols; c0 += segLen) {
					final int seg = Math.min(segLen, ncols - c0);
					final int firstBlockCol = c0 / blen;

					for(int rb = 0; rb < nBrow; rb++) {
						final int r0 = rb * blen;
						final int rows = Math.min(blen, nrows - r0);
						final MatrixBlock[] blocks = allocateBlocks(rows, firstBlockCol, seg, ncols);
						final DenseBlock[] dense = extractDense(blocks);

						for(int i = 0; i < rows; i++) {
							in.seek(segStarts[r0 + i]);
							int col = c0;
							int read = 0;
							while(read < seg) {
								final Token token = parseToken(in, delim, fill, fillValue, naStrings);
								final int bci = (col / blen) - firstBlockCol;
								final int within = col % blen;
								dense[bci].set(i, within, token.value);
								if(token.term == delim)
									in.read();
								read++;
								col++;
							}
							segStarts[r0 + i] = in.getPos();
							skipRestOfLineFast(in, hasCRLF);
						}

						emitBlocks(qOut, blocks, rows, rb, firstBlockCol);
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

	private static ColumnInfo detectColumnInfo(SeekableInput in, int delim, boolean hasCRLF, boolean hasHeader)
		throws IOException {
		in.seek(0);
		if(hasHeader)
			skipRestOfLineFast(in, hasCRLF);

		final long dataStart = in.getPos();
		int ch;
		int ncols = 0;
		boolean seenToken = false;
		while((ch = in.read()) != -1) {
			if(ch == delim) {
				ncols++;
				seenToken = false;
			}
			else if(ch == '\n') {
				if(seenToken || ncols > 0)
					ncols++;
				break;
			}
			else if(ch == '\r') {
				if(hasCRLF && consumeLF(in))
					ch = '\n';
				if(seenToken || ncols > 0)
					ncols++;
				break;
			}
			else {
				seenToken = true;
			}
		}

		if(ch == -1 && (seenToken || ncols > 0))
			ncols++;

		in.seek(dataStart);
		return new ColumnInfo(ncols, dataStart);
	}

	private long fillFirstSegmentRow(SeekableInput in, DenseBlock[] denseBlocks, int rowOffset, int segLen, int ncols,
		int delim, boolean fill, double fillValue, Set<String> naStrings, boolean hasCRLF) throws IOException {
		int col = 0;
		long nextSegPos = -1;

		while(col < segLen) {
			final Token token = parseToken(in, delim, fill, fillValue, naStrings);
			final int bci = col / blen;
			final int within = col % blen;
			denseBlocks[bci].set(rowOffset, within, token.value);
			col++;

			if(token.term == delim) {
				in.read();
				if(col == segLen)
					nextSegPos = in.getPos();
				continue;
			}

			nextSegPos = in.getPos();
			break;
		}

		if(nextSegPos < 0)
			nextSegPos = in.getPos();

		skipRestOfLineFast(in, hasCRLF);
		return nextSegPos;
	}

	private MatrixBlock[] allocateBlocks(int rows, int firstBlockCol, int segLen, int ncols) {
		final int c0 = firstBlockCol * blen;
		final int lastBlockCol = (c0 + segLen - 1) / blen;
		final int numBlocks = lastBlockCol - firstBlockCol + 1;
		final MatrixBlock[] blocks = new MatrixBlock[numBlocks];

		for(int bci = 0; bci < numBlocks; bci++) {
			final int bcol = firstBlockCol + bci;
			final int cStart = bcol * blen;
			final int cEnd = Math.min(ncols, cStart + blen);
			final MatrixBlock block = new MatrixBlock(rows, cEnd - cStart, false);
			block.allocateDenseBlock();
			blocks[bci] = block;
		}

		return blocks;
	}

	private DenseBlock[] extractDense(MatrixBlock[] blocks) {
		final DenseBlock[] dense = new DenseBlock[blocks.length];
		for(int i = 0; i < blocks.length; i++) {
			final DenseBlock db = blocks[i].getDenseBlock();
			if(db == null)
				throw new DMLRuntimeException("Failed to allocate dense block");
			dense[i] = db;
		}
		return dense;
	}

	private void emitBlocks(OOCStream<IndexedMatrixValue> qOut, MatrixBlock[] blocks, int rowsInBand, int brow,
		int firstBlockCol) {
		for(int bci = 0; bci < blocks.length; bci++) {
			MatrixBlock block = blocks[bci];
			if(block.getNumRows() != rowsInBand) {
				block = block.slice(0, rowsInBand - 1, 0, block.getNumColumns() - 1);
			}
			block.recomputeNonZeros();
			block.examSparsity();
			final MatrixIndexes idx = new MatrixIndexes(brow + 1, firstBlockCol + bci + 1);
			qOut.enqueue(new IndexedMatrixValue(idx, block));
		}
	}

	private static Token parseToken(SeekableInput in, int delim, boolean fill, double fillValue, Set<String> naStrings)
		throws IOException {
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

		final double value;
		if(len == 0) {
			if(fill)
				value = fillValue;
			else
				throw new DMLRuntimeException("Empty value in CSV input");
		}
		else {
			value = UtilFunctions.parseToDouble(buf.toString(), naStrings);
		}

		final int term = (ch == -1) ? -1 : ch;
		return new Token(value, term);
	}

	private static void skipRestOfLineFast(SeekableInput in, boolean hasCRLF) throws IOException {
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

	private static boolean consumeLF(SeekableInput in) throws IOException {
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

	private static int peek(SeekableInput in) throws IOException {
		final long pos = in.getPos();
		final int ch = in.read();
		if(ch != -1)
			in.seek(pos);
		return ch;
	}

	private interface SeekableInput extends AutoCloseable {
		int read() throws IOException;

		void seek(long pos) throws IOException;

		long getPos();

		@Override
		void close() throws IOException;
	}

	private static final class BufferedSeekableInput implements SeekableInput {
		private static final int BUF_SIZE = 64 * 1024;

		private final FSDataInputStream in;
		private final byte[] buf = new byte[BUF_SIZE];
		private long bufStart = 0;
		private int bufLen = 0;
		private int bufPos = 0;

		private BufferedSeekableInput(FSDataInputStream in) {
			this.in = in;
		}

		@Override
		public int read() throws IOException {
			if(bufPos >= bufLen) {
				if(!fill())
					return -1;
			}
			return buf[bufPos++] & 0xFF;
		}

		private boolean fill() throws IOException {
			bufStart = in.getPos();
			bufLen = in.read(buf, 0, BUF_SIZE);
			if(bufLen <= 0) {
				bufLen = 0;
				bufPos = 0;
				return false;
			}
			bufPos = 0;
			return true;
		}

		@Override
		public void seek(long pos) throws IOException {
			final long bufEnd = bufStart + bufLen;
			if(pos >= bufStart && pos < bufEnd) {
				bufPos = (int) (pos - bufStart);
			}
			else {
				in.seek(pos);
				bufStart = pos;
				bufLen = 0;
				bufPos = 0;
			}
		}

		@Override
		public long getPos() {
			return bufStart + bufPos;
		}

		@Override
		public void close() throws IOException {
			in.close();
		}
	}

	private static final class ColumnInfo {
		private final int ncols;
		private final long dataStart;

		private ColumnInfo(int ncols, long dataStart) {
			this.ncols = ncols;
			this.dataStart = dataStart;
		}
	}

	private static final class Token {
		private final double value;
		private final int term;

		private Token(double value, int term) {
			this.value = value;
			this.term = term;
		}
	}
}
