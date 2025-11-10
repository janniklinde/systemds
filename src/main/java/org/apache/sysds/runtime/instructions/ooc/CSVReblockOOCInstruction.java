package org.apache.sysds.runtime.instructions.ooc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
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

			final List<Path> files = collectInputFiles(fs, path);

			if(files.isEmpty()) {
				qOut.closeInput();
				return;
			}

			if (props.getDelim().length() != 1)
				throw new DMLRuntimeException("Out-of-core CSV reader only supports single char delimiters");

			final int delim = props.getDelim().charAt(0);
			int ncols = -1;
			int segLenMax = -1;
			int segLen = -1;
			final boolean fill = props.isFill();
			final double fillValue = props.getFillValue();
			final Set<String> naStrings = props.getNAStrings();

			final List<RowSegmentStart> segStartList = new ArrayList<>();
			MatrixBlock[] firstBlocks = null;
			DenseBlock[] firstDense = null;
			int rowsInBand = 0;
			int brow = 0;
			boolean headerPending = props.hasHeader();
			int fileIdx = 0;

			// First pass to determine total number of rows while emitting first MAX_BLOCKS_IN_CACHE column blocks per row
			for(Path file : files) {
				try(FSDataInputStream rawIn = fs.open(file); BufferedSeekableInput in = new BufferedSeekableInput(rawIn)) {
					if (ncols == -1) {
						// Init ncol info
						ncols = detectNumColumns(in, delim, props.hasHeader());
						segLenMax = Math.min(MAX_BLOCKS_IN_CACHE * blen, ncols);
						segLen = Math.min(segLenMax, ncols);
						in.seek(0); // Reset stream
					}

					if(headerPending) {
						skipRestOfLineFast(in);
						headerPending = false;
					}

					while(true) {
						int next = peek(in);
						if(next == -1)
							break;

						if(rowsInBand == 0) {
							firstBlocks = allocateBlocks(blen, 0, segLen, ncols);
							firstDense = extractDense(firstBlocks);
						}

						final long nextSegPos = fillFirstSegmentRow(in, firstDense, rowsInBand, segLen, ncols, delim, fill,
							fillValue, naStrings);
						segStartList.add(new RowSegmentStart(fileIdx, nextSegPos));

						rowsInBand++;
						if(rowsInBand == blen) {
							emitBlocks(qOut, firstBlocks, rowsInBand, brow, 0);
							rowsInBand = 0;
							firstBlocks = null;
							firstDense = null;
							brow++;
						}
					}
				}
				fileIdx++;
			}

			if(rowsInBand > 0)
				emitBlocks(qOut, firstBlocks, rowsInBand, brow, 0);

			final RowSegmentStart[] segStarts = segStartList.toArray(RowSegmentStart[]::new);
			final int nrows = segStarts.length;
			if(nrows == 0) {
				qOut.closeInput();
				return;
			}

			final int nBrow = (nrows + blen - 1) / blen;
			// process segments c0 ≥ segLen
			for(int c0 = segLen; c0 < ncols; c0 += segLen) {
				final int seg = Math.min(segLen, ncols - c0);
				final int firstBlockCol = c0 / blen;

				BufferedSeekableInput activeIn = null;
				int activeFileIdx = -1;
				try {
					for(int rb = 0; rb < nBrow; rb++) {
						final int r0 = rb * blen;
						final int rows = Math.min(blen, nrows - r0);
						final MatrixBlock[] blocks = allocateBlocks(rows, firstBlockCol, seg, ncols);
						final DenseBlock[] dense = extractDense(blocks);

						for(int i = 0; i < rows; i++) {
							final RowSegmentStart start = segStarts[r0 + i];
							if(start.fileIdx != activeFileIdx) {
								if(activeIn != null)
									activeIn.close();
								activeIn = new BufferedSeekableInput(fs.open(files.get(start.fileIdx)));
								activeFileIdx = start.fileIdx;
							}
							activeIn.seek(start.offset);

							int col = c0;
							for(int read = 0; read < seg; read++) {
								final Token token = parseToken(activeIn, delim, fill, fillValue, naStrings);
								final int bci = (col / blen) - firstBlockCol;
								final int within = col % blen;
								dense[bci].set(i, within, token.value);
								if(token.term == delim)
									activeIn.read();
								col++;
							}
							start.offset = activeIn.getPos();
							skipRestOfLineFast(activeIn);
						}

						emitBlocks(qOut, blocks, rows, rb, firstBlockCol);
					}
				}
				finally {
					if(activeIn != null)
						activeIn.close();
				}
			}

			qOut.closeInput();
		}
		catch(IOException ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	private static int detectNumColumns(SeekableInput in, int delim, boolean hasHeader) throws IOException {
		in.seek(0);
		if(hasHeader)
			skipRestOfLineFast(in);

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
				if(consumeLF(in))
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

		return ncols;
	}

	private long fillFirstSegmentRow(SeekableInput in, DenseBlock[] denseBlocks, int rowOffset, int segLen, int ncols,
		int delim, boolean fill, double fillValue, Set<String> naStrings) throws IOException {
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

		skipRestOfLineFast(in);
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

	private static List<Path> collectInputFiles(FileSystem fs, Path path) throws IOException {
		if(!fs.getFileStatus(path).isDirectory())
			return Collections.singletonList(path);

		final List<Path> files = new ArrayList<>();
		for(FileStatus stat : fs.listStatus(path, IOUtilFunctions.hiddenFileFilter))
			files.add(stat.getPath());
		Collections.sort(files);
		return files;
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

		return new Token(value, ch);
	}

	private static void skipRestOfLineFast(SeekableInput in) throws IOException {
		int ch;
		while((ch = in.read()) != -1) {
			if(ch == '\n')
				return;
			if(ch == '\r') {
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

	private static final class RowSegmentStart {
		private final int fileIdx;
		private long offset;

		private RowSegmentStart(int fileIdx, long offset) {
			this.fileIdx = fileIdx;
			this.offset = offset;
		}
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

	private static final class Token {
		private final double value;
		private final int term;

		private Token(double value, int term) {
			this.value = value;
			this.term = term;
		}
	}
}
