package org.apache.sysds.runtime.instructions.ooc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.antlr.v4.misc.MutableInt;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.FileStatus;
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

		submitOOCTask(() -> mReadCSVBlock(q, min, mcOut, csvProps), q);

		MatrixObject mout = ec.getMatrixObject(output);
		mout.setStreamHandle(q);
	}

	private static final int BUF = 1 << 15; // 32 KiB (tune)

	private void mReadCSVBlock(OOCStream<IndexedMatrixValue> qOut, MatrixObject min,
		DataCharacteristics mcOut, FileFormatPropertiesCSV props) {

		try {
			JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
			Path path = new Path(min.getFileName());
			FileSystem fs = IOUtilFunctions.getFileSystem(path, job);
			MatrixReader.checkValidInputFile(fs, path);

			final boolean hasHeader = props.hasHeader();
			final char[] delim = props.getDelim().toCharArray(); // single- or multi-char
			final Set<String> na = new HashSet<>();
			if (props.getNAStrings() != null)
				na.addAll(props.getNAStrings());
			final boolean fill = props.isFill();
			final double fillVal = props.getFillValue();

			long nrows = min.getDataCharacteristics().getRows();
			long ncols = min.getDataCharacteristics().getCols();
			if (ncols < 0) ncols = Integer.MAX_VALUE;
			if (nrows < 0) nrows = Integer.MAX_VALUE;

			//double[] row = new double[(int)Math.min(blen, ncols)];
			int blockIdx = 0;
			int colIdx = 0;

			List<MatrixBlock> blocksToEmit = new ArrayList<>(MAX_BLOCKS_IN_CACHE);
			blocksToEmit.add(new MatrixBlock((int)Math.min(blen, nrows), (int)Math.min(blen, ncols), true));
			MatrixBlock dest = blocksToEmit.get(0);
			dest.allocateDenseBlock();
			DenseBlock dense = dest.getDenseBlock();

			try (FSDataInputStream fin = fs.open(path);
				PushbackReader r = new PushbackReader(new InputStreamReader(fin, StandardCharsets.UTF_8), BUF)) {

				// skip header line, if any
				if (hasHeader) {
					// TODO improve by using skip
					MutableInt lc = new MutableInt(0);
					StringBuilder tmp = new StringBuilder(256);
					char[] b = new char[BUF];
					// read until EOL only; ignore field delimiter
					readUntilEOLOrDelim(r, new char[]{'\n'}, tmp, b, lc); // effectively skip one logical line
				}

				final StringBuilder sb = new StringBuilder(1024);
				final char[] b = new char[BUF];
				final MutableInt lineCtr = new MutableInt(0);
				int lastLine = 0;

				while (true) {
					sb.setLength(0);
					String field = readUntilEOLOrDelim(r, delim, sb, b, lineCtr);
					if (field == null) break; // EOF

					boolean eol = (lineCtr.v != lastLine);

					double v;
					if (field.isEmpty()) {
						v = fill ? fillVal : 0.0; // or Double.NaN, depending on semantics
					} else if (na.contains(field)) {
						v = fill ? fillVal : Double.NaN;
					} else {
						v = UtilFunctions.parseToDouble(field, na);
					}

					dense.set(lastLine, colIdx++, v);
					lastLine = lineCtr.v;

					if (eol) {
						colIdx = 0;
						// TODO Might have to flush blocks
					}

					if (dest.getNumColumns() == colIdx) {
						blockIdx++;

						if (blocksToEmit.size() <= blockIdx) {
							blocksToEmit.add(new MatrixBlock((int) Math.min(blen, nrows - blockIdx * blen),
								(int) Math.min(blen, ncols - blockIdx * blen), false));
							dest = blocksToEmit.get(blockIdx).allocateDenseBlock();

						}

						dest = blocksToEmit.get(blockIdx);
						dense = dest.getDenseBlock();
					}
				}
			}
		}
		catch (IOException e) {
			throw new DMLRuntimeException(e);
		}
	}

	static String readUntilEOLOrDelim(PushbackReader r, char[] delim,
		StringBuilder sb, char[] buf, MutableInt lineCtr) throws IOException {

		final int dlen = delim.length;
		int match = 0, n;

		while ((n = r.read(buf)) != -1) {
			int segStart = 0; // where the next append slice begins
			for (int i = 0; i < n; i++) {
				char c = buf[i];

				// newline detection (\n or \r\n)
				if (c == '\n' || c == '\r') {
					if (c == '\r' && i + 1 < n && buf[i + 1] == '\n') i++; // consume \r\n
					// append segment up to newline start minus any partial match
					int end = i - match; if (end > segStart) sb.append(buf, segStart, end - segStart);
					if (i + 1 < n) r.unread(buf, i + 1, n - i - 1); // push back after newline
					lineCtr.v++;
					return sb.toString();
				}

				// delimiter detection (multi-char, streaming)
				if (c == delim[match]) {
					match++;
					if (match == dlen) {
						int end = i + 1 - dlen; // end index of data before delim
						if (end >= segStart) sb.append(buf, segStart, end - segStart);
						if (i + 1 < n) r.unread(buf, i + 1, n - i - 1);
						return sb.toString();
					}
				} else {
					if (match > 0) {
						// rollback partial match: append the matched prefix
						// but do not re-scan it: we just dump it into output
						sb.append(delim, 0, match);
						match = 0;
					}
					// continue accumulating; delay append until we hit delim/EOL or buffer end
				}
			}
			// buffer ended without a terminator: append chunk minus any trailing partial match
			int end = n - match;
			if (end > 0) sb.append(buf, 0, end);
			// leave 'match' as-is to allow cross-buffer matches
		}
		// EOF: append any leftover (partial match belongs to data)
		if (match > 0) sb.append(delim, 0, match);
		return sb.length() == 0 ? null : sb.toString();
	}


	private void readCSVBlock(OOCStream<IndexedMatrixValue> q, MatrixObject min, DataCharacteristics mcOut,
		FileFormatPropertiesCSV props) {
		FSDataInputStream seqStream = null;
		try {
			JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
			Path path = new Path(min.getFileName());
			FileSystem fs = IOUtilFunctions.getFileSystem(path, job);

			MatrixReader.checkValidInputFile(fs, path);

			List<Path> files = collectCSVPaths(fs, path);

			boolean hasHeader = props.hasHeader();
			CSVReadConfig readCfg = new CSVReadConfig(props.getDelim(), props.isFill(), props.getFillValue(),
				props.getNAStrings());
			long knownCols = mcOut.getCols();
			long expectedRows = mcOut.getRows();
			int numCols = (knownCols > 0 && knownCols <= Integer.MAX_VALUE) ? (int) knownCols : -1;
			long globalRowIdx = 0;
			long totalNonZeros = 0;

			int chunkCapacity = Math.max(blen, blen * MAX_BLOCKS_IN_CACHE);
			double[] rowScratch = new double[chunkCapacity];
			double[][] chunkRows = new double[blen][];
			RowState[] rowStates = new RowState[blen];

			int seqFileIdx = files.isEmpty() ? -1 : 0;
			if(seqFileIdx >= 0)
				seqStream = fs.open(files.get(seqFileIdx));
			boolean headerConsumed = !hasHeader;
			if(!headerConsumed && seqFileIdx == 0 && seqStream != null) {
				skipRemainingRow(seqStream);
				headerConsumed = true;
			}

			while(true) {
				int rowsInBuffer = 0;
				int chunkCols = -1;

				while(rowsInBuffer < blen) {
					if(seqStream == null) {
						if(seqFileIdx + 1 < files.size()) {
							seqStream = fs.open(files.get(++seqFileIdx));
						}
						else {
							break;
						}
					}

					long startPos = seqStream.getPos();
					RowState state = new RowState(seqFileIdx);
					state.nextOffset = startPos;

					ChunkReadResult result = readChunk(seqStream, rowScratch, chunkCapacity, readCfg);
					if(result == null) {
						IOUtilFunctions.closeSilently(seqStream);
						seqStream = null;
						continue;
					}
					if(result.columnsRead <= 0)
						throw new DMLRuntimeException("Invalid CSV row with zero columns detected.");

					chunkRows[rowsInBuffer] = Arrays.copyOf(rowScratch, result.columnsRead);
					if(chunkCols < 0)
						chunkCols = result.columnsRead;
					else if(result.columnsRead != chunkCols)
						throw new DMLRuntimeException("Inconsistent number of columns detected within a block row.");

					state.nextOffset = result.nextOffset;
					state.columnsProcessed = result.columnsRead;
					state.finished = result.rowFinished;
					rowStates[rowsInBuffer] = state;

					if(!result.rowFinished)
						skipRemainingRow(seqStream);

					rowsInBuffer++;
				}

				if(rowsInBuffer == 0)
					break;

				long blockRowIndex = (globalRowIdx / blen) + 1;
				totalNonZeros += emitChunk(q, chunkRows, rowsInBuffer, chunkCols, blockRowIndex, 0L);
				long colOffset = chunkCols;

				while(!allRowsFinished(rowStates, rowsInBuffer)) {
					chunkCols = -1;
					for(int r = 0; r < rowsInBuffer; r++) {
						RowState state = rowStates[r];
						if(state.finished)
							throw new DMLRuntimeException("Detected inconsistent row lengths while reading CSV input.");

						try(FSDataInputStream rowStream = fs.open(files.get(state.fileIdx))) {
							rowStream.seek(state.nextOffset);
							ChunkReadResult result = readChunk(rowStream, rowScratch, chunkCapacity, readCfg);
							if(result == null || result.columnsRead <= 0)
								throw new DMLRuntimeException("Unexpected end of CSV row while resuming chunk.");

							if(chunkCols < 0)
								chunkCols = result.columnsRead;
							else if(result.columnsRead != chunkCols)
								throw new DMLRuntimeException(
									"Inconsistent number of columns detected while streaming block columns.");

							chunkRows[r] = Arrays.copyOf(rowScratch, result.columnsRead);
							state.nextOffset = result.nextOffset;
							state.columnsProcessed += result.columnsRead;
							state.finished = result.rowFinished;
						}
					}

					totalNonZeros += emitChunk(q, chunkRows, rowsInBuffer, chunkCols, blockRowIndex, colOffset);
					colOffset += chunkCols;
				}

				int rowColumns = rowStates[0].columnsProcessed;
				for(int r = 1; r < rowsInBuffer; r++) {
					if(rowStates[r].columnsProcessed != rowColumns)
						throw new DMLRuntimeException("Inconsistent number of columns detected across rows.");
				}

				if(numCols < 0) {
					numCols = rowColumns;
					mcOut.setCols(numCols);
				}
				else if(rowColumns != numCols) {
					throw new DMLRuntimeException(
						"Read matrix inconsistent with given meta data: expected " + numCols + " columns but found " +
							rowColumns + ".");
				}

				for(int r = 0; r < rowsInBuffer; r++) {
					chunkRows[r] = null;
					rowStates[r] = null;
				}

				globalRowIdx += rowsInBuffer;
			}

			q.closeInput();

			if(numCols > 0)
				mcOut.setCols(numCols);
			if(globalRowIdx > 0) {
				if(expectedRows > 0 && expectedRows != globalRowIdx)
					throw new DMLRuntimeException(
						"Read matrix inconsistent with given meta data: expected " + expectedRows + " rows but found " +
							globalRowIdx + ".");
				mcOut.setRows(globalRowIdx);
			}
			else if(expectedRows > 0) {
				throw new DMLRuntimeException(
					"Read matrix inconsistent with given meta data: expected " + expectedRows + " rows but found 0.");
			}
			mcOut.setNonZeros(totalNonZeros);
		}
		catch(IOException ex) {
			throw new DMLRuntimeException("Failed to read CSV for out-of-core reblock.", ex);
		}
		finally {
			IOUtilFunctions.closeSilently(seqStream);
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

	private long emitChunk(OOCStream<IndexedMatrixValue> q, double[][] chunkRows, int rowsInBuffer, int chunkCols,
		long blockRowIndex, long colOffset) {
		if(chunkCols <= 0)
			throw new DMLRuntimeException("Unable to emit column chunk without data.");

		int numColBlocks = (chunkCols + blen - 1) / blen;
		long totalNonZeros = 0;

		for(int cb = 0; cb < numColBlocks; cb++) {
			int colStart = cb * blen;
			int colBlockSize = Math.min(chunkCols - colStart, blen);

			MatrixBlock block = new MatrixBlock(rowsInBuffer, colBlockSize, false);
			block.allocateDenseBlock();
			DenseBlock db = block.getDenseBlock();

			long blockNonZeros = 0;
			for(int r = 0; r < rowsInBuffer; r++) {
				double[] src = chunkRows[r];
				double[] dest = db.values(r);
				int dpos = db.pos(r);
				for(int c = 0; c < colBlockSize; c++) {
					double v = src[colStart + c];
					dest[dpos + c] = v;
					if(v != 0)
						blockNonZeros++;
				}
			}

			block.setNonZeros(blockNonZeros);
			block.examSparsity();

			long blockColIndex = (colOffset / blen) + cb + 1;
			MatrixIndexes idx = new MatrixIndexes(blockRowIndex, blockColIndex);
			q.enqueue(new IndexedMatrixValue(idx, block));
			totalNonZeros += blockNonZeros;
		}

		return totalNonZeros;
	}

	/**
	 * Returns the number of columns read
	 */
	private void skipRemainingRow(FSDataInputStream stream) throws IOException {
		int ch;
		while((ch = stream.read()) != -1) {
			if(ch == '\n')
				break;
			if(ch == '\r') {
				long pos = stream.getPos();
				int next = stream.read();
				if(next != '\n')
					stream.seek(pos);
				break;
			}
		}
	}

	private boolean allRowsFinished(RowState[] states, int rowsInBuffer) {
		for(int r = 0; r < rowsInBuffer; r++) {
			if(states[r] == null || !states[r].finished)
				return false;
		}
		return true;
	}

	private ChunkReadResult readChunk(FSDataInputStream stream, double[] destRow, int chunkCapacity, CSVReadConfig cfg)
		throws IOException {
		int columnsRead = 0;
		boolean emptyValuesFound = false;
		FieldStatus lastStatus = FieldStatus.DELIMITER;
		StringBuilder fieldBuilder = new StringBuilder(64);

		while(columnsRead < chunkCapacity) {
			FieldReadResult field = readField(stream, cfg, fieldBuilder);
			if(field == null) {
				if(columnsRead == 0)
					return null;
				lastStatus = FieldStatus.EOF;
				break;
			}

			String token = field.value.trim();
			double cellValue;
			if(token.isEmpty()) {
				emptyValuesFound = true;
				cellValue = cfg.fillValue;
			}
			else {
				cellValue = UtilFunctions.parseToDouble(token, cfg.naStrings);
			}
			destRow[columnsRead++] = cellValue;
			lastStatus = field.status;
			if(field.status != FieldStatus.DELIMITER)
				break;
		}

		try {
			IOUtilFunctions.checkAndRaiseErrorCSVEmptyField(null, cfg.fill, emptyValuesFound);
		}
		catch(IOException ioe) {
			throw new DMLRuntimeException(ioe);
		}

		long nextOffset = stream.getPos();
		boolean rowFinished = (lastStatus != FieldStatus.DELIMITER);
		return new ChunkReadResult(columnsRead, rowFinished, nextOffset);
	}

	private FieldReadResult readField(FSDataInputStream stream, CSVReadConfig cfg, StringBuilder fieldBuilder)
		throws IOException {
		fieldBuilder.setLength(0);
		boolean readAny = false;
		while(true) {
			int ch = stream.read();
			if(ch == -1) {
				if(!readAny)
					return null;
				return new FieldReadResult(fieldBuilder.toString(), FieldStatus.EOF);
			}
			readAny = true;
			char c = (char) ch;
			if(c == '\n')
				return new FieldReadResult(fieldBuilder.toString(), FieldStatus.LINE_END);
			if(c == '\r') {
				long posAfter = stream.getPos();
				int next = stream.read();
				if(next != '\n')
					stream.seek(posAfter);
				return new FieldReadResult(fieldBuilder.toString(), FieldStatus.LINE_END);
			}
			fieldBuilder.append(c);
			if(cfg.delimChars.length > 0 && matchesDelimiter(fieldBuilder, cfg.delimChars)) {
				fieldBuilder.setLength(fieldBuilder.length() - cfg.delimChars.length);
				return new FieldReadResult(fieldBuilder.toString(), FieldStatus.DELIMITER);
			}
		}
	}

	private static boolean matchesDelimiter(StringBuilder builder, char[] delimChars) {
		if(delimChars.length == 0 || builder.length() < delimChars.length)
			return false;
		int start = builder.length() - delimChars.length;
		for(int i = 0; i < delimChars.length; i++) {
			if(builder.charAt(start + i) != delimChars[i])
				return false;
		}
		return true;
	}

	private static final class RowState {
		private final int fileIdx;
		private long nextOffset;
		private int columnsProcessed;
		private boolean finished;

		private RowState(int fileIdx) {
			this.fileIdx = fileIdx;
		}
	}

	private static final class ChunkReadResult {
		private final int columnsRead;
		private final boolean rowFinished;
		private final long nextOffset;

		private ChunkReadResult(int columnsRead, boolean rowFinished, long nextOffset) {
			this.columnsRead = columnsRead;
			this.rowFinished = rowFinished;
			this.nextOffset = nextOffset;
		}
	}

	private static final class CSVReadConfig {
		private final char[] delimChars;
		private final boolean fill;
		private final double fillValue;
		private final HashSet<String> naStrings;

		private CSVReadConfig(String delim, boolean fill, double fillValue, HashSet<String> naStrings) {
			this.delimChars = delim == null ? new char[0] : delim.toCharArray();
			this.fill = fill;
			this.fillValue = fillValue;
			this.naStrings = naStrings;
		}
	}

	private static final class FieldReadResult {
		private final String value;
		private final FieldStatus status;

		private FieldReadResult(String value, FieldStatus status) {
			this.value = value;
			this.status = status;
		}
	}

	private enum FieldStatus {
		DELIMITER,
		LINE_END,
		EOF
	}
}
