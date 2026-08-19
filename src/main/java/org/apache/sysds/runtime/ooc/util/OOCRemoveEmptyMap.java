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

package org.apache.sysds.runtime.ooc.util;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.matrix.data.IJV;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

public final class OOCRemoveEmptyMap {
	private final long[] _bits;
	private final long[] _blockBase;
	private final long _length;
	private final long _kept;
	private final int _blen;

	private OOCRemoveEmptyMap(long[] bits, long[] blockBase, long length, long kept, int blen) {
		_bits = bits;
		_blockBase = blockBase;
		_length = length;
		_kept = kept;
		_blen = blen;
	}

	@FunctionalInterface
	public interface RunConsumer {
		void accept(int srcOffset, int length, long outputBlock, int dstOffset);
	}

	public long getLength() {
		return _length;
	}

	public long getKeptCount() {
		return _kept;
	}

	public boolean isSelected(long index) {
		return (_bits[(int) (index >>> 6)] & (1L << (index & 63))) != 0;
	}

	/** Number of blocks the compacted margin occupies. */
	public long getOutputBlocks() {
		return (_kept + _blen - 1) / _blen;
	}

	/**
	 * Expected fragment count per output block along the compacted margin. The count is independent of the other
	 * dimension, so a repartition can index it with the output block position along the margin.
	 *
	 * @return counts indexed by zero-based output block
	 */
	public int[] fragmentCounts() {
		int[] counts = new int[Math.toIntExact(getOutputBlocks())];
		long blocks = (_length + _blen - 1) / _blen;
		for(long block = 0; block < blocks; block++)
			forEachRun(block, blockLength(block),
				(srcOffset, length, outputBlock, dstOffset) -> counts[(int) outputBlock]++);
		return counts;
	}

	private int blockLength(long block) {
		return (int) Math.min(_blen, _length - block * _blen);
	}

	/**
	 * Enumerates the output fragments contributed by one input block. Runs are split at output block boundaries, so
	 * every reported fragment is a contiguous copy into a single output block.
	 *
	 * @param block       zero-based input block along the compacted margin
	 * @param blockLength number of margin entries in that block
	 * @param consumer    receives one call per fragment
	 */
	public void forEachRun(long block, int blockLength, RunConsumer consumer) {
		long start = block * _blen;
		long dst = _blockBase[(int) block];
		int position = 0;
		while(position < blockLength) {
			if(!isSelected(start + position)) {
				position++;
				continue;
			}
			int runEnd = position;
			while(runEnd < blockLength && isSelected(start + runEnd))
				runEnd++;
			int source = position;
			while(source < runEnd) {
				int dstOffset = (int) (dst % _blen);
				int length = Math.min(runEnd - source, _blen - dstOffset);
				consumer.accept(source, length, dst / _blen, dstOffset);
				source += length;
				dst += length;
			}
			position = runEnd;
		}
	}

	/**
	 * @param blen       block size along the compacted margin
	 * @param maxEntries guard on the margin length
	 * @return a builder for the position map
	 */
	public static Builder builder(int blen, long maxEntries) {
		return new Builder(blen, maxEntries);
	}

	/**
	 * Collects a select vector into a position map. Blocks may arrive in any order and from any number of streams; only
	 * their margin offset matters. The margin length is observed from the blocks rather than required up front, because
	 * a select vector produced by an out-of-core chain often carries no published dimensions.
	 * <p>
	 * The orientation of the vector is never assumed. CP accepts a select vector either way round, and SliceLine passes
	 * a column vector for {@code margin="cols"}, so both the block index and the cells are read shape agnostically.
	 */
	public static final class Builder {
		private final int _blen;
		private final long _maxEntries;
		private long[] _bits;
		private long _length;

		private Builder(int blen, long maxEntries) {
			if(blen <= 0)
				throw new DMLRuntimeException("Remove empty requires a positive block size");
			_blen = blen;
			_maxEntries = maxEntries;
			_bits = new long[0];
		}

		/**
		 * Block index along the margin. A vector is singleton in one dimension, so exactly one of the two block indexes
		 * varies and the other stays at 1.
		 *
		 * @param rowIndex    one-based block row of the select block
		 * @param columnIndex one-based block column of the select block
		 * @return zero-based block index along the margin
		 */
		public static long marginBlock(long rowIndex, long columnIndex) {
			return Math.max(rowIndex, columnIndex) - 1;
		}

		private void ensureLength(long length) {
			if(length > _maxEntries)
				throw new DMLRuntimeException("Planner-backed OOC removeEmpty cannot compact a margin of " + length
					+ " entries; the limit is " + _maxEntries);
			_length = Math.max(_length, length);
			int words = Math.toIntExact((_length + 63) / 64);
			if(words > _bits.length)
				_bits = Arrays.copyOf(_bits, Math.max(words, Math.min(2 * _bits.length, Integer.MAX_VALUE - 8)));
		}

		/**
		 * Adds one block of the select vector.
		 *
		 * @param marginOffset zero-based index of the block's first entry along the margin
		 * @param block        select values, a row or column vector
		 * @return this builder
		 */
		public Builder add(long marginOffset, MatrixBlock block) {
			if(block == null)
				return this;
			int rows = block.getNumRows();
			int cols = block.getNumColumns();
			if(rows != 1 && cols != 1)
				throw new DMLRuntimeException("Remove empty requires a select vector, got " + rows + "x" + cols);
			//one dimension is singleton, so the cell index along the margin is the sum of the two coordinates
			ensureLength(marginOffset + (long) rows * cols);
			if(block.isEmptyBlock(false))
				return this;
			if(block.isInSparseFormat()) {
				Iterator<IJV> cells = block.getSparseBlockIterator();
				while(cells.hasNext()) {
					IJV cell = cells.next();
					if(cell.getV() != 0)
						set(marginOffset + cell.getI() + cell.getJ());
				}
				return this;
			}
			for(int row = 0; row < rows; row++)
				for(int col = 0; col < cols; col++)
					if(block.get(row, col) != 0)
						set(marginOffset + row + col);
			return this;
		}

		private void set(long index) {
			if(index < 0 || index >= _length)
				throw new DMLRuntimeException(
					"Select entry " + index + " is outside the compacted margin of length " + _length);
			_bits[(int) (index >>> 6)] |= 1L << (index & 63);
		}

		/**
		 * @param minLength margin length published by the target, or a negative value if unknown
		 * @return the finished position map
		 */
		public OOCRemoveEmptyMap build(long minLength) {
			ensureLength(Math.max(minLength, 0));
			int blocks = Math.toIntExact((_length + _blen - 1) / _blen);
			long[] blockBase = new long[Math.max(blocks, 1)];
			long kept = 0;
			for(int block = 0; block < blocks; block++) {
				blockBase[block] = kept;
				long start = (long) block * _blen;
				long end = Math.min(start + _blen, _length);
				for(long index = start; index < end; index++)
					if((_bits[(int) (index >>> 6)] & (1L << (index & 63))) != 0)
						kept++;
			}
			return new OOCRemoveEmptyMap(_bits, blockBase, _length, kept, _blen);
		}
	}
}
