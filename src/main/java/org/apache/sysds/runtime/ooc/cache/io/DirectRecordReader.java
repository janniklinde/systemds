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

package org.apache.sysds.runtime.ooc.cache.io;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.sun.nio.file.ExtendedOpenOption;
import org.apache.hadoop.util.CleanerUtil;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.util.ByteBufferDataInput;

final class DirectRecordReader implements Closeable {
	private static final int ALIGNMENT = 4096;
	private static final int SYNC_ESCAPE = -1;

	private final FileChannel _channel;
	private ByteBuffer _allocation;
	private ByteBuffer _buffer;

	DirectRecordReader(String file) throws IOException {
		URI uri = new org.apache.hadoop.fs.Path(file).toUri();
		Path path = uri.getScheme() == null ? Paths.get(file) : Paths.get(uri);
		_channel = FileChannel.open(path, StandardOpenOption.READ, ExtendedOpenOption.DIRECT);
	}

	IndexedMatrixValue read(OOCIOHandler.SourceBlockDescriptor src, MatrixBlock matrix) throws IOException {
		long alignedStart = src.offset / ALIGNMENT * ALIGNMENT;
		int skip = (int) (src.offset - alignedStart);
		int readLength = align(skip + src.recordLength);
		ensureCapacity(readLength);
		_buffer.clear();
		_buffer.limit(readLength);
		int bytesRead = _channel.read(_buffer, alignedStart);
		if(bytesRead < skip + src.recordLength)
			throw new EOFException("Short direct record read at " + src.offset + " in " + src.path);
		_buffer.flip();
		_buffer.position(skip);
		_buffer.order(ByteOrder.BIG_ENDIAN);

		int recordLength = _buffer.getInt();
		if(recordLength == SYNC_ESCAPE) {
			_buffer.position(_buffer.position() + 16);
			recordLength = _buffer.getInt();
		}
		int keyLength = _buffer.getInt();
		if(keyLength < 16 || recordLength < keyLength || recordLength > _buffer.remaining())
			throw new IOException("Invalid SequenceFile record at " + src.offset + " in " + src.path);

		ByteBufferDataInput input = new ByteBufferDataInput(_buffer);
		MatrixIndexes indexes = new MatrixIndexes();
		indexes.readFields(input);
		if(keyLength > 16)
			input.skipBytes(keyLength - 16);
		int valueEnd = _buffer.position() + recordLength - keyLength;
		matrix.readFields(input);
		if(_buffer.position() != valueEnd)
			throw new IOException("Matrix block consumed " + (_buffer.position() - valueEnd)
				+ " unexpected bytes at " + src.offset + " in " + src.path);
		return new IndexedMatrixValue(indexes, matrix);
	}

	private void ensureCapacity(int size) throws IOException {
		if(_buffer != null && _buffer.capacity() >= size)
			return;
		releaseBuffer();
		_allocation = ByteBuffer.allocateDirect(size + ALIGNMENT);
		_buffer = _allocation.alignedSlice(ALIGNMENT);
	}

	@Override
	public void close() throws IOException {
		try {
			_channel.close();
		}
		finally {
			releaseBuffer();
		}
	}

	private void releaseBuffer() throws IOException {
		if(_allocation != null) {
			CleanerUtil.getCleaner().freeBuffer(_allocation);
			_allocation = null;
			_buffer = null;
		}
	}

	private static int align(int size) {
		return (size + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
	}
}
