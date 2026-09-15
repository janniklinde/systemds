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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.sun.nio.file.ExtendedOpenOption;
import org.apache.hadoop.util.CleanerUtil;

final class DirectRangeReader implements Closeable {
	private static final int ALIGNMENT = 4096;

	private final Path _path;
	private final FileChannel _channel;
	private ByteBuffer _allocation;
	private ByteBuffer _buffer;
	private byte[] _heapBuffer;

	DirectRangeReader(Path path) throws IOException {
		_path = path;
		_channel = FileChannel.open(path, StandardOpenOption.READ, ExtendedOpenOption.DIRECT);
	}

	ByteBuffer read(long offset, int length) throws IOException {
		long alignedStart = offset / ALIGNMENT * ALIGNMENT;
		int skip = (int) (offset - alignedStart);
		int readLength = align(skip + length);
		ensureCapacity(readLength);
		_buffer.clear();
		_buffer.limit(readLength);
		int bytesRead = _channel.read(_buffer, alignedStart);
		if(bytesRead < skip + length)
			throw new EOFException("Short direct read at " + offset + " in " + _path);
		_buffer.limit(skip + length);
		_buffer.position(skip);
		_buffer.order(ByteOrder.BIG_ENDIAN);
		return _buffer;
	}

	ByteBuffer readHeap(long offset, int length) throws IOException {
		ByteBuffer direct = read(offset, length);
		if(_heapBuffer == null || _heapBuffer.length < length)
			_heapBuffer = new byte[length];
		direct.get(_heapBuffer, 0, length);
		return ByteBuffer.wrap(_heapBuffer, 0, length).order(ByteOrder.BIG_ENDIAN);
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
