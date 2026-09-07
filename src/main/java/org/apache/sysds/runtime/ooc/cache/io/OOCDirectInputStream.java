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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

import com.sun.nio.file.ExtendedOpenOption;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.util.CleanerUtil;

final class OOCDirectInputStream extends FSInputStream {
	private static final int ALIGNMENT = 4096;
	private final FileChannel _channel;
	private ByteBuffer _buffer;
	private ByteBuffer _allocation;
	private long _bufferStart;
	private long _position;

	OOCDirectInputStream(Path path, int bufferSize) throws IOException {
		if(bufferSize <= 0)
			throw new IllegalArgumentException("Direct read buffer size must be positive");
		if(!CleanerUtil.UNMAP_SUPPORTED)
			throw new IOException("OOC direct buffer cleanup unavailable: " + CleanerUtil.UNMAP_NOT_SUPPORTED_REASON);
		try {
			_channel = FileChannel.open(path, StandardOpenOption.READ, ExtendedOpenOption.DIRECT);
		}
		catch(UnsupportedOperationException e) {
			throw new IOException("OOC direct reads are not supported for " + path, e);
		}
		try {
			allocateBuffer(bufferSize);
		}
		catch(IOException | RuntimeException | Error e) {
			_channel.close();
			throw e;
		}
	}

	@Override
	public int read() throws IOException {
		byte[] value = new byte[1];
		return read(value, 0, 1) < 0 ? -1 : value[0] & 255;
	}

	@Override
	public int read(byte[] bytes, int offset, int length) throws IOException {
		if(!_channel.isOpen())
			throw new ClosedChannelException();
		Objects.checkFromIndexSize(offset, length, bytes.length);
		if(length == 0)
			return 0;
		if(_position < _bufferStart || _position >= _bufferStart + _buffer.limit()) {
			long start = _position / ALIGNMENT * ALIGNMENT;
			int skip = (int) (_position - start);
			int size = (int) Math.min((long) length + skip, Integer.MAX_VALUE - 2L * ALIGNMENT);
			size = (size + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
			if(size > _buffer.capacity())
				allocateBuffer(size);
			_buffer.clear();
			_buffer.limit(size);
			int n = _channel.read(_buffer, start);
			_buffer.flip();
			_bufferStart = start;
			if(n <= skip)
				return -1;
		}
		_buffer.position((int) (_position - _bufferStart));
		int n = Math.min(length, _buffer.remaining());
		_buffer.get(bytes, offset, n);
		_position += n;
		return n;
	}

	@Override
	public void seek(long position) throws IOException {
		if(position < 0)
			throw new EOFException("Negative seek offset");
		_position = position;
	}

	@Override
	public long getPos() {
		return _position;
	}

	@Override
	public boolean seekToNewSource(long position) {
		return false;
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

	private void allocateBuffer(int size) throws IOException {
		releaseBuffer();
		_allocation = ByteBuffer.allocateDirect((size + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT + ALIGNMENT);
		_buffer = _allocation.alignedSlice(ALIGNMENT);
		_buffer.limit(0);
	}

	private void releaseBuffer() throws IOException {
		if(_allocation != null) {
			CleanerUtil.getCleaner().freeBuffer(_allocation);
			_allocation = null;
			_buffer = null;
		}
	}
}
