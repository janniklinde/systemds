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
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.util.ByteBufferDataInput;

final class DirectRecordReader implements Closeable {
	private static final int SYNC_ESCAPE = -1;

	private final DirectRangeReader _reader;

	DirectRecordReader(String file) throws IOException {
		URI uri = new org.apache.hadoop.fs.Path(file).toUri();
		Path path = uri.getScheme() == null ? Paths.get(file) : Paths.get(uri);
		_reader = new DirectRangeReader(path);
	}

	IndexedMatrixValue read(OOCIOHandler.SourceBlockDescriptor src, MatrixBlock matrix) throws IOException {
		ByteBuffer buffer = _reader.read(src.offset, src.recordLength);

		int recordLength = buffer.getInt();
		if(recordLength == SYNC_ESCAPE) {
			buffer.position(buffer.position() + 16);
			recordLength = buffer.getInt();
		}
		int keyLength = buffer.getInt();
		if(keyLength < 16 || recordLength < keyLength || recordLength > buffer.remaining())
			throw new IOException("Invalid SequenceFile record at " + src.offset + " in " + src.path);

		ByteBufferDataInput input = new ByteBufferDataInput(buffer);
		MatrixIndexes indexes = new MatrixIndexes();
		indexes.readFields(input);
		if(keyLength > 16)
			input.skipBytes(keyLength - 16);
		int valueEnd = buffer.position() + recordLength - keyLength;
		matrix.readFields(input);
		if(buffer.position() != valueEnd)
			throw new IOException("Matrix block consumed " + (buffer.position() - valueEnd)
				+ " unexpected bytes at " + src.offset + " in " + src.path);
		return new IndexedMatrixValue(indexes, matrix);
	}

	@Override
	public void close() throws IOException {
		_reader.close();
	}
}
