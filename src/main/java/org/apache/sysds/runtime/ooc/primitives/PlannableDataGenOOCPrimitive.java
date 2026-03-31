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

package org.apache.sysds.runtime.ooc.primitives;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.util.IndexRange;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

public class PlannableDataGenOOCPrimitive extends PlannableOOCPrimitive {
	private final OOCStream<MatrixIndexes> _stream;
	private final OOCStreamable<?> _outputStream;
	private final long numRowBlocks;
	private final long numColBlocks;

	public PlannableDataGenOOCPrimitive(OOCStreamable<?> outputStream) {
		super(Collections.emptyList());
		_stream = new SubscribableTaskQueue<>();
		_outputStream = outputStream;
		numRowBlocks = OOCUtils.getNumRowBlocks(outputStream.getDataCharacteristics());
		numColBlocks = OOCUtils.getNumColBlocks(outputStream.getDataCharacteristics());
	}

	@Override
	public void requestNext(MatrixIndexes idx) {
		_stream.enqueue(idx);
	}

	public OOCStream<MatrixIndexes> getRequestStream() {
		return _stream;
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return Collections.emptyList();
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStream);
	}

	@Override
	public BiFunction<Boolean, IndexRange, IndexRange> getIXTransform() {
		return null;
	}

	@Override
	public void requestNext() {
		throw new NotImplementedException();
	}

	@Override
	public void inferPatterns() {
		_pattern = OOCAccessPattern.ANY;
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		_pattern = accessPattern;
	}

	@Override
	public void start() {
		System.out.println("Starting DataGen: " + _pattern);
		switch(_pattern) {
			case COL_MAJOR:
				startColMajor();
				break;
			default:
				startRowMajor();
				break;
		}
		_stream.closeInput();
	}

	private void startRowMajor() {
		long nTiles = numRowBlocks*numColBlocks;
		for(long i = 0; i < nTiles; i++) {
			_stream.enqueue(new MatrixIndexes(i / numRowBlocks + 1, i % numRowBlocks + 1));
		}
	}

	private void startColMajor() {
		long nTiles = numRowBlocks*numColBlocks;
		for(long i = 0; i < nTiles; i++) {
			_stream.enqueue(new MatrixIndexes(i % numColBlocks + 1, i / numColBlocks + 1));
		}
	}
}
