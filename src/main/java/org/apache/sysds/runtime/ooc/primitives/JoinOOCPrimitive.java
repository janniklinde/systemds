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

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.util.IndexRange;

import java.util.List;
import java.util.function.BiFunction;

public class JoinOOCPrimitive extends PlannableOOCPrimitive {
	private final List<OOCStreamable<?>> _inputStreamables;
	private final OOCStreamable<?> _outputStreamable;
	private final OOCStream<MatrixIndexes> _nextStream;
	private final long _numRowBlocks;
	private final long _numColBlocks;
	private long _ptr;

	private JoinOOCPrimitive(List<OOCPrimitive> inputPrimitives, List<OOCStreamable<?>> inputs, OOCStreamable<?> output) {
		super(inputPrimitives);
		_inputStreamables = inputs;
		_outputStreamable = output;
		_nextStream = new SubscribableTaskQueue<>();
		_numRowBlocks = OOCUtils.getNumRowBlocks(output.getDataCharacteristics());
		_numColBlocks = OOCUtils.getNumColBlocks(output.getDataCharacteristics());
	}

	public JoinOOCPrimitive(List<OOCStreamable<?>> inputs, OOCStreamable<?> output) {
		this(inputs.stream().map(OOCStreamable::getPrimitive).toList(), inputs, output);
	}

	@Override
	public void requestNext(MatrixIndexes idx) {
		_nextStream.enqueue(idx);
	}

	public OOCStream<MatrixIndexes> getRequestStream() {
		return _nextStream;
	}

	public void requestChildren(List<MatrixIndexes> idxs) {
		// TODO
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return _inputStreamables;
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStreamable);
	}

	@Override
	public BiFunction<Boolean, IndexRange, IndexRange> getIXTransform() {
		return _outputStreamable.getIXTransform();
	}

	@Override
	public void requestNext() {
		long rBlock = _ptr / _numColBlocks + 1;
		long cBlock = (_ptr % _numColBlocks) + 1;
		_ptr++;
		requestNext(new MatrixIndexes(rBlock, cBlock));
	}

	@Override
	public void inferPatterns() {
		_pattern = OOCAccessPattern.ANY;
		for(OOCPrimitive p : getChildren()) {
			if(p.getAccessPattern() == OOCAccessPattern.UNSET)
				return;
			_pattern = _pattern.fused(p.getAccessPattern());
		}
		if(_pattern.isPlannable() && _pattern != OOCAccessPattern.ANY) {
			for(OOCPrimitive p : getChildren()) {
				p.requestPattern(_pattern);
			}
		}
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		for(OOCPrimitive p : getChildren()) {
			p.requestPattern(accessPattern);
		}
	}
}
