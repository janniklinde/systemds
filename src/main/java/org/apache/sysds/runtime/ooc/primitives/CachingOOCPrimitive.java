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
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.util.IndexRange;

import java.util.List;
import java.util.function.BiFunction;

public class CachingOOCPrimitive extends PlannableOOCPrimitive {
	private final OOCStreamable<?> _inStream;
	private final OOCStreamable<?> _outStream;

	private CachingOOCPrimitive(OOCPrimitive input, OOCStreamable<?> inStream, OOCStreamable<?> outStream) {
		super(List.of(input));
		_inStream = inStream;
		_outStream = outStream;
	}

	public CachingOOCPrimitive(OOCStreamable<?> input, OOCStreamable<?> output) {
		this(input.getPrimitive(), input, output);
	}

	@Override
	public void requestNext(MatrixIndexes idx) {
		throw new NotImplementedException();
		//_keyPrimitives.get(0).requestNext(idx);
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return List.of(_inStream);
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outStream);
	}

	@Override
	public BiFunction<Boolean, IndexRange, IndexRange> getIXTransform() {
		return null;
	}

	@Override
	public void requestNext() {

	}

	@Override
	public void inferPatterns() {
		_pattern = getChildren().get(0).getAccessPattern();
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = accessPattern;
		getChildren().get(0).requestPattern(accessPattern);
	}
}
