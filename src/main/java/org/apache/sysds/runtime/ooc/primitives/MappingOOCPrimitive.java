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
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.List;
import java.util.function.Function;

public class MappingOOCPrimitive extends OOCPrimitive {
	private final Function<IndexedMatrixValue, MatrixBlock> _fn;
	private final StreamContext _sc;

	private MappingOOCPrimitive(OOCPrimitive inputPrimitive, OOCStreamable<IndexedMatrixValue> inputStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, Function<IndexedMatrixValue, MatrixBlock> fn,
		StreamContext sc) {
		super(inputPrimitive == null ? List.of() : List.of(inputPrimitive), List.of(inputStreamable),
			List.of(outputStreamable));
		_fn = fn;
		_sc = sc;
	}

	public MappingOOCPrimitive(OOCStreamable<IndexedMatrixValue> inputStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, Function<IndexedMatrixValue, MatrixBlock> fn, StreamContext sc) {
		this(safePrimitive(inputStreamable), inputStreamable, outputStreamable, fn, sc);
	}

	@Override
	public boolean isTileLocal() {
		return true;
	}

	@Override
	public boolean isOneToOne() {
		return true;
	}

	@Override
	public boolean isIndexPreserving() {
		return true;
	}

	@Override
	public long getDenseTileMemoryFactor() {
		return 2;
	}

	@Override
	public void inferPatterns() {
		OOCStreamable<IndexedMatrixValue> input = getInputStream(0);
		_pattern = _pattern.preferred(getPattern(input));
		inferPatterns(getParents());
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = _pattern.preferred(accessPattern);
		if(!getChildren().isEmpty() && !getChildren().get(0).hasStartedExecution())
			getChildren().get(0).requestPattern(accessPattern);
	}

	@Override
	public void startExecution() {
		OOCStreamable<IndexedMatrixValue> inputStreamable = getInputStream(0);
		OOCStreamable<IndexedMatrixValue> outputStreamable = getOutputStream(0);
		final OOCStream<IndexedMatrixValue> in = inputStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> out = outputStreamable.getWriteStream();
		OOCInstructionUtils.submitAdmittedOOCTasks(in, out,
			input -> new IndexedMatrixValue(input.getIndexes(), _fn.apply(input)),
			IndexedMatrixValue::getIndexes, _allowance, _allocFn, _startsRegion, _crossBoundaries, _sc);
	}
}
