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

import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;

import java.util.List;

public class MappingOOCPrimitive extends OOCPrimitive {
	private final OOCStreamable<?> _inputStreamable;
	private final OOCStreamable<?> _outputStreamable;

	private MappingOOCPrimitive(OOCPrimitive inputPrimitive, OOCStreamable<?> inputStreamable, OOCStreamable<?> outputStreamable) {
		super(inputPrimitive == null ? List.of() : List.of(inputPrimitive));
		_inputStreamable = inputStreamable;
		_outputStreamable = outputStreamable;
	}

	public MappingOOCPrimitive(OOCStreamable<?> inputStreamable, OOCStreamable<?> outputStreamable) {
		this(inputStreamable == null ? null : inputStreamable.getPrimitive(), inputStreamable, outputStreamable);
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return List.of(_inputStreamable);
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStreamable);
	}

	@Override
	public void inferPatterns() {
		_pattern = getPattern(_inputStreamable);
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = accessPattern;
		if(!getChildren().isEmpty())
			getChildren().get(0).requestPattern(accessPattern);
	}
}
