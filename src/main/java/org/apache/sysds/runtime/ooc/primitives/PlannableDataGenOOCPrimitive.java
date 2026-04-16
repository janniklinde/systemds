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

import java.util.Collections;
import java.util.List;

public class PlannableDataGenOOCPrimitive extends PlannableOOCPrimitive {
	private final OOCStreamable<?> _outputStream;

	public PlannableDataGenOOCPrimitive(OOCStreamable<?> outputStream) {
		super(Collections.emptyList());
		_outputStream = outputStream;
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
	public void inferPatterns() {
		_pattern = OOCAccessPattern.ANY;
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		_pattern = accessPattern;
	}

	@Override
	public void startExecution() {
		// TODO
	}
}
