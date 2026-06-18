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

package org.apache.sysds.runtime.ooc.planning;

import java.util.function.ToIntFunction;

import org.apache.sysds.runtime.matrix.data.MatrixIndexes;

/**
 * Minimal primitive declaration for a planner-owned materialized input: the input slot that must be
 * materialized and the preferred physical store layout. The layout is a locality hint; when
 * several consumers of the same input disagree, the planner may choose any one layout and share the
 * resulting materialization.
 */
public final class OOCMaterializedInputRequest {
	private final int _inputIndex;
	private final ToIntFunction<MatrixIndexes> _preferredLayout;
	private final int _expectedReaders;
	private final int _consumers;

	public OOCMaterializedInputRequest(int inputIndex, ToIntFunction<MatrixIndexes> preferredLayout,
		int expectedReaders, int consumers) {
		if(inputIndex < 0)
			throw new IllegalArgumentException("Materialized input request requires a valid input slot.");
		if(preferredLayout == null)
			throw new IllegalArgumentException("Materialized input request requires a preferred layout.");
		_inputIndex = inputIndex;
		_preferredLayout = preferredLayout;
		_expectedReaders = expectedReaders;
		_consumers = consumers;
	}

	public int inputIndex() {
		return _inputIndex;
	}

	public ToIntFunction<MatrixIndexes> preferredLayout() {
		return _preferredLayout;
	}

	public int expectedReaders() {
		return _expectedReaders;
	}

	public int consumers() {
		return _consumers;
	}
}
