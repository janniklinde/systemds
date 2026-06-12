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
 * Declaration of a materialized input boundary by a migrated consumer primitive (the
 * {@code requiresStore()}/{@code bindStore()} seam): the linearization of the boundary's tile
 * indexes to the store's dense int range and the registration counts the binding enforces. The
 * planner owns everything else (cache, stream id, sink allowance). A single consumer declares
 * 1 reader / 1 consumer; once shared boundaries are migrated, the planner aggregates the requests
 * of all consumers of a boundary into one binding.
 */
public final class OOCStoreRequest {
	private final ToIntFunction<MatrixIndexes> _linearize;
	private final int _expectedReaders;
	private final int _consumers;

	public OOCStoreRequest(ToIntFunction<MatrixIndexes> linearize, int expectedReaders, int consumers) {
		if(linearize == null)
			throw new IllegalArgumentException("Store request requires a linearization function.");
		_linearize = linearize;
		_expectedReaders = expectedReaders;
		_consumers = consumers;
	}

	public ToIntFunction<MatrixIndexes> linearize() {
		return _linearize;
	}

	public int expectedReaders() {
		return _expectedReaders;
	}

	public int consumers() {
		return _consumers;
	}
}
