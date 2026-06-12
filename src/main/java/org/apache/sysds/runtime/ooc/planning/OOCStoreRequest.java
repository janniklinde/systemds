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

import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;

/**
 * Declaration of a materialized input boundary by a migrated consumer primitive (the
 * {@code requiresStore()}/{@code bindStore()} seam): the boundary handle (source streamable), the
 * linearization of the boundary's tile indexes to the store's dense int range, and the registration
 * counts the binding enforces for THIS consumer. The planner owns everything else (cache, stream id,
 * sink allowance) and aggregates the requests of all consumers of one boundary into one shared
 * {@link OOCStoreBinding} — a consumer whose boundary is already materialized receives the existing
 * binding and finds {@code completion()} already done. A null source declares an anonymous boundary
 * that is never shared.
 */
public final class OOCStoreRequest {
	private final OOCStreamable<IndexedMatrixValue> _source;
	private final ToIntFunction<MatrixIndexes> _linearize;
	private final int _expectedReaders;
	private final int _consumers;

	public OOCStoreRequest(OOCStreamable<IndexedMatrixValue> source, ToIntFunction<MatrixIndexes> linearize,
		int expectedReaders, int consumers) {
		if(linearize == null)
			throw new IllegalArgumentException("Store request requires a linearization function.");
		_source = source;
		_linearize = linearize;
		_expectedReaders = expectedReaders;
		_consumers = consumers;
	}

	public OOCStreamable<IndexedMatrixValue> source() {
		return _source;
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
