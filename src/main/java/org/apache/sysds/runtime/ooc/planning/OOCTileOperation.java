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

import java.util.Objects;
import java.util.function.ToLongFunction;

/** Symbolic tile dependency and output sparsity bound attached by the OOC instruction builders. */
public final class OOCTileOperation {
	public enum Relation {
		EQUI, TRANSPOSE, ROW_GROUP, COL_GROUP, OPAQUE
	}

	private final Relation[] _inputRelations;
	private final ToLongFunction<long[]> _worstCaseOutputNnz;

	public OOCTileOperation(ToLongFunction<long[]> worstCaseOutputNnz, Relation... inputRelations) {
		if(inputRelations.length == 0)
			throw new IllegalArgumentException("A tile operation requires at least one input relation");
		_inputRelations = inputRelations.clone();
		for(Relation relation : _inputRelations)
			Objects.requireNonNull(relation);
		_worstCaseOutputNnz = Objects.requireNonNull(worstCaseOutputNnz);
	}

	public int getNumInputs() {
		return _inputRelations.length;
	}

	public Relation getInputRelation(int input) {
		return _inputRelations[input];
	}

	public long worstCaseOutputNnz(long[] inputNnz, long outputCells) {
		if(inputNnz.length != _inputRelations.length)
			throw new IllegalArgumentException("Expected " + _inputRelations.length + " input NNZ values");
		long bound = _worstCaseOutputNnz.applyAsLong(inputNnz.clone());
		return bound < 0 ? outputCells : Math.min(bound, outputCells);
	}

	public boolean isIndexPreserving() {
		return _inputRelations.length == 1 && _inputRelations[0] == Relation.EQUI;
	}

	public static ToLongFunction<long[]> denseOutput() {
		return ignored -> -1;
	}

	public static ToLongFunction<long[]> preservesSingleInputNnz() {
		return nnz -> nnz.length == 0 ? -1 : nnz[0];
	}
}
