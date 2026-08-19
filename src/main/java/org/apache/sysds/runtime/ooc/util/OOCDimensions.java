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

package org.apache.sysds.runtime.ooc.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.meta.DataCharacteristics;

/**
 * Single decision point for out-of-core operations that need to know their input dimensions.
 * <p>
 * An instruction either tolerates unresolved dimensions, in which case it asks {@link #known} and picks a plan that
 * does not depend on them, or it cannot be planned without them, in which case it calls {@link #require}. The second
 * group is what the planner will be able to defer once deferred plan construction exists, so those sites are
 * deliberately funnelled through one method rather than each raising their own error.
 */
public final class OOCDimensions {
	private OOCDimensions() {
	}

	/**
	 * Indicates whether an operation can rely on the dimensions of the given inputs.
	 *
	 * @param inputs matrix operands, may be empty
	 * @return true if every input reports both dimensions and a positive block size
	 */
	public static boolean known(MatrixObject... inputs) {
		for(MatrixObject input : inputs)
			if(!known(input))
				return false;
		return true;
	}

	/**
	 * Indicates whether an operation can rely on the dimensions of the given input.
	 *
	 * @param input matrix operand
	 * @return true if the input reports both dimensions and a positive block size
	 */
	public static boolean known(MatrixObject input) {
		if(input == null)
			return false;
		DataCharacteristics dc = input.getDataCharacteristics();
		return dc != null && dc.dimsKnown() && dc.getBlocksize() > 0;
	}

	/**
	 * Fails when an operation that cannot be planned without its input dimensions is given unresolved ones. Null
	 * inputs are skipped so that callers can pass optional operands directly.
	 *
	 * @param opcode operation name used in the error message
	 * @param inputs matrix operands, may contain nulls
	 */
	public static void require(String opcode, MatrixObject... inputs) {
		List<String> unresolved = new ArrayList<>();
		for(int i = 0; i < inputs.length; i++)
			if(inputs[i] != null && !known(inputs[i]))
				unresolved.add("input " + (i + 1) + " " + describe(inputs[i]));
		if(!unresolved.isEmpty())
			throw new DMLRuntimeException("Planner-backed OOC " + opcode + " requires known input dimensions: "
				+ String.join(", ", unresolved));
	}

	private static String describe(MatrixObject input) {
		DataCharacteristics dc = input.getDataCharacteristics();
		if(dc == null)
			return "without characteristics";
		return dc.getRows() + "x" + dc.getCols() + " (blocksize " + dc.getBlocksize() + ")";
	}
}
