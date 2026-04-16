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

import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;

import java.util.HashSet;
import java.util.Set;

public class OOCPlanner {
	public static void compile(OOCPrimitive root) {
		Set<OOCPrimitive> leaves = new HashSet<>();
		findLeaves(root, leaves);
		inferAccessPatterns(leaves);

		if(root.getAccessPattern() == OOCAccessPattern.ANY)
			root.requestPattern(OOCAccessPattern.ROW_MAJOR);

		for(OOCPrimitive leaf : leaves)
			leaf.startExecution();
	}

	private static void findLeaves(OOCPrimitive primitive, Set<OOCPrimitive> leaves) {
		if(primitive.isLeaf()) {
			leaves.add(primitive);
			return;
		}

		for(OOCPrimitive child : primitive.getChildren())
			findLeaves(child, leaves);
	}

	private static void inferAccessPatterns(Set<OOCPrimitive> leaves) {
		for(OOCPrimitive leaf : leaves)
			leaf.inferPatterns();
	}
}
