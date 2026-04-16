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

import java.util.ArrayList;
import java.util.List;

public abstract class PlannableOOCPrimitive extends OOCPrimitive {
	protected final List<OOCPrimitive> _keyPrimitives;

	public PlannableOOCPrimitive(List<OOCPrimitive> children) {
		super(children);
		_keyPrimitives = new ArrayList<>(getChildren().size());
		for(OOCPrimitive child : getChildren()) {
			findKeyPrimitives(child);
		}
	}

	@Override
	public boolean isPlannable() {
		return true;
	}

	private void findKeyPrimitives(OOCPrimitive primitive) {
		if(primitive.isPlannable() || primitive.isLeaf() || primitive.getChildren().size() > 1) {
			_keyPrimitives.add(primitive);
			return;
		}
		for(OOCPrimitive child : primitive.getChildren())
			findKeyPrimitives(child);
	}
}
