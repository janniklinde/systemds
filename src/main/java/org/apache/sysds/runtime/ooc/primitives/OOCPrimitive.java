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
import org.apache.sysds.runtime.ooc.planning.OOCPlanner;

import java.util.ArrayList;
import java.util.List;

public abstract class OOCPrimitive {
	private final List<OOCPrimitive> _children;
	private final List<OOCPrimitive> _parents;
	protected OOCAccessPattern _pattern;

	public OOCPrimitive(List<OOCPrimitive> children) {
		_children = new ArrayList<>();
		if(children != null) {
			for(OOCPrimitive child : children) {
				if(child == null)
					continue;
				_children.add(child);
				child.addParent(this);
			}
		}
		_parents = new ArrayList<>();
		_pattern = OOCAccessPattern.UNSET;
	}

	public List<OOCPrimitive> getChildren() {
		return _children;
	}

	public void addParent(OOCPrimitive p) {
		_parents.add(p);
	}

	public List<OOCPrimitive> getParents() {
		return _parents;
	}

	public boolean isPlannable() {
		return false;
	}

	public boolean isLeaf() {
		return _children.isEmpty();
	}

	public OOCAccessPattern getAccessPattern() {
		return _pattern;
	}

	public final void start() {
		OOCPlanner.compile(this);
	}

	public void startExecution() {
		// Default no-op. Plannable leaf primitives can override this to emit
		// work once the planner has assigned an access pattern.
	}

	protected OOCAccessPattern getPattern(OOCStreamable<?> streamable) {
		if(streamable == null)
			return OOCAccessPattern.UNKNOWN;
		OOCPrimitive primitive = streamable.getPrimitive();
		return primitive == null ? OOCAccessPattern.UNKNOWN : primitive.getAccessPattern();
	}

	public abstract List<OOCStreamable<?>> getInputStreams();
	public abstract List<OOCStreamable<?>> getOutputStreams();
	public abstract void inferPatterns();
	public abstract void requestPattern(OOCAccessPattern accessPattern);
}
