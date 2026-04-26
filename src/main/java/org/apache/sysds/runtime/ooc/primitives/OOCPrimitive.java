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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCPlanner;
import org.apache.sysds.runtime.ooc.planning.OOCRegionBinding;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;

public abstract class OOCPrimitive {
	private final List<OOCPrimitive> _children;
	private final List<OOCPrimitive> _parents;
	protected OOCAccessPattern _pattern;
	protected OOCRegionBinding _regionBinding;
	protected MemoryAllowance _allowance;
	protected ToLongFunction<MatrixIndexes> _allocFn;
	protected boolean _crossBoundaries;

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

	public void bindRegion(OOCRegionBinding binding, boolean crossBoundaries) {
		_regionBinding = binding;
		_allowance = binding.allowance();
		_allocFn = binding.allocFn();
		_crossBoundaries = crossBoundaries;
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

	public boolean isEmissionControlled() {
		return false;
	}

	public boolean isTileLocal() {
		return false;
	}

	public boolean isOneToOne() {
		return false;
	}

	public boolean isIndexPreserving() {
		return false;
	}

	public boolean isMaterializationBoundary() {
		return false;
	}

	public boolean requiresCache() {
		return false;
	}

	public void bindCache(CachedAllowance cache) {
		throw new UnsupportedOperationException();
	}

	public long getDenseTileMemoryFactor() {
		return 1;
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
		throw new NotImplementedException();
	}

	public void onComplete() {
		_regionBinding.dereference();
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
