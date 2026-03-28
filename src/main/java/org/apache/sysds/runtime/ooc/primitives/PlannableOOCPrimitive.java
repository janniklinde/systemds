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

import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.util.IndexRange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

public abstract class PlannableOOCPrimitive extends OOCPrimitive {
	protected final List<OOCPrimitive> _keyPrimitives;
	protected final List<List<BiFunction<Boolean, IndexRange, IndexRange>>> _ixTransforms;
	protected final CompletableFuture<Void> _startHook;

	public PlannableOOCPrimitive(List<OOCPrimitive> children) {
		super(children);
		_keyPrimitives = new ArrayList<>(children.size());
		_ixTransforms = new ArrayList<>(children.size());
		_startHook = new CompletableFuture<>();
		for(OOCPrimitive p : children) {
			List<BiFunction<Boolean, IndexRange, IndexRange>> ixTransforms = new ArrayList<>();
			findKeyPrimitives(p, ixTransforms);
		}
	}

	private void findKeyPrimitives(OOCPrimitive primitive, List<BiFunction<Boolean, IndexRange, IndexRange>> ixTransforms) {
		if(primitive.getIXTransform() != null)
			ixTransforms.add(primitive.getIXTransform());
		if(primitive.isPlannable() || primitive.isLeaf() || primitive.getChildren().size() > 1) {
			_keyPrimitives.add(primitive);
			return;
		}
		for(OOCPrimitive child : primitive.getChildren()) {
			findKeyPrimitives(child, ixTransforms);
		}
	}

	public CompletableFuture<Void> getStartFuture() {
		return _startHook;
	}

	public abstract void requestNext(MatrixIndexes idx);
}
