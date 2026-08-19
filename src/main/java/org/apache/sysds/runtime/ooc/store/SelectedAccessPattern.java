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

package org.apache.sysds.runtime.ooc.store;

import org.apache.sysds.runtime.ooc.cache.collections.ConcurrentBitSet;

import java.util.Arrays;

public final class SelectedAccessPattern implements MaterializedStore.AccessPattern {
	private final int[] _indices;
	private final int _size;
	private final ConcurrentBitSet _selected;
	private final ConcurrentBitSet _consumed;
	private int _next;

	public SelectedAccessPattern(int size, int[] indices) {
		if(size < 0)
			throw new IllegalArgumentException("Size must not be negative: " + size);
		_indices = indices;
		_size = size;
		_selected = new ConcurrentBitSet(Math.max(1, size));
		_consumed = new ConcurrentBitSet(Math.max(1, size));
		for(int index : indices) {
			if(index < 0 || index >= size)
				throw new IndexOutOfBoundsException("Selected index outside the store: " + index);
			_selected.set(index);
		}
		_next = 0;
	}

	public int selectedCount() {
		return _indices.length;
	}

	@Override
	public boolean hasNext() {
		return _next < _indices.length;
	}

	@Override
	public int next() {
		if(!hasNext())
			throw new IllegalStateException("No remaining index");
		return _indices[_next++];
	}

	@Override
	public boolean needs(int index) {
		return index >= 0 && index < _size && _selected.get(index) && !_consumed.get(index);
	}

	@Override
	public void consumed(int index) {
		if(index < 0 || index >= _size)
			throw new IndexOutOfBoundsException("Invalid consumed index: " + index);
		_consumed.set(index);
	}

	@Override
	public String toString() {
		return "SelectedAccessPattern[" + _indices.length + " of " + _size + ", head=" +
			Arrays.toString(Arrays.copyOf(_indices, Math.min(4, _indices.length))) + "]";
	}
}
