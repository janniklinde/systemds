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

package org.apache.sysds.runtime.ooc.cache.collections;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class ConcurrentGrowableBitSet {
	private static final int SEGMENT_BITS = 14;
	private static final int SEGMENT_SIZE = 1 << SEGMENT_BITS;
	private static final int SEGMENT_MASK = SEGMENT_SIZE - 1;
	private static final int DIRECTORY_BITS = 6;
	private static final int DIRECTORY_SIZE = 1 << DIRECTORY_BITS;
	private static final int DIRECTORY_MASK = DIRECTORY_SIZE - 1;
	private static final int ROOT_SIZE = 32;
	private static final VarHandle ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);

	private final Object[] roots = new Object[ROOT_SIZE];

	public boolean get(int index) {
		checkIndex(index);
		int segmentIndex = index >>> SEGMENT_BITS;
		Object[] middle = (Object[])ARRAY.getAcquire(roots, segmentIndex >>> (2 * DIRECTORY_BITS));
		if(middle == null)
			return false;
		Object[] leaves = (Object[])ARRAY.getAcquire(middle, (segmentIndex >>> DIRECTORY_BITS) & DIRECTORY_MASK);
		if(leaves == null)
			return false;
		ConcurrentBitSet segment = (ConcurrentBitSet)ARRAY.getAcquire(leaves, segmentIndex & DIRECTORY_MASK);
		return segment != null && segment.get(index & SEGMENT_MASK);
	}

	public boolean set(int index) {
		checkIndex(index);
		int segmentIndex = index >>> SEGMENT_BITS;
		Object[] middle = getOrCreateNode(roots, segmentIndex >>> (2 * DIRECTORY_BITS));
		Object[] leaves = getOrCreateNode(middle, (segmentIndex >>> DIRECTORY_BITS) & DIRECTORY_MASK);
		int offset = segmentIndex & DIRECTORY_MASK;
		ConcurrentBitSet segment = (ConcurrentBitSet)ARRAY.getAcquire(leaves, offset);
		while(segment == null) {
			ConcurrentBitSet created = new ConcurrentBitSet(SEGMENT_SIZE);
			if(ARRAY.compareAndSet(leaves, offset, null, created))
				segment = created;
			else
				segment = (ConcurrentBitSet)ARRAY.getAcquire(leaves, offset);
		}
		return segment.set(index & SEGMENT_MASK);
	}

	private static Object[] getOrCreateNode(Object[] parent, int index) {
		Object[] node = (Object[])ARRAY.getAcquire(parent, index);
		while(node == null) {
			Object[] created = new Object[DIRECTORY_SIZE];
			if(ARRAY.compareAndSet(parent, index, null, created))
				return created;
			node = (Object[])ARRAY.getAcquire(parent, index);
		}
		return node;
	}

	private static void checkIndex(int index) {
		if(index < 0)
			throw new IndexOutOfBoundsException("Negative bit index: " + index);
	}
}
