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

public final class SequentialAccessPattern implements MaterializedStore.AccessPattern {
	private final int size;
	private int next;
	private volatile int consumedThrough;

	public SequentialAccessPattern(int size) {
		this.size = size;
		next = 0;
		consumedThrough = -1;
	}

	@Override
	public boolean hasNext() {
		return next < size;
	}

	@Override
	public int next() {
		if(!hasNext())
			throw new IllegalStateException("No remaining index");
		return next++;
	}

	@Override
	public boolean needs(int index) {
		return index > consumedThrough && index < size;
	}

	@Override
	public void consumed(int index) {
		if(index != consumedThrough + 1)
			throw new IllegalStateException("Sequential consumption out of order: " + index);
		consumedThrough = index;
	}
}
