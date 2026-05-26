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

package org.apache.sysds.runtime.ooc.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Consumer;

public class OnceArray<T> {
	private static final VarHandle VH = MethodHandles.arrayElementVarHandle(Object[].class);

	protected final Object[] _a;

	public OnceArray(int length) {
		_a = new Object[length];
	}

	public OnceArray() {
		this(16);
	}

	public void put(int i, T value) {
		// Publish value; all prior writes become visible to acquire readers.
		VH.setRelease(_a, i, value);
	}

	@SuppressWarnings("unchecked")
	public T get(int i) {
		// See value immediately once release-store is observed.
		return (T) VH.getAcquire(_a, i);
	}

	public void forEachVisible(Consumer<? super T> action) {
		for(int i = 0; i < _a.length; i++) {
			@SuppressWarnings("unchecked")
			T v = (T) VH.getAcquire(_a, i);
			if(v != null)
				action.accept(v);
		}
	}
}
