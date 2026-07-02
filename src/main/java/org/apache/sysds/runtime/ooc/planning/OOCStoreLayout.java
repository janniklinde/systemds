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

import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

/**
 * Bidirectional logical tile layout for a materialized store. Publication uses
 * {@link #linearize(T)} as the physical cache key; eviction policy callbacks receive that
 * physical key and use {@link #delinearize(int)} to score in logical tile coordinates.
 */
public interface OOCStoreLayout<T> extends ToIntFunction<T> {
	int linearize(T t);

	T delinearize(int index);

	@Override
	default int applyAsInt(T t) {
		return linearize(t);
	}

	static <T> OOCStoreLayout<T> of(ToIntFunction<T> linearize, IntFunction<T> delinearize) {
		if(linearize == null || delinearize == null)
			throw new IllegalArgumentException("Store layout requires both linearize and delinearize functions.");
		return new OOCStoreLayout<T>() {
			@Override
			public int linearize(T t) {
				return linearize.applyAsInt(t);
			}

			@Override
			public T delinearize(int index) {
				return delinearize.apply(index);
			}
		};
	}
}
