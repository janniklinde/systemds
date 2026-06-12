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

package org.apache.sysds.runtime.ooc.cache.packed;

interface PackedCacheLocation {
}

record PendingPackLocation(PackBuilder builder, int slot) implements PackedCacheLocation {
}

final class SealedPackLocation implements PackedCacheLocation {
	private final PackedPinState state;
	private final int slot;
	private int references;

	SealedPackLocation(PackedPinState state, int slot) {
		this(state, slot, 1);
	}

	SealedPackLocation(PackedPinState state, int slot, int references) {
		if(references <= 0)
			throw new IllegalArgumentException("Sealed location requires a positive reference count.");
		this.state = state;
		this.slot = slot;
		this.references = references;
	}

	PackedPinState state() {
		return state;
	}

	int slot() {
		return slot;
	}

	synchronized int retain() {
		if(references <= 0)
			throw new IllegalStateException("Cannot retain a forgotten packed location.");
		return ++references;
	}

	synchronized int release() {
		if(references <= 0) {
			//tolerated for legacy double-forget callers; assertion surfaces it in debug runs
			assert false : "Packed location slot " + slot + " dereferenced below zero.";
			return 0;
		}
		return --references;
	}
}

record PendingLogicalPin(PackBuilder builder, int slot) {
}

record PackedLogicalPin(SealedPackLocation location) {
}
