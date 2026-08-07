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

package org.apache.sysds.runtime.ooc.cache.io;

import org.apache.sysds.runtime.ooc.cache.BlockKey;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Used for keeping track of serialized object offsets and cache indices.
 * Can be used for read-ahead operations.
 */
final class BlockLayoutIndex {
	static final long NO_KEY = Long.MIN_VALUE;

	private static final VarHandle KEYS = MethodHandles.arrayElementVarHandle(long[].class);
	private static final int INITIAL_CAPACITY = 64;

	private volatile long[] _offsets;
	private volatile long[] _keys;
	private volatile int _count;

	BlockLayoutIndex() {
		_offsets = new long[INITIAL_CAPACITY + 1];
		_keys = new long[INITIAL_CAPACITY];
		_count = 0;
	}

	synchronized int append(long start, long end, long key) {
		int count = _count;
		if(end < start || (count > 0 && start < _offsets[count]))
			return -1;
		ensureCapacity(count + 1);
		_offsets[count] = start;
		_offsets[count + 1] = end;
		KEYS.setRelease(_keys, count, key);
		_count = count + 1;
		return count;
	}

	synchronized void setKey(int slot, long key) {
		if(slot >= 0 && slot < _count)
			KEYS.setRelease(_keys, slot, key);
	}

	int slotOf(long offset) {
		int high = _count - 1;
		long[] offsets = _offsets;
		int low = 0;
		while(low <= high) {
			int mid = (low + high) >>> 1;
			long value = offsets[mid];
			if(value < offset)
				low = mid + 1;
			else if(value > offset)
				high = mid - 1;
			else
				return mid;
		}
		return -1;
	}

	int count() {
		return _count;
	}

	long startAt(int slot) {
		return _offsets[slot];
	}

	long endAt(int slot) {
		return _offsets[slot + 1];
	}

	long keyAt(int slot) {
		return (long) KEYS.getAcquire(_keys, slot);
	}

	static long packKey(BlockKey key) {
		long streamId = key.getStreamId();
		long sequenceNumber = key.getSequenceNumber();
		if(streamId < 0 || streamId > Integer.MAX_VALUE || sequenceNumber < 0 || sequenceNumber > Integer.MAX_VALUE)
			throw new IllegalArgumentException("Block key is not packable: " + key);
		return (streamId << 32) | sequenceNumber;
	}

	static BlockKey unpackKey(long packed) {
		return new BlockKey(packed >>> 32, packed & 0xFFFFFFFFL);
	}

	private void ensureCapacity(int slots) {
		long[] keys = _keys;
		if(slots <= keys.length)
			return;
		int capacity = keys.length;
		while(capacity < slots)
			capacity <<= 1;
		long[] grownOffsets = new long[capacity + 1];
		long[] grownKeys = new long[capacity];
		System.arraycopy(_offsets, 0, grownOffsets, 0, _count + 1);
		System.arraycopy(keys, 0, grownKeys, 0, _count);
		_offsets = grownOffsets;
		_keys = grownKeys;
	}
}
