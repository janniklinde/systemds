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

public class SegmentedStreamTableList<T> {
	private static final VarHandle SEGMENTS;
	private static final VarHandle ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
	private static final int DEFAULT_SEGMENT_SIZE = 64;

	static {
		try {
			SEGMENTS = MethodHandles.lookup().findVarHandle(SegmentedStreamTableList.class,
				"_segments", Object[].class);
		}
		catch(ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	private final int _segmentSize;
	private final int _segmentBits;
	private final int _segmentMask;
	private final int _streamPartitionSize;

	private volatile Object[] _segments;

	public SegmentedStreamTableList() {
		this(DEFAULT_SEGMENT_SIZE);
	}

	public SegmentedStreamTableList(int segmentSize) {
		this(segmentSize, 1024);
	}

	public SegmentedStreamTableList(int segmentSize, int streamPartitionSize) {
		_segmentSize = segmentSize;
		_segmentBits = Integer.numberOfTrailingZeros(segmentSize);
		_segmentMask = segmentSize - 1;
		_streamPartitionSize = streamPartitionSize;
		_segments = new Object[1];
	}

	public MaskedOnceArrayList<T> get(int streamId) {
		Object[] segments = (Object[]) SEGMENTS.getAcquire(this);
		int segmentIndex = segmentIndex(streamId);
		if(segmentIndex >= segments.length)
			return null;

		Object[] segment = (Object[]) ARRAY.getAcquire(segments, segmentIndex);
		if(segment == null)
			return null;

		@SuppressWarnings("unchecked")
		MaskedOnceArrayList<T> streamTable =
			(MaskedOnceArrayList<T>) ARRAY.getAcquire(segment, offsetInSegment(streamId));
		return streamTable;
	}

	public MaskedOnceArrayList<T> get(long streamId) {
		return get((int)streamId);
	}

	public MaskedOnceArrayList<T> getOrCreate(int streamId) {
		int segmentIndex = segmentIndex(streamId);
		int offset = offsetInSegment(streamId);

		while(true) {
			Object[] segments = ensureOuterCapacity(segmentIndex + 1);
			Object[] segment = (Object[]) ARRAY.getAcquire(segments, segmentIndex);
			if(segment == null) {
				Object[] newSegment = new Object[_segmentSize];
				if(!ARRAY.compareAndSet(segments, segmentIndex, null, newSegment))
					continue;
				segment = newSegment;
			}

			@SuppressWarnings("unchecked")
			MaskedOnceArrayList<T> streamTable =
				(MaskedOnceArrayList<T>) ARRAY.getAcquire(segment, offset);
			if(streamTable != null)
				return streamTable;

			MaskedOnceArrayList<T> newTable = new MaskedOnceArrayList<>(_streamPartitionSize);
			if(ARRAY.compareAndSet(segment, offset, null, newTable))
				return newTable;
		}
	}

	public MaskedOnceArrayList<T> getOrCreate(long streamId) {
		return getOrCreate((int)streamId);
	}

	public int capacity() {
		Object[] segments = (Object[]) SEGMENTS.getAcquire(this);
		return segments.length * _segmentSize;
	}

	private Object[] ensureOuterCapacity(int minLength) {
		Object[] segments = (Object[]) SEGMENTS.getAcquire(this);
		while(minLength > segments.length) {
			int newLength = segments.length;
			while(newLength < minLength) {
				if(newLength > Integer.MAX_VALUE / 2)
					throw new IllegalStateException("SegmentedStreamTableList capacity overflow");
				newLength <<= 1;
			}

			Object[] bigger = new Object[newLength];
			System.arraycopy(segments, 0, bigger, 0, segments.length);
			if(SEGMENTS.compareAndSet(this, segments, bigger))
				return bigger;
			segments = (Object[]) SEGMENTS.getAcquire(this);
		}
		return segments;
	}

	private int segmentIndex(int streamId) {
		return streamId >>> _segmentBits;
	}

	private int offsetInSegment(int streamId) {
		return streamId & _segmentMask;
	}
}
