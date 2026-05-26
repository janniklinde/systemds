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

public class MaskedOnceArrayList<T> {
	private static final VarHandle PARTITIONS;
	private static final int DEFAULT_PARTITION_SIZE = 1024;

	static {
		try {
			PARTITIONS = MethodHandles.lookup().findVarHandle(MaskedOnceArrayList.class,
				"_partitions", MaskedOnceArray[].class);
		}
		catch(ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	private final int _partitionSize;
	private final int _partitionBits;
	private final int _partitionMask;

	private volatile MaskedOnceArray[] _partitions;

	public MaskedOnceArrayList() {
		this(DEFAULT_PARTITION_SIZE);
	}

	public MaskedOnceArrayList(int partitionSize) {
		validatePartitionSize(partitionSize);
		_partitionSize = partitionSize;
		_partitionBits = Integer.numberOfTrailingZeros(partitionSize);
		_partitionMask = partitionSize - 1;
		_partitions = newPartitions(1);
	}

	public void put(int i, T value) {
		checkIndex(i);
		partitionAt(partitionIndex(i)).put(offsetInPartition(i), value);
	}

	@SuppressWarnings("rawtypes")
	public void clear(int i) {
		checkIndex(i);
		int partition = partitionIndex(i);
		MaskedOnceArray[] partitions = (MaskedOnceArray[]) PARTITIONS.getAcquire(this);
		if(partition < partitions.length)
			partitions[partition].clear(offsetInPartition(i));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public T get(int i) {
		checkIndex(i);
		int partition = partitionIndex(i);
		MaskedOnceArray[] partitions = (MaskedOnceArray[]) PARTITIONS.getAcquire(this);
		if(partition >= partitions.length)
			return null;
		return (T) partitions[partition].get(offsetInPartition(i));
	}

	public void setLive(int i) {
		checkIndex(i);
		partitionAt(partitionIndex(i)).setLive(offsetInPartition(i));
	}

	@SuppressWarnings("rawtypes")
	public void clearLive(int i) {
		checkIndex(i);
		int partition = partitionIndex(i);
		MaskedOnceArray[] partitions = (MaskedOnceArray[]) PARTITIONS.getAcquire(this);
		if(partition < partitions.length)
			partitions[partition].clearLive(offsetInPartition(i));
	}

	@SuppressWarnings("rawtypes")
	public int capacity() {
		MaskedOnceArray[] partitions = (MaskedOnceArray[]) PARTITIONS.getAcquire(this);
		return partitions.length * _partitionSize;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public void forEachLive(Consumer<? super T> action, boolean reversed) {
		MaskedOnceArray[] partitions = (MaskedOnceArray[]) PARTITIONS.getAcquire(this);
		if(reversed) {
			for(int i = partitions.length - 1; i >= 0; i--)
				partitions[i].forEachLive(action, true);
		}
		else {
			for(MaskedOnceArray partition : partitions)
				partition.forEachLive(action, false);
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public void forEachVisible(Consumer<? super T> action) {
		MaskedOnceArray[] partitions = (MaskedOnceArray[]) PARTITIONS.getAcquire(this);
		for(MaskedOnceArray partition : partitions)
			partition.forEachVisible(action);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private MaskedOnceArray<T> partitionAt(int partitionIndex) {
		MaskedOnceArray[] partitions = (MaskedOnceArray[]) PARTITIONS.getAcquire(this);
		while(partitionIndex >= partitions.length) {
			MaskedOnceArray[] bigger = growPartitions(partitions, partitionIndex + 1);
			if(PARTITIONS.compareAndSet(this, partitions, bigger))
				partitions = bigger;
			else
				partitions = (MaskedOnceArray[]) PARTITIONS.getAcquire(this);
		}
		return (MaskedOnceArray<T>) partitions[partitionIndex];
	}

	@SuppressWarnings("rawtypes")
	private MaskedOnceArray[] growPartitions(MaskedOnceArray[] partitions, int minLength) {
		int newLength = partitions.length;
		while(newLength < minLength) {
			if(newLength > Integer.MAX_VALUE / 2)
				throw new IllegalStateException("MaskedOnceArrayList capacity overflow");
			newLength <<= 1;
		}

		MaskedOnceArray[] bigger = new MaskedOnceArray[newLength];
		System.arraycopy(partitions, 0, bigger, 0, partitions.length);
		for(int i = partitions.length; i < newLength; i++)
			bigger[i] = new MaskedOnceArray<>(_partitionSize);
		return bigger;
	}

	@SuppressWarnings("rawtypes")
	private MaskedOnceArray[] newPartitions(int length) {
		MaskedOnceArray[] partitions = new MaskedOnceArray[length];
		for(int i = 0; i < length; i++)
			partitions[i] = new MaskedOnceArray<>(_partitionSize);
		return partitions;
	}

	private int partitionIndex(int index) {
		return index >>> _partitionBits;
	}

	private int offsetInPartition(int index) {
		return index & _partitionMask;
	}

	private static void validatePartitionSize(int partitionSize) {
		if(partitionSize < 64 || (partitionSize & (partitionSize - 1)) != 0) {
			throw new IllegalArgumentException(
				"partitionSize must be a power of two and at least 64: " + partitionSize);
		}
	}

	private static void checkIndex(int i) {
		if(i < 0)
			throw new IndexOutOfBoundsException("Negative index: " + i);
	}
}
