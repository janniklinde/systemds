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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.packed.PackedBlock;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public final class PartitionedOOCStreamMaterializer implements Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> {
	private final MaterializedStore<PackedBlock> _store;
	private final MemoryAllowance _allowance;
	private final DataCharacteristics _characteristics;
	private final long _targetBytes;
	private final OOCFuture<Void> _completion = new OOCFuture<>();
	private final List<IndexedMatrixValue> _values = new ArrayList<>();
	private final List<ReservationBudget> _ownership = new ArrayList<>();
	private final ScheduledExecutorService _timer;
	private long _bytes;
	private int _partition;
	private boolean _done;

	public PartitionedOOCStreamMaterializer(MaterializedStore<PackedBlock> store, MemoryAllowance allowance,
		DataCharacteristics characteristics, long targetBytes) {
		if(targetBytes <= 0)
			throw new IllegalArgumentException("Partition target must be positive");
		_store = store;
		_allowance = allowance;
		_characteristics = characteristics;
		_targetBytes = targetBytes;
		_timer = Executors.newSingleThreadScheduledExecutor(r -> {
			Thread thread = new Thread(r, "ooc-partition-flush");
			thread.setDaemon(true);
			return thread;
		});
		_timer.scheduleWithFixedDelay(() -> {
			synchronized(this) {
				if(!_done) {
					try {
						flush();
					}
					catch(Throwable error) {
						fail(error);
					}
				}
			}
		}, 100, 100, TimeUnit.MILLISECONDS);
	}

	public void attach(OOCStream<IndexedMatrixValue> source) {
		source.setSubscriber(this);
	}

	public OOCFuture<Void> completion() {
		return _completion;
	}

	@Override
	public synchronized void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		try(callback) {
			if(_done)
				return;
			if(callback.isFailure()) {
				callback.get();
				throw new DMLRuntimeException("Source partition materialization failed");
			}
			if(callback.isEos()) {
				flush();
				_done = true;
				_timer.shutdown();
				_store.complete(_characteristics);
				_completion.complete(null);
				return;
			}
			if(!(callback instanceof SourceBackedGroupCallback source))
				throw new DMLRuntimeException("Partition materialization requires source-owned input");
			SourceBackedGroupCallback.SourceGroup group = source.take();
			_ownership.add(group.ownership());
			for(IndexedMatrixValue value : group.values()) {
				_values.add(value);
				_bytes += OOCUtils.memoryCharge(value);
			}
			if(_bytes >= _targetBytes)
				flush();
		}
		catch(Throwable error) {
			fail(error);
		}
	}

	private void flush() {
		if(_values.isEmpty())
			return;
		long[] sizes = new long[_values.size()];
		for(int i = 0; i < sizes.length; i++)
			sizes[i] = OOCUtils.memoryCharge(_values.get(i));
		PackedBlock pack = PackedBlock.fromValues(_values.toArray(), sizes);
		_allowance.reserveBlocking(pack.size());
		try(StoreLease<PackedBlock> lease = _store.publishPinnedUnpackedLive(_partition++, pack, pack.size(),
			_allowance)) {
			for(ReservationBudget owner : _ownership)
				owner.close();
			_ownership.clear();
			_values.clear();
			_bytes = 0;
		}
	}

	private void fail(Throwable error) {
		if(_done)
			return;
		_done = true;
		_timer.shutdown();
		for(ReservationBudget owner : _ownership)
			owner.close();
		_ownership.clear();
		_values.clear();
		_store.failMaterialization(error);
		_completion.completeExceptionally(error);
	}
}
