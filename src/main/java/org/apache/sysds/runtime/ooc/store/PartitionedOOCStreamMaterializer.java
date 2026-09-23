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

import java.util.List;
import java.util.function.Consumer;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.packed.PackedBlock;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public final class PartitionedOOCStreamMaterializer implements Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> {
	private final MaterializedStore<PackedBlock> _store;
	private final DataCharacteristics _characteristics;
	private final OOCFuture<Void> _completion = new OOCFuture<>();
	private final Consumer<StoreLease<PackedBlock>> _liveConsumer;
	private int _partition;
	private boolean _done;

	public PartitionedOOCStreamMaterializer(MaterializedStore<PackedBlock> store,
		DataCharacteristics characteristics, Consumer<StoreLease<PackedBlock>> liveConsumer) {
		_store = store;
		_characteristics = characteristics;
		_liveConsumer = liveConsumer;
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
				_done = true;
				_store.complete(_characteristics);
				_completion.complete(null);
				return;
			}
			if(!(callback instanceof SourceBackedGroupCallback source))
				throw new DMLRuntimeException("Partition materialization requires source-owned input");
			SourceBackedGroupCallback.SourceGroup group = source.take();
			publish(group);
		}
		catch(Throwable error) {
			fail(error);
		}
	}

	private void publish(SourceBackedGroupCallback.SourceGroup group) {
		List<IndexedMatrixValue> values = group.values();
		ReservationBudget ownership = group.ownership();
		try {
			long[] sizes = new long[values.size()];
			for(int i = 0; i < sizes.length; i++)
				sizes[i] = OOCUtils.memoryCharge(values.get(i));
			PackedBlock pack = PackedBlock.fromValues(values.toArray(), sizes);
			ownership.reserveBlocking(pack.size());
			StoreLease<PackedBlock> lease = _store.publishPinnedUnpackedLive(_partition++, pack, pack.size(),
				ownership);
			OOCIOHandler.SourceBlockDescriptor descriptor = group.descriptor();
			if(!(descriptor instanceof OOCIOHandler.GroupSourceBlockDescriptor))
				descriptor = new OOCIOHandler.GroupSourceBlockDescriptor(descriptor.path, descriptor.format,
					descriptor.indexes, descriptor.offset, descriptor.recordLength, descriptor.serializedSize,
					1);
			_store.cache().getIOHandler().registerSourceLocation(lease.entry().getKey(), descriptor);
			_store.cache().markBacked(lease.entry());
			try(lease) {
				if(_liveConsumer != null) {
					StoreLease<PackedBlock> live = lease.retain();
					try {
						_liveConsumer.accept(live);
					}
					catch(Throwable error) {
						live.close();
						throw error;
					}
				}
			}
		}
		finally {
			ownership.close();
		}
	}

	private void fail(Throwable error) {
		if(_done)
			return;
		_done = true;
		_store.failMaterialization(error);
		_completion.completeExceptionally(error);
	}
}
