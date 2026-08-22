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

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.cache.packed.PackedBlock;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

public final class OOCStreamMaterializer implements Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> {
	private final MaterializedStore<IndexedMatrixValue> _store;
	private final ToIntFunction<MatrixIndexes> _linearize;
	private final MemoryAllowance _allowance;
	private final List<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> _liveConsumers;
	private final OOCFuture<Void> _completion;
	private final AtomicBoolean _done;
	private final int _blocksize;

	private long _maxRowIndex;
	private long _maxColIndex;
	private int _rowsAtMaxRowIndex;
	private int _colsAtMaxColIndex;
	private int _widestBlockRows;
	private int _widestBlockCols;
	private long _nonZeros;

	public OOCStreamMaterializer(MaterializedStore<IndexedMatrixValue> store, ToIntFunction<MatrixIndexes> linearize,
		MemoryAllowance allowance) {
		this(store, linearize, allowance, List.of(), 0);
	}

	public OOCStreamMaterializer(MaterializedStore<IndexedMatrixValue> store, ToIntFunction<MatrixIndexes> linearize,
		MemoryAllowance allowance, List<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> liveConsumers) {
		this(store, linearize, allowance, liveConsumers, 0);
	}

	public OOCStreamMaterializer(MaterializedStore<IndexedMatrixValue> store, ToIntFunction<MatrixIndexes> linearize,
		MemoryAllowance allowance, List<Consumer<OOCStream.QueueCallback<IndexedMatrixValue>>> liveConsumers,
		int blocksize) {
		_store = store;
		_linearize = linearize;
		_allowance = allowance;
		_liveConsumers = List.copyOf(liveConsumers);
		_completion = new OOCFuture<>();
		_done = new AtomicBoolean(false);
		_blocksize = blocksize;
	}

	public static OOCStream.QueueCallback<IndexedMatrixValue> sourceBackedCallback(List<IndexedMatrixValue> values,
		OOCIOHandler.SourceBlockDescriptor descriptor, ReservationBudget ownership) {
		return new SourceBackedGroupCallback(values, descriptor, ownership);
	}

	public void attach(OOCStream<IndexedMatrixValue> source) {
		try {
			source.setSubscriber(this);
		}
		catch(Throwable failure) {
			DMLRuntimeException wrapped = DMLRuntimeException.of(failure);
			fail(wrapped);
			throw wrapped;
		}
	}

	public OOCFuture<Void> completion() {
		return _completion;
	}

	@Override
	public void accept(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		if(_done.get()) {
			callback.close();
			return;
		}
		try(callback) {
			if(callback instanceof SourceBackedGroupCallback sourceGroup) {
				publishSourceGroup(sourceGroup.take());
				return;
			}
			if(callback.isFailure()) {
				try {
					callback.get();
				}
				catch(DMLRuntimeException ex) {
					fail(ex);
				}
				return;
			}
			if(callback.isEos()) {
				finish();
				return;
			}
			if(callback instanceof OOCStream.GroupQueueCallback<?> grouped) {
				@SuppressWarnings("unchecked")
				OOCStream.GroupQueueCallback<IndexedMatrixValue> group = (OOCStream.GroupQueueCallback<IndexedMatrixValue>) grouped;
				for(int i = 0; i < group.size(); i++) {
					try(OOCStream.QueueCallback<IndexedMatrixValue> item = group.getCallback(i)) {
						publish(item);
					}
				}
			}
			else
				publish(callback);
		}
		catch(RuntimeException ex) {
			fail(DMLRuntimeException.of(ex));
		}
	}

	private void publishSourceGroup(SourceBackedGroupCallback.SourceGroup source) {
		ReservationBudget ownership = source.ownership();
		try {
			publishSourceGroup(source, ownership);
		}
		catch(RuntimeException | Error failure) {
			ownership.close();
			throw failure;
		}
	}

	private void publishSourceGroup(SourceBackedGroupCallback.SourceGroup source, ReservationBudget ownership) {
		List<IndexedMatrixValue> values = source.values();
		if(values.size() > 1 && (!(source.descriptor() instanceof OOCIOHandler.GroupSourceBlockDescriptor group) ||
			!group.packed || group.count != values.size()))
			throw new IllegalArgumentException("Source pack values do not match their physical resource descriptor.");
		long[] tileIds = new long[values.size()];
		Object[] packedValues = new Object[values.size()];
		long[] sizes = new long[values.size()];
		long totalBytes = 0;
		for(int i = 0; i < values.size(); i++) {
			IndexedMatrixValue value = values.get(i);
			observe(value);
			tileIds[i] = _linearize.applyAsInt(value.getIndexes());
			packedValues[i] = value;
			sizes[i] = OOCUtils.memoryCharge(value);
			totalBytes = Math.addExact(totalBytes, sizes[i]);
		}

		BlockEntry[] entries = null;
		List<StoreLease<IndexedMatrixValue>> leases = new ArrayList<>(values.size());
		try {
			ownership.reserveBlocking(totalBytes);
			OOCCache cache = _store.cache();
			BlockEntry physical;
			if(values.size() == 1) {
				physical = cache.putUnpackedPinned(_store.streamId(), tileIds[0], values.get(0), sizes[0], ownership);
				entries = new BlockEntry[] {physical};
			}
			else {
				if(!(cache instanceof OOCPackedCache packedCache))
					throw new IllegalStateException("Source packs require the packed OOC cache.");
				OOCPackedCache.PrepackedEntries packed = packedCache.putPrepackedPinned(_store.streamId(), tileIds,
					PackedBlock.fromValues(packedValues, sizes), ownership);
				physical = packed.physicalEntry();
				entries = packed.logicalEntries();
			}
			cache.getIOHandler().registerSourceLocation(physical.getKey(), source.descriptor());
			cache.markBacked(physical);
			for(int i = 0; i < entries.length; i++)
				leases.add(_store.publishPinnedEntryLive(Math.toIntExact(tileIds[i]), entries[i], ownership));
			ownership.close();

			for(int i = 0; i < leases.size(); i++) {
				int index = Math.toIntExact(tileIds[i]);
				try(StoreLease<IndexedMatrixValue> lease = leases.get(i)) {
					for(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> liveConsumer : _liveConsumers) {
						try(OOCStream.QueueCallback<IndexedMatrixValue> alias = new MaterializedCallback<>(
							lease.retain(), index)) {
							liveConsumer.accept(alias);
						}
					}
				}
			}
		}
		catch(RuntimeException | Error failure) {
			int adopted = leases.size();
			for(StoreLease<IndexedMatrixValue> lease : leases)
				lease.close();
			if(entries != null)
				for(int i = adopted; i < entries.length; i++)
					_store.cache().unpin(entries[i], ownership);
			ownership.close();
			throw failure;
		}
	}

	private void publish(OOCStream.QueueCallback<IndexedMatrixValue> callback) {
		IndexedMatrixValue value = callback.get();
		observe(value);
		int index = _linearize.applyAsInt(value.getIndexes());
		StoreLease<IndexedMatrixValue> lease;
		if(callback instanceof InMemoryQueueCallback<IndexedMatrixValue> managed && managed.getManagedBytes() > 0) {
			lease = _store.publishPinnedLive(index, managed.extractManagedPayload());
		}
		else {
			// Non-managed callbacks may retain another store, so materialization must establish independent ownership.
			MatrixBlock block = new MatrixBlock((MatrixBlock) value.getValue());
			IndexedMatrixValue copy = new IndexedMatrixValue(new MatrixIndexes(value.getIndexes()), block);
			long bytes = OOCUtils.memoryCharge(copy);
			_allowance.reserveBlocking(bytes);
			lease = _store.publishPinnedLive(index, copy, bytes, _allowance);
		}
		try(lease) {
			for(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> liveConsumer : _liveConsumers) {
				try(OOCStream.QueueCallback<IndexedMatrixValue> alias = new MaterializedCallback<>(lease.retain(),
					index)) {
					liveConsumer.accept(alias);
				}
			}
		}
	}

	private void finish() {
		if(!_done.compareAndSet(false, true))
			return;
		try {
			_store.complete(observedCharacteristics());
		}
		catch(RuntimeException ex) {
			_store.failMaterialization(ex);
			deliverEos(DMLRuntimeException.of(ex));
			_completion.completeExceptionally(ex);
			return;
		}
		deliverEos(null);
		_completion.complete(null);
	}

	private void fail(DMLRuntimeException failure) {
		if(!_done.compareAndSet(false, true))
			return;
		_store.failMaterialization(failure);
		deliverEos(failure);
		_completion.completeExceptionally(failure);
	}

	private void deliverEos(DMLRuntimeException failure) {
		for(Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> liveConsumer : _liveConsumers) {
			try {
				liveConsumer.accept(OOCStream.eos(failure));
			}
			catch(RuntimeException ignored) {
			}
		}
	}

	private synchronized void observe(IndexedMatrixValue value) {
		MatrixIndexes indexes = value.getIndexes();
		MatrixBlock block = (MatrixBlock) value.getValue();
		if(indexes.getRowIndex() > _maxRowIndex) {
			_maxRowIndex = indexes.getRowIndex();
			_rowsAtMaxRowIndex = block.getNumRows();
		}
		if(indexes.getColumnIndex() > _maxColIndex) {
			_maxColIndex = indexes.getColumnIndex();
			_colsAtMaxColIndex = block.getNumColumns();
		}
		_widestBlockRows = Math.max(_widestBlockRows, block.getNumRows());
		_widestBlockCols = Math.max(_widestBlockCols, block.getNumColumns());
		_nonZeros += block.getNonZeros();
	}

	/**
	 * Derives the dimensions of the materialized stream from the blocks it carried. Only valid once the stream is
	 * closed, because blocks arrive in an arbitrary order and the boundary blocks are not known before that.
	 *
	 * @return observed dimensions, or null if the block size could not be established
	 */
	private synchronized DataCharacteristics observedCharacteristics() {
		if(_maxRowIndex == 0 || _maxColIndex == 0)
			return new MatrixCharacteristics(0, 0, Math.max(_blocksize, 1), 0);
		int blocksize = _blocksize > 0 ? _blocksize : Math.max(_widestBlockRows, _widestBlockCols);
		if(blocksize <= 0)
			return null;
		long rows = (_maxRowIndex - 1) * blocksize + _rowsAtMaxRowIndex;
		long cols = (_maxColIndex - 1) * blocksize + _colsAtMaxColIndex;
		return new MatrixCharacteristics(rows, cols, blocksize, _nonZeros);
	}
}
