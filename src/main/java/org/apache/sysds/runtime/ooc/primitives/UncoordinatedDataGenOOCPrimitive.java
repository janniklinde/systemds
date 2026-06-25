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

package org.apache.sysds.runtime.ooc.primitives;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

public class UncoordinatedDataGenOOCPrimitive extends PlannableOOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _outputStream;
	private final StreamContext _sc;
	private final long _bulkAlloc;
	private final AtomicLong _remainingBudget = new AtomicLong();
	private final AtomicBoolean _finished = new AtomicBoolean(false);
	private final AtomicInteger _pendingEmits = new AtomicInteger(1);
	private LongConsumer _bulkProducer;
	private OOCStream<IndexedMatrixValue> _out;
	private boolean _shutdown;

	public UncoordinatedDataGenOOCPrimitive(OOCStreamable<IndexedMatrixValue> outputStream, long bulkAlloc,
		StreamContext sc) {
		super(Collections.emptyList());
		_outputStream = outputStream;
		_sc = sc;
		_bulkAlloc = bulkAlloc;
		_shutdown = false;
	}

	public void setProducer(LongConsumer bulkProducer) {
		_bulkProducer = bulkProducer;
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return Collections.emptyList();
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStream);
	}

	@Override
	public boolean isEmissionControlled() {
		return true;
	}

	@Override
	public boolean isTileLocal() {
		return true;
	}

	@Override
	public long getDenseTileMemoryFactor() {
		return 1;
	}

	@Override
	public void inferPatterns() {
		if(_pattern == OOCAccessPattern.UNSET)
			_pattern = OOCAccessPattern.ANY;
		inferPatterns(getParents());
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		_pattern = accessPattern;
	}

	@Override
	public void startExecution() {
		_out = _outputStream.getWriteStream();
		final long minAlloc = _allocFn.applyAsLong(new MatrixIndexes(1, 1));
		final long targetAlloc = Math.max(_bulkAlloc, minAlloc);

		runCoordinator("ooc-uncoordinated-datagen", OOCInstructionUtils.oocTask(() -> {
			while(!_shutdown) {
				long allow = targetAlloc;
				if(_startsRegion)
					allow = topUpBudget(targetAlloc, minAlloc);
				_bulkProducer.accept(allow);
			}
			if(_startsRegion) {
				long remaining = _remainingBudget.getAndSet(0);
				if(remaining < 0)
					throw new IllegalArgumentException("Negative remaining budget: " + remaining);
				_allowance.release(remaining);
			}
			producerFinished();
		}, new CompletableFuture<>(), _sc));
	}

	public void emit(IndexedMatrixValue imv) {
		long newMem = _allocFn.applyAsLong(imv.getIndexes());
		if(!_startsRegion) {
			forward(imv, 0);
			return;
		}
		long deficit = consumeAvailableBudget(newMem);
		if(deficit == 0)
			forward(imv, newMem);
		else
			admitAsync(imv, newMem, deficit);
	}

	public void emit(IndexedMatrixValue imv, OOCIOHandler.SourceBlockDescriptor desc) {
		// Not yet properly supported
		emit(imv);
	}

	public void shutdown() {
		_shutdown = true;
	}

	private void finish() {
		if(_finished.compareAndSet(false, true)) {
			_out.closeInput();
			onComplete();
		}
	}

	private void producerFinished() {
		if(_pendingEmits.decrementAndGet() == 0)
			finish();
	}

	private long topUpBudget(long targetAlloc, long minAlloc) {
		while(true) {
			long current = _remainingBudget.get();
			if(current >= targetAlloc)
				return targetAlloc;
			if(current >= minAlloc) {
				long extra = targetAlloc - current;
				if(extra > 0 && _allowance.tryReserve(extra)) {
					if(_remainingBudget.compareAndSet(current, targetAlloc))
						return targetAlloc;
					_allowance.release(extra);
					continue;
				}
				return current;
			}
			long delta = minAlloc - current;
			_allowance.reserveBlocking(delta);
			if(_remainingBudget.compareAndSet(current, minAlloc))
				return minAlloc;
			_allowance.release(delta);
		}
	}

	private long consumeAvailableBudget(long bytes) {
		if(bytes < 0)
			throw new IllegalArgumentException("Cannot consume negative bytes: " + bytes);
		while(true) {
			long current = _remainingBudget.get();
			if(current <= 0)
				return bytes;
			long consumed = Math.min(current, bytes);
			if(_remainingBudget.compareAndSet(current, current - consumed))
				return bytes - consumed;
		}
	}

	private void admitAsync(IndexedMatrixValue imv, long bytes, long deficit) {
		long consumedBytes = bytes - deficit;
		_pendingEmits.incrementAndGet();
		OOCFuture<Void> reservation;
		try {
			reservation = _allowance.reserveAsync(deficit);
		}
		catch(Throwable t) {
			if(consumedBytes > 0)
				_allowance.release(consumedBytes);
			fail(t);
			producerFinished();
			return;
		}
		reservation.whenComplete((ignored, error) -> {
			try {
				if(error != null) {
					if(consumedBytes > 0)
						_allowance.release(consumedBytes);
					fail(error);
					return;
				}
				forward(imv, bytes);
			}
			catch(Throwable t) {
				fail(t);
			}
			finally {
				producerFinished();
			}
		});
	}

	private void forward(IndexedMatrixValue imv, long bytes) {
		if(_crossBoundaries) {
			InMemoryQueueCallback cb = new InMemoryQueueCallback(imv, null, _allowance, bytes);
			boolean handedOff = false;
			try {
				_out.enqueue(cb);
				handedOff = true;
			}
			finally {
				if(!handedOff)
					cb.close();
			}
			return;
		}
		_out.enqueue(imv);
	}

	private void fail(Throwable t) {
		_shutdown = true;
		_out.propagateFailure(DMLRuntimeException.of(t));
	}
}
