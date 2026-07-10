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
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.io.OOCIOHandler;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

public class UncoordinatedDataGenOOCPrimitive extends PlannableOOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _outputStream;
	private final StreamContext _sc;
	private final long _bulkAlloc;
	private final AtomicBoolean _finished = new AtomicBoolean(false);
	private final AtomicInteger _pendingEmits = new AtomicInteger(1);
	private final AtomicReference<ReservationBudget> _activeBudget = new AtomicReference<>();
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
		final long targetAlloc = Math.max(0, _bulkAlloc);
		final DataCharacteristics dc = _outputStream.getDataCharacteristics();
		final long tileBytes = OOCInstructionUtils.estimateOutputTileBytes(dc);
		final long batchBudgetBytes = saturatingAdd(targetAlloc, tileBytes);
		final long totalBudgetBytes = dc != null && dc.dimsKnown() ?
			saturatingMultiply(tileBytes, OOCUtils.getNumBlocks(dc)) : 0;
		final long taskBudgetBytes = totalBudgetBytes > 0 ? Math.min(batchBudgetBytes, totalBudgetBytes) :
			batchBudgetBytes;

		runCoordinator("ooc-uncoordinated-datagen", OOCInstructionUtils.oocTask(() -> {
			while(!_shutdown) {
				ReservationBudget budget = OOCInstructionUtils.reserveBudget(_allowance, taskBudgetBytes);
				if(budget == null)
					throw new DMLRuntimeException("Uncoordinated data generation requires a positive task budget.");
				if(!_activeBudget.compareAndSet(null, budget)) {
					budget.close();
					throw new IllegalStateException("Overlapping uncoordinated data generation phases.");
				}
				try {
					_bulkProducer.accept(targetAlloc);
				}
				finally {
					_activeBudget.compareAndSet(budget, null);
					budget.close();
				}
			}
			producerFinished();
		}, new CompletableFuture<>(), _sc));
	}

	public void emit(IndexedMatrixValue imv) {
		forward(imv);
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

	private void forward(IndexedMatrixValue imv) {
		try {
			ReservationBudget budget = _activeBudget.get();
			if(budget == null)
				throw new DMLRuntimeException("Data block emitted outside an admitted producer phase.");
			OOCInstructionUtils.enqueueExact(_out, imv, budget, false);
		}
		catch(Throwable t) {
			fail(t);
		}
	}

	private void fail(Throwable t) {
		_shutdown = true;
		_out.propagateFailure(DMLRuntimeException.of(t));
	}

	private static long saturatingAdd(long a, long b) {
		long result = a + b;
		return result < 0 ? Long.MAX_VALUE : result;
	}

	private static long saturatingMultiply(long a, long b) {
		if(a <= 0 || b <= 0)
			return 0;
		return a > Long.MAX_VALUE / b ? Long.MAX_VALUE : a * b;
	}
}
