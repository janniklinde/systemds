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

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCIOHandler;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;

public class UncoordinatedDataGenOOCPrimitive extends PlannableOOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _outputStream;
	private final StreamContext _sc;
	private final int _bulkAlloc;
	private final LongAdder _spentCtr = new LongAdder();
	private LongConsumer _bulkProducer;
	private OOCStream<IndexedMatrixValue> _out;
	private boolean _shutdown;

	public UncoordinatedDataGenOOCPrimitive(OOCStreamable<IndexedMatrixValue> outputStream, int bulkAlloc,
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
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		_pattern = accessPattern;
	}

	@Override
	public void startExecution() {
		final long baseAlloc = _allocFn.applyAsLong(new MatrixIndexes(1, 1)) * _bulkAlloc;
		_out = _outputStream.getWriteStream();

		new Thread(() -> {
			long allow = 0;
			while(!_shutdown) {
				allow -= _spentCtr.sumThenReset();
				if(allow < baseAlloc)
					_allowance.reserveBlocking(baseAlloc - allow);
				allow = baseAlloc;
				_bulkProducer.accept(allow);
			}
			allow -= _spentCtr.sumThenReset();
			_allowance.release(allow);
			_out.closeInput();
			onComplete();
		}).start();
	}

	public void emit(IndexedMatrixValue imv) {
		long newMem = _allocFn.applyAsLong(imv.getIndexes());
		_spentCtr.add(newMem);

		if(_crossBoundaries) {
			_out.enqueue(new InMemoryQueueCallback(imv, null, _allowance, newMem));
			return;
		}
		_out.enqueue(imv);
	}

	public void emit(IndexedMatrixValue imv, OOCIOHandler.SourceBlockDescriptor desc) {
		// Not yet properly supported
		emit(imv);
	}

	public void shutdown() {
		_shutdown = true;
	}
}
