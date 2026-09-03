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

package org.apache.sysds.runtime.instructions.ooc;

import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.SharedRowsOOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.StreamContext;

public final class SharedRowsStreamable implements OOCStreamable<IndexedMatrixValue> {
	private final OOCStreamable<IndexedMatrixValue> _input;
	private final int _maxOpenRows;
	private final StreamContext _context;
	private SharedRowsOOCPrimitive _primitive;
	private CacheableData<?> _data;
	private boolean _deleteScheduled;

	public SharedRowsStreamable(OOCStreamable<IndexedMatrixValue> input, CacheableData<?> data, int maxOpenRows,
		StreamContext context) {
		_input = input;
		_data = data;
		_maxOpenRows = maxOpenRows;
		_context = context;
		_input.reserveLazyHandle();
	}

	@Override
	public synchronized OOCStreamable<IndexedMatrixValue> claimConsumer() {
		if(_primitive == null || _primitive.hasStartedExecution())
			_primitive = new SharedRowsOOCPrimitive(_input, _maxOpenRows, _context);
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		output.setData(_data);
		_primitive.addOutput(output);
		return output;
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReadStream() {
		return claimConsumer().getReadStream();
	}

	@Override
	public OOCStream<IndexedMatrixValue> getWriteStream() {
		throw new UnsupportedOperationException("Shared rows expose consumer streams only.");
	}

	@Override
	public boolean hasStreamCache() {
		return false;
	}

	@Override
	public boolean hasMaterializedStore() {
		return true;
	}

	@Override
	public synchronized void scheduleMaterializedStoreDeletion() {
		if(!_deleteScheduled) {
			_deleteScheduled = true;
			_input.discardHandle();
		}
	}

	@Override
	public CachingStream getStreamCache() {
		return null;
	}

	@Override
	public boolean isProcessed() {
		return _primitive != null && _primitive.hasStartedExecution();
	}

	@Override
	public DataCharacteristics getDataCharacteristics() {
		return _data.getDataCharacteristics();
	}

	@Override
	public CacheableData<?> getData() {
		return _data;
	}

	@Override
	public void setData(CacheableData<?> data) {
		_data = data;
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return _primitive;
	}

}
