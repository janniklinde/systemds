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

package org.apache.sysds.runtime.ooc.memory;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

public class OOCMatrixStreamTask<T> implements OOCStreamTask<T, IndexedMatrixValue> {
	protected final MemoryAllowance _allowance;
	protected final long _reservedBytes;
	protected final OOCStream.QueueCallback<T> _input;
	protected boolean _open = true;
	protected OOCStream.QueueCallback<IndexedMatrixValue> _output;

	public OOCMatrixStreamTask(MemoryAllowance allowance, long reservedBytes, OOCStream.QueueCallback<T> input) {
		_allowance = allowance;
		_reservedBytes = reservedBytes;
		_input = input;
	}

	public T input() {
		return _input.get();
	}

	public void setOutput(IndexedMatrixValue v) {
		if(_output != null)
			throw new IllegalStateException();
		long delta = _reservedBytes - ((MatrixBlock)v.getValue()).getInMemorySize();
		if(delta > 0)
			_allowance.release(delta);
		else if(delta < 0)
			_allowance.reserveBlocking(-delta);
		// Transfer allowance ownership to callback
		_output = new InMemoryQueueCallback(v, null, _allowance, ((MatrixBlock)v.getValue()).getInMemorySize());
	}

	@Override
	public OOCStream.QueueCallback<IndexedMatrixValue> output() {
		return _output;
	}

	@Override
	public synchronized void close() {
		if(_open) {
			_open = false;
			_input.close();
		}
	}
}
