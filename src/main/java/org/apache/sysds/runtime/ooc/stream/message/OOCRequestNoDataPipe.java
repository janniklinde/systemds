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

package org.apache.sysds.runtime.ooc.stream.message;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.runtime.util.IndexRange;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class OOCRequestNoDataPipe implements OOCStreamMessage {
	private static final AtomicInteger CALLER_ID = new AtomicInteger(0);
	private final Consumer<IndexedMatrixValue> _consumer;
	private boolean _cancelled;
	private boolean _handled;
	private final AtomicBoolean _logged;

	public OOCRequestNoDataPipe(Consumer<IndexedMatrixValue> consumer) {
		_consumer = consumer;
		_cancelled = false;
		_handled = false;
		_logged = new AtomicBoolean(false);
	}

	public void emit(MatrixIndexes ix) {
		if(_cancelled)
			return;
		logEmitOnce();
		_consumer.accept(new IndexedMatrixValue(ix, null));
	}

	@Override
	public OOCStreamMessage split() {
		cancel();
		return this;
	}

	@Override
	public boolean isCancelled() {
		return _cancelled;
	}

	@Override
	public void cancel() {
		_cancelled = true;
	}

	@Override
	public boolean isHandled() {
		return _handled;
	}

	@Override
	public void markHandled() {
		_handled = true;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.REQUEST_NO_DATA_PIPE;
	}

	@Override
	public void addIXTransform(BiFunction<Boolean, IndexRange, IndexRange> transform) {
		if(transform != null)
			_cancelled = true;
	}

	private void logEmitOnce() {
		if (!DMLScript.OOC_LOG_EVENTS || !_logged.compareAndSet(false, true))
			return;

		int id = CALLER_ID.get();
		if (id == 0) {
			int newId = OOCEventLog.registerCaller("OOCNoDataPipe");
			if (!CALLER_ID.compareAndSet(0, newId))
				newId = CALLER_ID.get();
			id = newId;
		}

		OOCEventLog.onNoDataEnumerateEvent(id, System.nanoTime(), 1);
	}
}
