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

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.util.IndexRange;

import java.util.function.BiFunction;
import java.util.function.Consumer;

public class OOCRequestNoDataPipe implements OOCStreamMessage {
	private final Consumer<IndexedMatrixValue> _consumer;
	private boolean _cancelled;

	public OOCRequestNoDataPipe(Consumer<IndexedMatrixValue> consumer) {
		_consumer = consumer;
		_cancelled = false;
	}

	public void emit(MatrixIndexes ix) {
		if(_cancelled)
			return;
		_consumer.accept(new IndexedMatrixValue(ix, null));
	}

	@Override
	public OOCStreamMessage split() {
		_cancelled = true;
		return this;
	}

	@Override
	public boolean isCancelled() {
		return _cancelled;
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
}
