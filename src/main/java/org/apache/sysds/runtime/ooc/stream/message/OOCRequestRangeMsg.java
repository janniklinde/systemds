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

import org.apache.sysds.runtime.util.IndexRange;

import java.util.function.BiFunction;

public class OOCRequestRangeMsg implements OOCStreamMessage {
	private final double _priority;
	private IndexRange _range;

	public OOCRequestRangeMsg(IndexRange range, double priority) {
		_priority = priority;
		_range = range;
	}

	public double getPriority() {
		return _priority;
	}

	public IndexRange getTransformedRange() {
		return _range;
	}

	@Override
	public MessageType getMessageType() {
		return null;
	}

	@Override
	public void addIXTransform(BiFunction<Boolean, IndexRange, IndexRange> transform) {
		if (transform != null)
			_range = transform.apply(false, _range);
	}
}
