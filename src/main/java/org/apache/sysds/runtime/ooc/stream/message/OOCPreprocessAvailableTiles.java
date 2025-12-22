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

import java.util.ArrayList;
import java.util.function.BiFunction;

public class OOCPreprocessAvailableTiles implements OOCStreamMessage {
	private ArrayList<BiFunction<Boolean, IndexRange, IndexRange>> _ixTransformations;

	public OOCPreprocessAvailableTiles() {
		_ixTransformations = null;
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.REQUEST_RANGE;
	}

	@Override
	public void addIXTransform(BiFunction<Boolean, IndexRange, IndexRange> transform) {
		if(transform != null) {
			if (_ixTransformations == null)
				_ixTransformations = new ArrayList<>();
			_ixTransformations.add(transform);
		}
	}

	@Override
	public void popIXTransform() {
		_ixTransformations.remove(_ixTransformations.size() - 1);
	}

	public IndexRange transform(IndexRange range, boolean downstream) {
		for(BiFunction<Boolean, IndexRange, IndexRange> transform : _ixTransformations)
			range = transform.apply(downstream, range);
		return range;
	}
}
