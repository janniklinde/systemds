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

import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.PlaybackStream;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;

import java.util.List;

public class PlannablePlaybackOOCPrimitive extends PlannableOOCPrimitive {
	private final CachingStream _cache;
	private final PlaybackStream _outputStream;

	public PlannablePlaybackOOCPrimitive(CachingStream cache, PlaybackStream outputStream) {
		super(List.of(cache.getPrimitive()));
		_cache = cache;
		_outputStream = outputStream;
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		return List.of(_cache);
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStream);
	}

	@Override
	public boolean isMaterializationBoundary() {
		return true;
	}

	@Override
	public void startExecution() {
		onComplete();
	}

	@Override
	public void inferPatterns() {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		_pattern = accessPattern;
	}
}
