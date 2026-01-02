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

package org.apache.sysds.runtime.ooc.stream;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StreamContext {
	private Set<OOCStream<?>> _inStreams;
	private Set<OOCStream<?>> _outStreams;

	public boolean inStreamsDefined() {
		return _inStreams != null;
	}

	public boolean outStreamsDefined() {
		return _outStreams != null;
	}

	public void addInStream(OOCStream<?>... inStream) {
		if(_inStreams == null)
			_inStreams = ConcurrentHashMap.newKeySet();
		_inStreams.addAll(List.of(inStream));
	}

	public void addOutStream(OOCStream<?>... outStream) {
		if(outStream.length == 0 && _outStreams == null) {
			_outStreams = Collections.emptySet();
			return;
		}

		if(_outStreams == null || _outStreams.isEmpty())
			_outStreams = ConcurrentHashMap.newKeySet();
		_outStreams.addAll(List.of(outStream));
	}

	public Collection<OOCStream<?>> inStreams() {
		return _inStreams;
	}

	public Collection<OOCStream<?>> outStreams() {
		return _outStreams;
	}

	public void failAll(DMLRuntimeException e) {
		for(OOCStream<?> stream : _outStreams) {
			try {
				stream.propagateFailure(e);
			}
			catch(Throwable ignored) {}
		}

		for(OOCStream<?> stream : _inStreams) {
			try {
				stream.propagateFailure(e);
			}
			catch(Throwable ignored) {}
		}
	}
}
