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

package org.apache.sysds.runtime.ooc.cache;

import java.util.function.Consumer;

public class MaskedOnceArray<T> extends OnceArray<T> {
	protected final ConcurrentBitSet _liveState;

	public MaskedOnceArray(int length) {
		super(length);
		_liveState = new ConcurrentBitSet(length);
	}

	@Override
	public void put(int i, T value) {
		if(value == null) {
			clear(i);
			return;
		}
		super.put(i, value);
		_liveState.set(i);
	}

	@Override
	public void clear(int i) {
		super.clear(i);
		_liveState.clear(i);
	}

	public void setLive(int i) {
		_liveState.set(i);
	}

	public void clearLive(int i) {
		_liveState.clear(i);
	}

	public void forEachLive(Consumer<? super T> action, boolean reversed) {
		if(reversed)
			forEachLiveBackward(action);
		else
			forEachLiveForward(action);
	}

	private void forEachLiveForward(Consumer<? super T> action) {
		int len = _liveState.length();
		T data;
		for(int word = 0; word < len; word++) {
			if(_liveState.getWord(word) == 0)
				continue;
			int lower = word * 64;
			int upper = (word + 1) * 64;
			for(int i = lower; i < upper; i++) {
				data = get(i);
				if(data != null)
					action.accept(data);
			}
		}
	}

	private void forEachLiveBackward(Consumer<? super T> action) {
		int len = _liveState.length();
		for(int word = len-1; word >= 0; word--) {
			if(_liveState.getWord(word) == 0)
				continue;
			int lower = word * 64;
			int upper = (word + 1) * 64;
			T data;
			for(int i = upper-1; i >= lower; i--) {
				data = get(i);
				if(data != null)
					action.accept(data);
			}
		}
	}
}
