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

import java.util.ArrayList;
import java.util.List;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;

public final class OwnedGroupQueueCallback<T> implements OOCStream.GroupQueueCallback<T> {
	private final List<OOCStream.QueueCallback<T>> _items;
	private boolean _closed;

	public OwnedGroupQueueCallback(List<OOCStream.QueueCallback<T>> items) {
		if(items.isEmpty())
			throw new IllegalArgumentException("A callback group must contain at least one item");
		_items = new ArrayList<>(items);
	}

	@Override
	public int size() {
		return _items.size();
	}

	@Override
	public synchronized OOCStream.QueueCallback<T> getCallback(int index) {
		if(_closed)
			throw new IllegalStateException("Cannot open an item from a closed group callback");
		return _items.get(index).keepOpen();
	}

	@Override
	public T get() {
		throw new UnsupportedOperationException("A callback group holds several values");
	}

	@Override
	public synchronized OOCStream.QueueCallback<T> keepOpen() {
		if(_closed)
			throw new IllegalStateException("Cannot keep open a closed group callback");
		List<OOCStream.QueueCallback<T>> retained = new ArrayList<>(_items.size());
		for(OOCStream.QueueCallback<T> item : _items)
			retained.add(item.keepOpen());
		return new OwnedGroupQueueCallback<>(retained);
	}

	@Override
	public synchronized void close() {
		if(_closed)
			return;
		_closed = true;
		for(OOCStream.QueueCallback<T> item : _items)
			item.close();
	}

	@Override
	public void fail(DMLRuntimeException failure) {
		for(OOCStream.QueueCallback<T> item : _items)
			item.fail(failure);
	}

	@Override
	public boolean isEos() {
		return false;
	}

	@Override
	public boolean isFailure() {
		return _items.get(0).isFailure();
	}
}
