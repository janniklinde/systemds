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

package org.apache.sysds.runtime.ooc.planning;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.store.OOCMaterializedView;
import org.apache.sysds.runtime.ooc.stream.message.OOCStreamMessage;
import org.apache.sysds.runtime.util.IndexRange;

/**
 * Planner-installed input wrapper: metadata and graph identity still come from the original source,
 * while consumers access the materialized representation through {@link #materializedView()}.
 */
public final class MaterializedInputStreamable implements OOCStreamable<IndexedMatrixValue> {
	private final OOCStreamable<IndexedMatrixValue> _source;
	private final OOCMaterializedView _view;

	public MaterializedInputStreamable(OOCStreamable<IndexedMatrixValue> source, OOCMaterializedView view) {
		_source = source;
		_view = view;
	}

	@Override
	public OOCStream<IndexedMatrixValue> getReadStream() {
		throw new UnsupportedOperationException("Use materializedView() for planner-materialized inputs.");
	}

	@Override
	public OOCStream<IndexedMatrixValue> getWriteStream() {
		return _source.getWriteStream();
	}

	@Override
	public boolean hasStreamCache() {
		return _source.hasStreamCache();
	}

	@Override
	public CachingStream getStreamCache() {
		return _source.getStreamCache();
	}

	@Override
	public boolean hasMaterializedStore() {
		return true;
	}

	@Override
	public boolean hasMaterializedView() {
		return true;
	}

	@Override
	public OOCMaterializedView materializedView() {
		return _view;
	}

	@Override
	public boolean isProcessed() {
		return _source.isProcessed();
	}

	@Override
	public DataCharacteristics getDataCharacteristics() {
		return _source.getDataCharacteristics();
	}

	@Override
	public CacheableData<?> getData() {
		return _source.getData();
	}

	@Override
	public void setData(CacheableData<?> data) {
		_source.setData(data);
	}

	@Override
	public void messageUpstream(OOCStreamMessage msg) {
		_source.messageUpstream(msg);
	}

	@Override
	public void messageDownstream(OOCStreamMessage msg) {
		_source.messageDownstream(msg);
	}

	@Override
	public void setUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		_source.setUpstreamMessageRelay(relay);
	}

	@Override
	public void setDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		_source.setDownstreamMessageRelay(relay);
	}

	@Override
	public void addUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		_source.addUpstreamMessageRelay(relay);
	}

	@Override
	public void addDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
		_source.addDownstreamMessageRelay(relay);
	}

	@Override
	public void clearUpstreamMessageRelays() {
		_source.clearUpstreamMessageRelays();
	}

	@Override
	public void clearDownstreamMessageRelays() {
		_source.clearDownstreamMessageRelays();
	}

	@Override
	public void setIXTransform(BiFunction<Boolean, IndexRange, IndexRange> transform) {
		_source.setIXTransform(transform);
	}

	@Override
	public BiFunction<Boolean, IndexRange, IndexRange> getIXTransform() {
		return _source.getIXTransform();
	}

	@Override
	public OOCPrimitive getPrimitive() {
		return _source.getPrimitive();
	}

	@Override
	public void assignPrimitive(OOCPrimitive primitive) {
		_source.assignPrimitive(primitive);
	}
}
