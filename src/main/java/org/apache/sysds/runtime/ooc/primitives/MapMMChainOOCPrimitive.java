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

import org.apache.sysds.lops.MapMultChain.ChainType;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCPrimitiveUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

import java.util.ArrayList;
import java.util.List;

public class MapMMChainOOCPrimitive extends PlannableOOCPrimitive {
	private final OOCStreamable<IndexedMatrixValue> _xStreamable;
	private final OOCStreamable<IndexedMatrixValue> _vStreamable;
	private final OOCStreamable<IndexedMatrixValue> _wStreamable;
	private final OOCStreamable<IndexedMatrixValue> _outputStreamable;
	private final ChainType _type;
	private final StreamContext _sc;
	private CachedAllowance _cache;

	private MapMMChainOOCPrimitive(List<OOCPrimitive> children, OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> wStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, ChainType type, StreamContext sc) {
		super(children);
		_xStreamable = xStreamable;
		_vStreamable = vStreamable;
		_wStreamable = wStreamable;
		_outputStreamable = outputStreamable;
		_type = type;
		_sc = sc;
	}

	public MapMMChainOOCPrimitive(OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> outputStreamable,
		ChainType type, StreamContext sc) {
		this(childrenOf(xStreamable, vStreamable, null), xStreamable, vStreamable, null, outputStreamable, type, sc);
	}

	public MapMMChainOOCPrimitive(OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> wStreamable,
		OOCStreamable<IndexedMatrixValue> outputStreamable, ChainType type, StreamContext sc) {
		this(childrenOf(xStreamable, vStreamable, wStreamable), xStreamable, vStreamable, wStreamable,
			outputStreamable, type, sc);
	}

	private static List<OOCPrimitive> childrenOf(OOCStreamable<IndexedMatrixValue> xStreamable,
		OOCStreamable<IndexedMatrixValue> vStreamable, OOCStreamable<IndexedMatrixValue> wStreamable) {
		ArrayList<OOCPrimitive> children = new ArrayList<>(3);
		addPrimitive(children, xStreamable);
		addPrimitive(children, vStreamable);
		addPrimitive(children, wStreamable);
		return children;
	}

	private static void addPrimitive(List<OOCPrimitive> children, OOCStreamable<?> streamable) {
		if(streamable == null)
			return;
		try {
			OOCPrimitive primitive = streamable.getPrimitive();
			if(primitive != null)
				children.add(primitive);
		}
		catch(RuntimeException ignored) {
		}
	}

	@Override
	public List<OOCStreamable<?>> getInputStreams() {
		ArrayList<OOCStreamable<?>> inputs = new ArrayList<>(3);
		inputs.add(_xStreamable);
		inputs.add(_vStreamable);
		if(_wStreamable != null)
			inputs.add(_wStreamable);
		return inputs;
	}

	@Override
	public List<OOCStreamable<?>> getOutputStreams() {
		return List.of(_outputStreamable);
	}

	@Override
	public boolean isMaterializationBoundary() {
		return true;
	}

	@Override
	public boolean requiresCache() {
		return true;
	}

	@Override
	public void bindCache(CachedAllowance cache) {
		_cache = cache;
	}

	@Override
	public void onComplete() {
		try {
			if(_cache != null)
				_cache.shutdown();
		}
		finally {
			super.onComplete();
		}
	}

	@Override
	public long getDenseTileMemoryFactor() {
		return 2;
	}

	@Override
	public void inferPatterns() {
		_pattern = OOCAccessPattern.ROW_MAJOR;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(OOCAccessPattern.ROW_MAJOR);
		getParents().forEach(OOCPrimitive::inferPatterns);
	}

	@Override
	public void requestPattern(OOCAccessPattern accessPattern) {
		if(_pattern == accessPattern)
			return;
		_pattern = accessPattern;
		for(OOCPrimitive child : getChildren())
			child.requestPattern(OOCAccessPattern.ROW_MAJOR);
	}

	@Override
	public void startExecution() {
		final OOCStream<IndexedMatrixValue> x = _xStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> v = _vStreamable.getReadStream();
		final OOCStream<IndexedMatrixValue> out = _outputStreamable.getWriteStream();
		final long rTiles = OOCUtils.getNumRowBlocks(v.getDataCharacteristics());
		final long nBroadcast = OOCUtils.getNumColBlocks(x.getDataCharacteristics());

		OOCPrimitiveUtils.collect(v, _cache, idx -> (int)(idx.getRowIndex()-1))
			.thenRun(() -> {
				OOCStream<IndexedMatrixValue> workStream = new SubscribableTaskQueue<>();
				workStream.setSubscriber(xcb -> {

				});
				OOCPrimitiveUtils.collect(x, _cache,
					idx -> (int)(2*rTiles + (idx.getRowIndex()-1) * nBroadcast + idx.getColumnIndex()-1), workStream);
			});
	}

	public OOCStreamable<IndexedMatrixValue> getXStreamable() {
		return _xStreamable;
	}

	public OOCStreamable<IndexedMatrixValue> getVStreamable() {
		return _vStreamable;
	}

	public OOCStreamable<IndexedMatrixValue> getWStreamable() {
		return _wStreamable;
	}

	public OOCStreamable<IndexedMatrixValue> getOutputStreamable() {
		return _outputStreamable;
	}

	public ChainType getType() {
		return _type;
	}

	public StreamContext getContext() {
		return _sc;
	}
}
