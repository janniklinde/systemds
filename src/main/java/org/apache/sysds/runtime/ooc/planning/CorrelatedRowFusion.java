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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.primitives.BroadcastOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.CorrelatedScanOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.CorrelatedScanOOCPrimitive.InputAccess;
import org.apache.sysds.runtime.ooc.primitives.GeneralMMultOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.GroupedReduceOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.JoinOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.MappingOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.MaterializeOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.TransposeOOCPrimitive;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

/** Planner rewrite for a retained row-local result which is immediately reused by {@code t(D) %*% X}. */
final class CorrelatedRowFusion {
	private CorrelatedRowFusion() {
	}

	static boolean rewrite(OOCPrimitive root) {
		List<OOCPrimitive> before = collect(root);
		boolean changed = false;
		for(OOCPrimitive primitive : before)
			if(primitive != root && primitive instanceof GeneralMMultOOCPrimitive multiply &&
				!multiply.hasStartedExecution())
				changed |= rewriteMultiply(multiply);
		if(!changed)
			return false;

		for(OOCPrimitive primitive : before)
			primitive.refreshInputDependencies();
		Set<OOCPrimitive> retained = Collections.newSetFromMap(new IdentityHashMap<>());
		collect(root, retained, new ArrayList<>());
		for(OOCPrimitive primitive : before)
			if(!retained.contains(primitive))
				primitive.discardInputHandles();
		return true;
	}

	private static boolean rewriteMultiply(GeneralMMultOOCPrimitive multiply) {
		OOCStreamable<IndexedMatrixValue> transposeOutput = unwrap(multiply.getInput(0));
		if(!(transposeOutput.getPrimitive() instanceof TransposeOOCPrimitive transpose))
			return false;
		OOCStreamable<IndexedMatrixValue> derived = unwrap(transpose.getInput(0));
		OOCPrimitive derivedProducer = derived.getPrimitive();
		if(derivedProducer == null || derivedProducer.hasStartedExecution())
			return false;

		OOCStreamable<IndexedMatrixValue> anchor = cast(multiply.getInput(1));
		DataCharacteristics anchorDc = anchor.getDataCharacteristics();
		DataCharacteristics derivedDc = derived.getDataCharacteristics();
		DataCharacteristics outputDc = multiply.getOutput().getDataCharacteristics();
		if(!known(anchorDc) || !known(derivedDc) || !known(outputDc) || derivedDc.getNumColBlocks() != 1 ||
			derivedDc.getNumRowBlocks() != anchorDc.getNumRowBlocks())
			return false;

		Builder builder = new Builder(anchor, derivedProducer.getPlanEpoch());
		Node expression = builder.build(derived, InputAccess.ROW_ALIGNED);
		if(expression == null || !builder.usesAnchor() || builder.sideBytes() > builder.anchorRowBytes())
			return false;

		SubscribableTaskQueue<IndexedMatrixValue> partials = new SubscribableTaskQueue<>();
		partials.setData(matrixObject(anchorDc.getRows(), outputDc.getCols(), anchorDc.getBlocksize()));
		List<OOCStreamable<IndexedMatrixValue>> inputs = new ArrayList<>();
		inputs.add(anchor);
		inputs.addAll(builder.sides());
		long workspace = builder.workspaceBytes();
		long outputBytes = OOCUtils.estimateFullTileBytes(derivedDc) +
			OOCUtils.estimateFullTileBytes(outputDc) * outputDc.getNumColBlocks();

		CorrelatedScanOOCPrimitive<Map<MatrixIndexes, IndexedMatrixValue>, IndexedMatrixValue> fused = new CorrelatedScanOOCPrimitive<>(
			inputs, builder.inputAccess(), List.of(derived, partials),
			(anchorTiles, sides) -> expression.evaluate(anchorTiles, sides),
			(anchorTiles, result) -> outputs(anchorTiles, result, transpose, multiply), OOCUtils::memoryCharge,
			workspace, outputBytes, 16, multiply.getContext());
		derived.replacePrimitive(derivedProducer, fused);
		partials.assignPrimitive(fused);

		GroupedReduceOOCPrimitive reducer = new GroupedReduceOOCPrimitive(partials, multiply.getOutput(),
			GroupedReduceOOCPrimitive.Grouping.COL_BLOCKS, value -> (MatrixBlock) value.getValue(),
			(left, right) -> left.binaryOperations(multiply.getMergeOperator(), right, new MatrixBlock()),
			Function.identity(), multiply.getContext());
		reducer.setTileOperation(
			new OOCTileOperation(OOCTileOperation.denseOutput(), OOCTileOperation.Relation.COL_GROUP));
		multiply.getOutput().replacePrimitive(multiply, reducer);
		return true;
	}

	private static List<CorrelatedScanOOCPrimitive.Output<IndexedMatrixValue>> outputs(List<IndexedMatrixValue> anchor,
		Map<MatrixIndexes, IndexedMatrixValue> derived, TransposeOOCPrimitive transpose,
		GeneralMMultOOCPrimitive multiply) {
		List<CorrelatedScanOOCPrimitive.Output<IndexedMatrixValue>> outputs = new ArrayList<>();
		for(IndexedMatrixValue value : derived.values())
			outputs.add(new CorrelatedScanOOCPrimitive.Output<>(0, value));
		for(IndexedMatrixValue leftValue : derived.values()) {
			MatrixBlock left = transpose.getOperation().apply((MatrixBlock) leftValue.getValue());
			for(IndexedMatrixValue rightValue : anchor) {
				MatrixBlock right = (MatrixBlock) rightValue.getValue();
				MatrixBlock block = left.aggregateBinaryOperations(left, right, new MatrixBlock(),
					multiply.getMultiplyOperator());
				outputs.add(new CorrelatedScanOOCPrimitive.Output<>(1, new IndexedMatrixValue(
					new MatrixIndexes(leftValue.getIndexes().getRowIndex(), rightValue.getIndexes().getColumnIndex()),
					block)));
			}
		}
		return outputs;
	}

	private static boolean known(DataCharacteristics dc) {
		return dc != null && dc.dimsKnown() && dc.getBlocksize() > 0;
	}

	private static MatrixObject matrixObject(long rows, long cols, int blocksize) {
		return new MatrixObject(ValueType.FP64, null,
			new MetaDataFormat(new MatrixCharacteristics(rows, cols, blocksize, -1), FileFormat.BINARY));
	}

	@SuppressWarnings("unchecked")
	private static OOCStreamable<IndexedMatrixValue> unwrap(OOCStreamable<?> stream) {
		OOCStreamable<?> current = stream;
		while(current.getPrimitive() instanceof MaterializeOOCPrimitive materialize)
			current = materialize.getInput(0);
		return (OOCStreamable<IndexedMatrixValue>) current;
	}

	private static List<OOCPrimitive> collect(OOCPrimitive root) {
		List<OOCPrimitive> result = new ArrayList<>();
		collect(root, Collections.newSetFromMap(new IdentityHashMap<>()), result);
		return result;
	}

	private static void collect(OOCPrimitive primitive, Set<OOCPrimitive> visited, List<OOCPrimitive> result) {
		if(!visited.add(primitive))
			return;
		result.add(primitive);
		for(OOCPrimitive child : primitive.getChildren())
			collect(child, visited, result);
	}

	private interface Node {
		Map<MatrixIndexes, IndexedMatrixValue> evaluate(List<IndexedMatrixValue> anchor,
			List<List<IndexedMatrixValue>> sides);
	}

	private static final class Builder {
		private final OOCStreamable<IndexedMatrixValue> _anchor;
		private final OOCStreamable<IndexedMatrixValue> _canonicalAnchor;
		private final long _epoch;
		private final IdentityHashMap<OOCStreamable<?>, Node> _nodes = new IdentityHashMap<>();
		private final IdentityHashMap<OOCStreamable<?>, Node> _fullNodes = new IdentityHashMap<>();
		private final IdentityHashMap<OOCStreamable<?>, Integer> _sideIndexes = new IdentityHashMap<>();
		private final List<OOCStreamable<IndexedMatrixValue>> _sides = new ArrayList<>();
		private final List<InputAccess> _sideAccess = new ArrayList<>();
		private final Set<OOCStreamable<?>> _visiting = Collections.newSetFromMap(new IdentityHashMap<>());
		private boolean _usesAnchor;
		private long _workspaceBytes;

		private Builder(OOCStreamable<IndexedMatrixValue> anchor, long epoch) {
			_anchor = anchor;
			_canonicalAnchor = unwrap(anchor);
			_epoch = epoch;
		}

		private Node build(OOCStreamable<IndexedMatrixValue> stream, InputAccess access) {
			OOCStreamable<IndexedMatrixValue> source = unwrap(stream);
			if(source == _canonicalAnchor) {
				if(access == InputAccess.FULL)
					return null;
				_usesAnchor = true;
				return (anchor, sides) -> index(anchor);
			}
			IdentityHashMap<OOCStreamable<?>, Node> nodes = access == InputAccess.ROW_ALIGNED ? _nodes : _fullNodes;
			Node cached = nodes.get(source);
			if(cached != null)
				return cached;
			if(!_visiting.add(source))
				return null;
			Node node = buildProducer(source, stream, access);
			_visiting.remove(source);
			if(node != null) {
				nodes.put(source, node);
				DataCharacteristics dc = source.getDataCharacteristics();
				if(known(dc) && dependsOnAnchor(source))
					_workspaceBytes += OOCUtils.estimateFullTileBytes(dc) * dc.getNumColBlocks();
			}
			return node;
		}

		private Node buildProducer(OOCStreamable<IndexedMatrixValue> stream,
			OOCStreamable<IndexedMatrixValue> materialized, InputAccess access) {
			OOCPrimitive primitive = stream.getPrimitive();
			if(primitive == null || !dependsOnAnchor(stream))
				return side(materialized, access);
			DataCharacteristics dc = stream.getDataCharacteristics();
			DataCharacteristics anchorDc = _anchor.getDataCharacteristics();
			if(primitive.getPlanEpoch() != _epoch ||
				known(dc) && known(anchorDc) && dc.getNumRowBlocks() != anchorDc.getNumRowBlocks())
				return side(materialized, access);
			if(primitive instanceof MappingOOCPrimitive map && equi(primitive, 1)) {
				Node input = build(cast(primitive.getInput(0)), access);
				return input == null ? null : (anchor, sides) -> map(input.evaluate(anchor, sides), map.getOperation());
			}
			if(primitive instanceof JoinOOCPrimitive<?, ?, ?> join && equi(primitive, 2)) {
				Node left = build(cast(primitive.getInput(0)), access);
				Node right = build(cast(primitive.getInput(1)), access);
				return left == null || right == null ? null : (anchor, sides) -> join(left.evaluate(anchor, sides),
					right.evaluate(anchor, sides), join);
			}
			if(primitive instanceof BroadcastOOCPrimitive broadcast) {
				List<Node> inputs = new ArrayList<>();
				for(int i = 0; i < broadcast.getNumInputs(); i++) {
					Node input = build(cast(primitive.getInput(i)), i == 0 ? access : InputAccess.FULL);
					if(input == null)
						return null;
					inputs.add(input);
				}
				return (anchor, sides) -> broadcast(inputs, anchor, sides, broadcast);
			}
			if(primitive instanceof GroupedReduceOOCPrimitive reduce &&
				reduce.getGrouping() == GroupedReduceOOCPrimitive.Grouping.ROW_BLOCKS) {
				Node input = build(cast(primitive.getInput(0)), access);
				return input == null ? null : (anchor, sides) -> reduce(input.evaluate(anchor, sides), reduce);
			}
			if(primitive instanceof GeneralMMultOOCPrimitive multiply) {
				Node left = build(cast(primitive.getInput(0)), access);
				Node right = build(cast(primitive.getInput(1)), InputAccess.FULL);
				return left == null || right == null ? null : (anchor, sides) -> multiply(left.evaluate(anchor, sides),
					right.evaluate(anchor, sides), multiply);
			}
			return null;
		}

		private Node side(OOCStreamable<IndexedMatrixValue> stream, InputAccess access) {
			if(access == InputAccess.ROW_ALIGNED && !rowAligned(stream))
				access = InputAccess.FULL;
			InputAccess requested = access;
			int side = _sideIndexes.computeIfAbsent(stream, ignored -> {
				_sides.add(stream);
				_sideAccess.add(requested);
				return _sides.size() - 1;
			});
			if(requested == InputAccess.FULL)
				_sideAccess.set(side, InputAccess.FULL);
			return (anchor, sides) -> index(sides.get(side));
		}

		private boolean rowAligned(OOCStreamable<IndexedMatrixValue> stream) {
			DataCharacteristics dc = stream.getDataCharacteristics();
			DataCharacteristics anchorDc = _anchor.getDataCharacteristics();
			return known(dc) && known(anchorDc) && dc.getBlocksize() == anchorDc.getBlocksize() &&
				dc.getNumRowBlocks() == anchorDc.getNumRowBlocks();
		}

		private boolean dependsOnAnchor(OOCStreamable<IndexedMatrixValue> stream) {
			return dependsOn(unwrap(stream), _canonicalAnchor, Collections.newSetFromMap(new IdentityHashMap<>()));
		}

		private long sideBytes() {
			long bytes = 0;
			for(int i = 0; i < _sides.size(); i++) {
				OOCStreamable<IndexedMatrixValue> side = _sides.get(i);
				DataCharacteristics dc = side.getDataCharacteristics();
				if(!known(dc))
					return Long.MAX_VALUE;
				long blocks = _sideAccess.get(i) == InputAccess.ROW_ALIGNED ? dc.getNumColBlocks() : OOCUtils
					.getNumBlocks(dc);
				bytes += OOCUtils.estimateOutputTileBytes(dc) * blocks;
			}
			return bytes;
		}

		private long anchorRowBytes() {
			DataCharacteristics dc = _anchor.getDataCharacteristics();
			return OOCUtils.estimateOutputTileBytes(dc) * dc.getNumColBlocks();
		}

		private boolean usesAnchor() {
			return _usesAnchor;
		}

		private List<OOCStreamable<IndexedMatrixValue>> sides() {
			return _sides;
		}

		private List<InputAccess> inputAccess() {
			List<InputAccess> access = new ArrayList<>(_sideAccess.size() + 1);
			access.add(InputAccess.ROW_ALIGNED);
			access.addAll(_sideAccess);
			return access;
		}

		private long workspaceBytes() {
			return _workspaceBytes;
		}
	}

	private static boolean dependsOn(OOCStreamable<IndexedMatrixValue> stream, OOCStreamable<IndexedMatrixValue> anchor,
		Set<OOCStreamable<?>> visited) {
		if(stream == anchor)
			return true;
		if(!visited.add(stream))
			return false;
		OOCPrimitive producer = stream.getPrimitive();
		if(producer == null)
			return false;
		for(int i = 0; i < producer.getNumInputs(); i++)
			if(dependsOn(unwrap(producer.getInput(i)), anchor, visited))
				return true;
		return false;
	}

	private static boolean equi(OOCPrimitive primitive, int inputs) {
		OOCTileOperation operation = primitive.getTileOperation();
		if(operation == null || operation.getNumInputs() != inputs)
			return false;
		for(int i = 0; i < inputs; i++)
			if(operation.getInputRelation(i) != OOCTileOperation.Relation.EQUI)
				return false;
		return true;
	}

	private static Map<MatrixIndexes, IndexedMatrixValue> index(List<IndexedMatrixValue> values) {
		Map<MatrixIndexes, IndexedMatrixValue> indexed = new LinkedHashMap<>();
		for(IndexedMatrixValue value : values)
			indexed.put(new MatrixIndexes(value.getIndexes()), value);
		return indexed;
	}

	private static Map<MatrixIndexes, IndexedMatrixValue> map(Map<MatrixIndexes, IndexedMatrixValue> input,
		Function<IndexedMatrixValue, MatrixBlock> operation) {
		Map<MatrixIndexes, IndexedMatrixValue> result = new LinkedHashMap<>();
		for(IndexedMatrixValue value : input.values())
			result.put(new MatrixIndexes(value.getIndexes()),
				new IndexedMatrixValue(new MatrixIndexes(value.getIndexes()), operation.apply(value)));
		return result;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static Map<MatrixIndexes, IndexedMatrixValue> join(Map<MatrixIndexes, IndexedMatrixValue> left,
		Map<MatrixIndexes, IndexedMatrixValue> right, JoinOOCPrimitive primitive) {
		Map<MatrixIndexes, IndexedMatrixValue> result = new LinkedHashMap<>();
		BiFunction operation = primitive.getOperation();
		for(Map.Entry<MatrixIndexes, IndexedMatrixValue> entry : left.entrySet()) {
			IndexedMatrixValue rightValue = right.get(entry.getKey());
			if(rightValue == null)
				continue;
			IndexedMatrixValue value = (IndexedMatrixValue) operation.apply(entry.getValue(), rightValue);
			result.put(new MatrixIndexes(value.getIndexes()), value);
		}
		return result;
	}

	private static Map<MatrixIndexes, IndexedMatrixValue> broadcast(List<Node> inputs, List<IndexedMatrixValue> anchor,
		List<List<IndexedMatrixValue>> sides, BroadcastOOCPrimitive primitive) {
		Map<MatrixIndexes, IndexedMatrixValue> streamed = inputs.get(0).evaluate(anchor, sides);
		List<Map<MatrixIndexes, IndexedMatrixValue>> indexed = new ArrayList<>();
		for(int i = 1; i < inputs.size(); i++)
			indexed.add(inputs.get(i).evaluate(anchor, sides));
		Map<MatrixIndexes, IndexedMatrixValue> result = new LinkedHashMap<>();
		for(IndexedMatrixValue value : streamed.values()) {
			IndexedMatrixValue[][] bands = new IndexedMatrixValue[indexed.size()][];
			for(int side = 0; side < indexed.size(); side++) {
				bands[side] = new IndexedMatrixValue[primitive.getBandWidths()[side]];
				long row = primitive.getLookupRows()[side].applyAsLong(value);
				long col = primitive.getLookupCols()[side].applyAsLong(value);
				for(int band = 0; band < bands[side].length; band++)
					bands[side][band] = indexed.get(side).get(new MatrixIndexes(row, col + band));
			}
			IndexedMatrixValue output = primitive.getOperation().apply(value, bands);
			result.put(new MatrixIndexes(output.getIndexes()), output);
		}
		return result;
	}

	private static Map<MatrixIndexes, IndexedMatrixValue> reduce(Map<MatrixIndexes, IndexedMatrixValue> input,
		GroupedReduceOOCPrimitive primitive) {
		Map<Long, MatrixBlock> groups = new LinkedHashMap<>();
		for(IndexedMatrixValue value : input.values()) {
			long row = value.getIndexes().getRowIndex();
			MatrixBlock partial = primitive.getPartialOperation().apply(value);
			groups.merge(row, partial, (left, right) -> primitive.getMergeOperation().apply(left, right));
		}
		Map<MatrixIndexes, IndexedMatrixValue> result = new LinkedHashMap<>();
		for(Map.Entry<Long, MatrixBlock> entry : groups.entrySet()) {
			MatrixIndexes indexes = new MatrixIndexes(entry.getKey(), 1);
			result.put(indexes,
				new IndexedMatrixValue(indexes, primitive.getFinishOperation().apply(entry.getValue())));
		}
		return result;
	}

	private static Map<MatrixIndexes, IndexedMatrixValue> multiply(Map<MatrixIndexes, IndexedMatrixValue> left,
		Map<MatrixIndexes, IndexedMatrixValue> right, GeneralMMultOOCPrimitive primitive) {
		Map<MatrixIndexes, MatrixBlock> blocks = new HashMap<>();
		for(IndexedMatrixValue leftValue : left.values())
			for(IndexedMatrixValue rightValue : right.values()) {
				if(leftValue.getIndexes().getColumnIndex() != rightValue.getIndexes().getRowIndex())
					continue;
				MatrixIndexes indexes = new MatrixIndexes(leftValue.getIndexes().getRowIndex(),
					rightValue.getIndexes().getColumnIndex());
				MatrixBlock leftBlock = (MatrixBlock) leftValue.getValue();
				MatrixBlock rightBlock = (MatrixBlock) rightValue.getValue();
				MatrixBlock partial = leftBlock.aggregateBinaryOperations(leftBlock, rightBlock, new MatrixBlock(),
					primitive.getMultiplyOperator());
				blocks.merge(indexes, partial, (existing, incoming) -> existing
					.binaryOperations(primitive.getMergeOperator(), incoming, new MatrixBlock()));
			}
		Map<MatrixIndexes, IndexedMatrixValue> result = new LinkedHashMap<>();
		for(Map.Entry<MatrixIndexes, MatrixBlock> entry : blocks.entrySet())
			result.put(entry.getKey(), new IndexedMatrixValue(entry.getKey(), entry.getValue()));
		return result;
	}

	@SuppressWarnings("unchecked")
	private static OOCStreamable<IndexedMatrixValue> cast(OOCStreamable<?> stream) {
		return (OOCStreamable<IndexedMatrixValue>) stream;
	}
}
