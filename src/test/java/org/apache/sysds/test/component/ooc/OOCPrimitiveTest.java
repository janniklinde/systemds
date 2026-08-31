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

package org.apache.sysds.test.component.ooc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.NotThreadSafe;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.lops.MMTSJ.MMTSJType;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.planning.OOCTileOperation;
import org.apache.sysds.runtime.ooc.primitives.CorrelatedScanOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.GroupedReduceOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.MaterializeOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.MappingOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.store.CountingLiveness;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;
import org.apache.sysds.runtime.ooc.stream.FilteredOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.utils.Statistics;
import org.junit.Assert;
import org.junit.Test;

@NotThreadSafe
public class OOCPrimitiveTest {
	@Test
	public void testOutputTileEstimateUsesDenseUpperBound() {
		MatrixCharacteristics dc = new MatrixCharacteristics(67_108_864, 1, 1_200_000, -1);
		long dense = MatrixBlock.estimateSizeDenseInMemory(1_200_000, 1);
		Assert.assertEquals(dense, OOCUtils.estimateOutputTileBytes(dc));
		Assert.assertTrue(MatrixBlock.estimateSizeSparseInMemory(1_200_000, 1, 1.0) > dense);
	}

	@Test
	public void testRetainForgottenCacheCallback() {
		OOCCacheManager.reset();
		try(OOCStream.QueueCallback<IndexedMatrixValue> callback = OOCCacheManager.putAndPin(1, 1,
			new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 7d)))) {
			OOCCacheManager.forget(1, 1);
			try(OOCStream.QueueCallback<IndexedMatrixValue> retained = callback.keepOpen()) {
				Assert.assertEquals(7, retained.get().getValue().get(0, 0), 0);
			}
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testGraphPatternsAndExecution() {
		TestPrimitive source = new TestPrimitive(List.of());
		TestPrimitive sink = new TestPrimitive(List.of(source, source));

		Assert.assertEquals(Set.of(source), sink.getChildren());
		Assert.assertEquals(Set.of(sink), source.getParents());
		source.inferPatterns();
		Assert.assertEquals(OOCAccessPattern.ANY, source.getAccessPattern());
		Assert.assertEquals(OOCAccessPattern.ANY, sink.getAccessPattern());
		Assert.assertEquals(OOCAccessPattern.COL_MAJOR, OOCAccessPattern.ROW_MAJOR.transposed());
		Assert.assertEquals(OOCAccessPattern.UNKNOWN, OOCAccessPattern.ROW_MAJOR.fused(OOCAccessPattern.COL_MAJOR));

		SubscribableTaskQueue<Integer> stream = new SubscribableTaskQueue<>();
		stream.assignPrimitive(sink);
		FilteredOOCStream<Integer> filtered = new FilteredOOCStream<>(stream, ignored -> true);
		Assert.assertSame(sink, filtered.getPrimitive());
		stream.start();
		filtered.start();
		Assert.assertTrue(sink.hasStartedExecution());
		Assert.assertEquals(1, sink._executions);
		Assert.assertEquals(1, source._executions);
		Assert.assertEquals(OOCAccessPattern.ROW_MAJOR, sink.getAccessPattern());
		sink.inferPatterns();
		sink.requestPattern(OOCAccessPattern.COL_MAJOR);
		Assert.assertEquals(OOCAccessPattern.ROW_MAJOR, sink.getAccessPattern());
	}

	@Test
	public void testEquiJoinExposesTileRelationship() {
		MatrixObject data = new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 2, 2), FileFormat.BINARY));
		SubscribableTaskQueue<IndexedMatrixValue> left = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> right = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		left.setData(data);
		right.setData(data);
		output.setData(data);
		OOCInstructionUtils.equiJoin(
			left, right, output, (leftBlock, rightBlock) -> leftBlock
				.binaryOperations(new BinaryOperator(Plus.getPlusFnObject()), rightBlock),
			nnz -> nnz[0] + nnz[1], new StreamContext());

		OOCTileOperation operation = output.getPrimitive().getTileOperation();
		Assert.assertEquals(2, operation.getNumInputs());
		Assert.assertEquals(OOCTileOperation.Relation.EQUI, operation.getInputRelation(0));
		Assert.assertEquals(OOCTileOperation.Relation.EQUI, operation.getInputRelation(1));
		Assert.assertEquals(5, operation.worstCaseOutputNnz(new long[] {2, 3}, 10));
	}

	@Test
	public void testMappingExposesFusionHooks() {
		MatrixObject data = new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 2, 2), FileFormat.BINARY));
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		input.setData(data);
		output.setData(data);
		OOCInstructionUtils.equiMapBlock(input, output, block -> block, true, new StreamContext());
		MappingOOCPrimitive primitive = (MappingOOCPrimitive) output.getPrimitive();
		MatrixBlock block = new MatrixBlock(2, 2, false);
		block.set(0, 0, 1);
		IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1), block);

		Assert.assertSame(block, primitive.getOperation().apply(value));
		Assert.assertEquals(MatrixBlock.estimateSizeInMemory(2, 2, 1), primitive.getMaxTaskReservationBytes(value));
		Assert.assertEquals(MatrixBlock.estimateSizeInMemory(2, 2, 4), primitive.getMaxTaskReservationBytes());
	}

	@Test
	public void testPlannerFusesCorrelatedRowMultiply() throws InterruptedException {
		OOCCacheManager.reset();
		try {
			SubscribableTaskQueue<IndexedMatrixValue> x = matrixStream(2, 2, 1);
			SubscribableTaskQueue<IndexedMatrixValue> derived = matrixStream(2, 1, 1);
			SubscribableTaskQueue<IndexedMatrixValue> transposed = matrixStream(1, 2, 1);
			SubscribableTaskQueue<IndexedMatrixValue> product = matrixStream(1, 2, 1);
			for(double[] tile : List.of(new double[] {1, 1, 1}, new double[] {1, 2, 2}, new double[] {2, 1, 3},
				new double[] {2, 2, 4}))
				x.enqueue(new IndexedMatrixValue(new MatrixIndexes((long) tile[0], (long) tile[1]),
					new MatrixBlock(1, 1, tile[2])));
			x.closeInput();

			BinaryOperator plus = new BinaryOperator(Plus.getPlusFnObject());
			OOCInstructionUtils.rowGroupedReduce(x, derived,
				(left, right) -> left.binaryOperations(plus, right, new MatrixBlock()), new StreamContext());
			OOCInstructionUtils.transpose(derived, transposed, new StreamContext());
			AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
			OOCInstructionUtils.matrixMultiply(transposed, x, product,
				new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate), plus, new StreamContext());
			CollectingSink sink = new CollectingSink(product, derived);

			sink.start();
			Assert.assertTrue("Correlated row fusion did not finish", sink._complete.await(10, TimeUnit.SECONDS));
			Assert.assertNull(sink.getFailure());
			Assert.assertTrue(derived.getPrimitive() instanceof CorrelatedScanOOCPrimitive);
			Assert.assertTrue(product.getPrimitive() instanceof GroupedReduceOOCPrimitive);
			Assert.assertEquals(Map.of("1,1", 24d, "1,2", 34d), sink._first);
			Assert.assertEquals(Map.of("1,1", 3d, "2,1", 7d), sink._second);
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	private static SubscribableTaskQueue<IndexedMatrixValue> matrixStream(long rows, long cols, int blocksize) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		stream.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(rows, cols, blocksize), FileFormat.BINARY)));
		return stream;
	}

	@Test
	public void testPlannerDoubleMaterialize() {
		OOCCacheManager.reset();
		try {
			SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
			source.setData(new MatrixObject(ValueType.FP64, "/dev/null",
				new MetaDataFormat(new MatrixCharacteristics(0, 0, 1), FileFormat.BINARY)));
			CachingStream cached = new CachingStream(source);
			source.closeInput();
			MaterializingTestPrimitive sink = new MaterializingTestPrimitive(cached);
			cached.getReadStream().setSubscriber(OOCStream.QueueCallback::close);
			cached.scheduleDeletion();

			sink.start();

			Assert.assertEquals(1, sink.getChildren().size());
			Assert.assertTrue(sink.getInputDependency(0) instanceof MaterializeOOCPrimitive);
			Assert.assertSame(sink.getInputDependency(0), sink.getInputDependency(1));
			Assert.assertEquals(1, sink._executions);
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testReusableMaterializedStream() {
		OOCCacheManager.reset();
		try {
			MatrixObject data = new MatrixObject(ValueType.FP64, "/dev/null",
				new MetaDataFormat(new MatrixCharacteristics(1, 2, 1), FileFormat.BINARY));
			SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
			source.setData(data);
			source.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 3d)));
			source.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(1, 1, 4d)));
			source.closeInput();

			MaterializedStoreStreamable handle = new MaterializedStoreStreamable(source, data);
			handle.reserveLazyHandle();
			handle.reserveLazyHandle();
			handle.scheduleMaterializedStoreDeletion();
			OOCStream<IndexedMatrixValue> first = handle.getReservedReadStream();
			OOCStream<IndexedMatrixValue> second = handle.getReservedReadStream();
			first.start();

			for(OOCStream<IndexedMatrixValue> replay : List.of(first, second)) {
				double sum = 0;
				OOCStream.QueueCallback<IndexedMatrixValue> callback;
				while((callback = replay.dequeueCB()) != null)
					try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
						sum += current.get().getValue().get(0, 0);
					}
				Assert.assertEquals(7, sum, 0);
			}
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testDataGenMapTransposePipeline() {
		SubscribableTaskQueue<IndexedMatrixValue> generated = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> mapped = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> transposed = new SubscribableTaskQueue<>();
		generated.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 3, 1), FileFormat.BINARY)));
		mapped.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 3, 1), FileFormat.BINARY)));
		transposed.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(3, 2, 1), FileFormat.BINARY)));

		OOCInstructionUtils.dataGen(generated,
			indexes -> new MatrixBlock(1, 1, (double) indexes.getRowIndex() * 10 + indexes.getColumnIndex()),
			new StreamContext());
		OOCInstructionUtils.equiMapBlock(generated, mapped, input -> new MatrixBlock(1, 1, input.get(0, 0) + 1),
			new StreamContext());
		OOCInstructionUtils.transpose(mapped, transposed, new StreamContext());

		transposed.start();
		Map<String, Double> values = new HashMap<>();
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = transposed.dequeueCB()) != null) {
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				IndexedMatrixValue value = current.get();
				values.put(value.getIndexes().getRowIndex() + "," + value.getIndexes().getColumnIndex(),
					value.getValue().get(0, 0));
			}
		}
		Assert.assertEquals(Map.of("1,1", 12.0, "2,1", 13.0, "3,1", 14.0, "1,2", 22.0, "2,2", 23.0, "3,2", 24.0),
			values);
	}

	@Test
	public void testRepartitionSplitAndAssemble() {
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> split = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		input.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(4, 4, 4), FileFormat.BINARY)));
		split.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(4, 4, 2), FileFormat.BINARY)));
		output.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(4, 4, 4), FileFormat.BINARY)));
		MatrixBlock block = new MatrixBlock(4, 4, false);
		for(int row = 0; row < 4; row++)
			for(int col = 0; col < 4; col++)
				block.set(row, col, row * 4 + col + 1);
		input.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), block));
		input.closeInput();

		OOCInstructionUtils.repartition(input, split, ignored -> 1, (tile, emit) -> {
			for(int row = 0; row < 2; row++)
				for(int col = 0; col < 2; col++)
					emit.copy(new MatrixIndexes(row + 1L, col + 1L), row * 2, col * 2, 2, 2, 0, 0);
		}, new StreamContext());
		OOCInstructionUtils.repartition(split, output, ignored -> 4, (tile, emit) -> {
			int row = Math.toIntExact(tile.getIndexes().getRowIndex() - 1);
			int col = Math.toIntExact(tile.getIndexes().getColumnIndex() - 1);
			emit.copy(new MatrixIndexes(1, 1), 0, 0, 2, 2, row * 2, col * 2);
		}, new StreamContext());

		output.start();
		try(OOCStream.QueueCallback<IndexedMatrixValue> callback = output.dequeueCB()) {
			MatrixBlock result = (MatrixBlock) callback.get().getValue();
			for(int row = 0; row < 4; row++)
				for(int col = 0; col < 4; col++)
					Assert.assertEquals(row * 4 + col + 1, result.get(row, col), 0);
		}
		Assert.assertNull(output.dequeueCB());
	}

	@Test
	public void testRepartitionSpill() throws InterruptedException {
		OOCCacheManager.reset();
		boolean statistics = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		Statistics.resetOOCEvictionStats();
		OOCCacheManager.getGlobalCache().updateLimits(2_000_000, 100_000);
		try {
			SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
			input.setData(new MatrixObject(ValueType.FP64, "/dev/null",
				new MetaDataFormat(new MatrixCharacteristics(800, 400, 200), FileFormat.BINARY)));
			output.setData(new MatrixObject(ValueType.FP64, "/dev/null",
				new MetaDataFormat(new MatrixCharacteristics(800, 200, 200), FileFormat.BINARY)));
			for(int row = 1; row <= 4; row++) {
				MatrixBlock block = new MatrixBlock(200, 100, row * 10d + 1);
				input.enqueue(new IndexedMatrixValue(new MatrixIndexes(row, 1), block));
			}

			OOCInstructionUtils.repartition(input, output, ignored -> 2, (tile, emit) -> {
				int col = Math.toIntExact(tile.getIndexes().getColumnIndex() - 1);
				emit.copy(new MatrixIndexes(tile.getIndexes().getRowIndex(), 1), 0, 0, 200, 100, 0, col * 100);
			}, new StreamContext());
			output.start();
			for(int attempt = 0; attempt < 100 && Statistics.getOOCEvictionWriteCount() == 0; attempt++)
				Thread.sleep(10);
			Assert.assertTrue("Expected repartition state to spill", Statistics.getOOCEvictionWriteCount() > 0);
			for(int row = 1; row <= 4; row++) {
				MatrixBlock block = new MatrixBlock(200, 100, row * 10d + 2);
				input.enqueue(new IndexedMatrixValue(new MatrixIndexes(row, 2), block));
			}
			input.closeInput();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = output.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					MatrixBlock block = (MatrixBlock) value.getValue();
					Assert.assertEquals(value.getIndexes().getRowIndex() * 10 + 1, block.get(0, 0), 0);
					Assert.assertEquals(value.getIndexes().getRowIndex() * 10 + 2, block.get(0, 199), 0);
					blocks++;
				}
			Assert.assertEquals(4, blocks);
		}
		finally {
			OOCCacheManager.reset();
			DMLScript.OOC_STATISTICS = statistics;
		}
	}

	@Test
	public void testCorrelatedScanRetainsSpilledRowGroup() throws InterruptedException {
		OOCCacheManager.reset();
		boolean statistics = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		Statistics.resetOOCEvictionStats();
		OOCCacheManager.getGlobalCache().updateLimits(1_500_000, 100_000);
		try {
			SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
			input.setData(new MatrixObject(ValueType.FP64, "/dev/null",
				new MetaDataFormat(new MatrixCharacteristics(1600, 400, 200), FileFormat.BINARY)));
			output.setData(new MatrixObject(ValueType.FP64, "/dev/null",
				new MetaDataFormat(new MatrixCharacteristics(1600, 1, 200), FileFormat.BINARY)));
			for(int row = 1; row <= 8; row++)
				for(int col = 1; col <= 2; col++)
					input.enqueue(new IndexedMatrixValue(new MatrixIndexes(row, col),
						new MatrixBlock(200, 200, row * 10d + col)));
			input.closeInput();

			CountDownLatch prefetchedGroups = new CountDownLatch(2);
			OOCInstructionUtils.correlatedScan(input, output, values -> {
				try {
					prefetchedGroups.countDown();
					Assert.assertTrue("Expected two correlated groups in flight",
						prefetchedGroups.await(5, TimeUnit.SECONDS));
					for(int attempt = 0; attempt < 100 && Statistics.getOOCEvictionWriteCount() == 0; attempt++)
						Thread.sleep(10);
				}
				catch(InterruptedException error) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(error);
				}
				double sum = 0;
				for(IndexedMatrixValue value : values)
					sum += ((MatrixBlock) value.getValue()).sum();
				return sum;
			}, (values, sum) -> List.of(new IndexedMatrixValue(
				new MatrixIndexes(values.get(0).getIndexes().getRowIndex(), 1), new MatrixBlock(1, 1, sum))),
				OOCUtils::memoryCharge, 128, 128, 2, new StreamContext());

			output.start();
			Map<Long, Double> sums = new HashMap<>();
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = output.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					sums.put(value.getIndexes().getRowIndex(), value.getValue().get(0, 0));
				}
			for(long row = 1; row <= 8; row++)
				Assert.assertEquals((row * 20 + 3) * 40_000, sums.get(row), 0);
			Assert.assertTrue("Expected correlated-scan input materialization to spill",
				Statistics.getOOCEvictionWriteCount() > 0);
		}
		finally {
			OOCCacheManager.reset();
			DMLScript.OOC_STATISTICS = statistics;
		}
	}

	@Test
	public void testReduce() {
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<MatrixBlock> output = new SubscribableTaskQueue<>();
		input.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 3, 1), FileFormat.BINARY)));
		output.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(1, 1, 1), FileFormat.BINARY)));
		for(long[] indexes : List.of(new long[] {2, 3}, new long[] {1, 1}, new long[] {2, 1}, new long[] {1, 3},
			new long[] {1, 2}, new long[] {2, 2}))
			input.enqueue(new IndexedMatrixValue(new MatrixIndexes(indexes[0], indexes[1]),
				new MatrixBlock(1, 1, indexes[0] * 10d + indexes[1])));
		input.closeInput();
		OOCInstructionUtils.reduce(input, output, value -> new MatrixBlock(1, 1, 2 * value.getValue().get(0, 0)),
			(left, right) -> new MatrixBlock(1, 1, left.get(0, 0) + right.get(0, 0)),
			MatrixBlock::getExactSerializedSize, new StreamContext());

		output.start();
		try(OOCStream.QueueCallback<MatrixBlock> callback = output.dequeueCB()) {
			Assert.assertEquals(204, callback.get().get(0, 0), 0);
		}
		Assert.assertNull(output.dequeueCB());
	}

	@Test
	public void testGroupedReduceModes() {
		Assert.assertEquals(Map.of("1,1", 136d, "2,1", 166d),
			runGroupedReduce(GroupedReduceOOCPrimitive.Grouping.ROW_BLOCKS, 2, 1));
		Assert.assertEquals(Map.of("1,1", 132d, "1,2", 134d, "1,3", 136d),
			runGroupedReduce(GroupedReduceOOCPrimitive.Grouping.COL_BLOCKS, 1, 3));
	}

	private static Map<String, Double> runGroupedReduce(GroupedReduceOOCPrimitive.Grouping grouping, long outputRows,
		long outputCols) {
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		input.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 3, 1), FileFormat.BINARY)));
		output.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(outputRows, outputCols, 1), FileFormat.BINARY)));
		for(long[] indexes : List.of(new long[] {2, 3}, new long[] {1, 1}, new long[] {2, 1}, new long[] {1, 3},
			new long[] {1, 2}, new long[] {2, 2}))
			input.enqueue(new IndexedMatrixValue(new MatrixIndexes(indexes[0], indexes[1]),
				new MatrixBlock(1, 1, indexes[0] * 10d + indexes[1])));
		input.closeInput();
		OOCInstructionUtils.groupedReduceIndexed(input, output, grouping, value -> (MatrixBlock) value.getValue(),
			(left, right) -> new MatrixBlock(1, 1, left.get(0, 0) + right.get(0, 0)),
			value -> new MatrixBlock(1, 1, value.get(0, 0) + 100), new StreamContext());

		output.start();
		Map<String, Double> values = new HashMap<>();
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = output.dequeueCB()) != null)
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				IndexedMatrixValue value = current.get();
				values.put(value.getIndexes().getRowIndex() + "," + value.getIndexes().getColumnIndex(),
					value.getValue().get(0, 0));
			}
		return values;
	}

	@Test
	public void testTsmmOutOfOrderGroups() {
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		input.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 2, 1), FileFormat.BINARY)));
		output.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 2, 1), FileFormat.BINARY)));
		for(double[] tile : List.of(new double[] {2, 1, 3}, new double[] {1, 2, 2}, new double[] {2, 2, 4},
			new double[] {1, 1, 1}))
			input.enqueue(new IndexedMatrixValue(new MatrixIndexes((long) tile[0], (long) tile[1]),
				new MatrixBlock(1, 1, tile[2])));
		input.closeInput();

		AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
		OOCInstructionUtils.tsmm(input, output, MMTSJType.LEFT,
			new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate),
			new BinaryOperator(Plus.getPlusFnObject()), new StreamContext());
		output.start();

		Map<String, Double> values = new HashMap<>();
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = output.dequeueCB()) != null)
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				IndexedMatrixValue value = current.get();
				values.put(value.getIndexes().getRowIndex() + "," + value.getIndexes().getColumnIndex(),
					value.getValue().get(0, 0));
			}
		Assert.assertEquals(Map.of("1,1", 10d, "1,2", 14d, "2,1", 14d, "2,2", 20d), values);
	}

	@Test
	public void testTsmmConsumesLiveMaterialization() throws InterruptedException {
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		input.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(1, 2, 1), FileFormat.BINARY)));
		output.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 2, 1), FileFormat.BINARY)));

		AggregateOperator aggregate = new AggregateOperator(0, Plus.getPlusFnObject());
		OOCInstructionUtils.tsmm(input, output, MMTSJType.LEFT,
			new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), aggregate),
			new BinaryOperator(Plus.getPlusFnObject()), new StreamContext());

		Map<String, Double> values = new ConcurrentHashMap<>();
		CountDownLatch blocks = new CountDownLatch(4);
		CountDownLatch complete = new CountDownLatch(1);
		output.setSubscriber(callback -> {
			try(callback) {
				if(callback.isEos() || callback.isFailure()) {
					complete.countDown();
					return;
				}
				IndexedMatrixValue value = callback.get();
				values.put(value.getIndexes().getRowIndex() + "," + value.getIndexes().getColumnIndex(),
					value.getValue().get(0, 0));
				blocks.countDown();
			}
		});
		output.start();

		try {
			input.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 1d)));
			input.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(1, 1, 2d)));
			Assert.assertTrue("TSMM waited for materialization completion", blocks.await(10, TimeUnit.SECONDS));
			Assert.assertEquals(Map.of("1,1", 1d, "1,2", 2d, "2,1", 2d, "2,2", 4d), values);
		}
		finally {
			input.closeInput();
		}
		Assert.assertTrue("TSMM output did not close", complete.await(10, TimeUnit.SECONDS));
	}

	@Test
	public void testNaryJoinOutOfOrder() {
		SubscribableTaskQueue<IndexedMatrixValue> first = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> second = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> third = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		for(SubscribableTaskQueue<IndexedMatrixValue> stream : List.of(first, second, third, output))
			stream.setData(new MatrixObject(ValueType.FP64, "/dev/null",
				new MetaDataFormat(new MatrixCharacteristics(1, 2, 1), FileFormat.BINARY)));
		CachingStream cachedSecond = new CachingStream(second);
		first.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 10d)));
		first.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(1, 1, 20d)));
		second.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(1, 1, 2d)));
		second.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 1d)));
		third.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 100d)));
		third.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(1, 1, 200d)));
		first.closeInput();
		second.closeInput();
		third.closeInput();

		OOCInstructionUtils.naryEquiJoin(List.of(first, cachedSecond, third), output,
			blocks -> new IndexedMatrixValue(blocks.get(0).getIndexes(),
				new MatrixBlock(1, 1, blocks.get(0).getValue().get(0, 0) + 10 * blocks.get(1).getValue().get(0, 0) +
					100 * blocks.get(2).getValue().get(0, 0))),
			new StreamContext());

		output.start();
		Map<Long, Double> values = new HashMap<>();
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = output.dequeueCB()) != null)
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				values.put(current.get().getIndexes().getColumnIndex(), current.get().getValue().get(0, 0));
			}
		Assert.assertEquals(Map.of(1L, 10020d, 2L, 20040d), values);
		cachedSecond.scheduleDeletion();
	}

	@Test
	public void testJoinOutOfOrder() {
		SubscribableTaskQueue<IndexedMatrixValue> left = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> right = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> joined = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> addends = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		for(SubscribableTaskQueue<IndexedMatrixValue> stream : List.of(left, right, joined, addends, output))
			stream.setData(new MatrixObject(ValueType.FP64, "/dev/null",
				new MetaDataFormat(new MatrixCharacteristics(1, 2, 1), FileFormat.BINARY)));
		CachingStream cachedLeft = new CachingStream(left);
		left.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 10d)));
		left.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(1, 1, 20d)));
		right.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(1, 1, 2d)));
		right.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 1d)));
		addends.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 100d)));
		addends.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(1, 1, 200d)));
		left.closeInput();
		right.closeInput();
		addends.closeInput();
		OOCInstructionUtils.equiJoin(cachedLeft, right, joined,
			(l, r) -> new MatrixBlock(1, 1, l.get(0, 0) + r.get(0, 0)), new StreamContext());
		OOCInstructionUtils.equiJoin(joined, addends, output,
			(l, r) -> new MatrixBlock(1, 1, l.get(0, 0) + r.get(0, 0)), new StreamContext());

		output.start();
		Map<Long, Double> values = new HashMap<>();
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = output.dequeueCB()) != null)
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				values.put(current.get().getIndexes().getColumnIndex(), current.get().getValue().get(0, 0));
			}
		Assert.assertEquals(Map.of(1L, 111.0, 2L, 222.0), values);
		cachedLeft.scheduleDeletion();
	}

	private static final class MaterializingTestPrimitive extends OOCPrimitive {
		private int _executions;

		private MaterializingTestPrimitive(OOCStreamable<IndexedMatrixValue> source) {
			super(new StreamContext(), source, source);
		}

		@Override
		public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
			return List.of(new OOCMaterializedInputRequest(0, OOCStoreLayout.ROW_MAJOR, 1),
				new OOCMaterializedInputRequest(1, OOCStoreLayout.ROW_MAJOR, 1));
		}

		@Override
		protected void startExecution() {
			getMaterializedInput(0).whenComplete((store, error) -> {
				if(error != null)
					return;
				store.completion().whenComplete((ignored, completionError) -> {
					if(completionError != null)
						return;
					IndexedMaterializedStoreReader<IndexedMatrixValue> first = store
						.openIndexedReader(new CountingLiveness(0, 0));
					IndexedMaterializedStoreReader<IndexedMatrixValue> second = store
						.openIndexedReader(new CountingLiveness(0, 0));
					Assert.assertTrue(store.readersSealed().isDone());
					first.close();
					second.close();
					store.close();
					store.close();
					_executions++;
					onComplete();
				});
			});
		}

		@Override
		protected void inferPatternsInternal() {
			_pattern = OOCAccessPattern.ANY;
			inferParentPatterns();
		}

		@Override
		protected void requestPatternInternal(OOCAccessPattern accessPattern) {
			_pattern = accessPattern;
		}
	}

	private static final class CollectingSink extends OOCPrimitive {
		private final Map<String, Double> _first = new ConcurrentHashMap<>();
		private final Map<String, Double> _second = new ConcurrentHashMap<>();
		private final CountDownLatch _sources = new CountDownLatch(2);
		private final CountDownLatch _complete = new CountDownLatch(1);

		private CollectingSink(OOCStreamable<IndexedMatrixValue> first, OOCStreamable<IndexedMatrixValue> second) {
			super(new StreamContext(), first, second);
		}

		@Override
		protected void startExecution() {
			for(int i = 0; i < 2; i++) {
				int input = i;
				OOCStream<IndexedMatrixValue> stream = getInputReadStream(i);
				getContext().addInStream(stream);
				stream.setSubscriber(callback -> accept(input, callback));
			}
		}

		private void accept(int input, OOCStream.QueueCallback<IndexedMatrixValue> callback) {
			boolean terminal = callback.isEos() || callback.isFailure();
			try(callback) {
				if(callback.isFailure())
					callback.get();
				else if(!callback.isEos()) {
					IndexedMatrixValue value = callback.get();
					(input == 0 ? _first : _second).put(
						value.getIndexes().getRowIndex() + "," + value.getIndexes().getColumnIndex(),
						value.getValue().get(0, 0));
				}
			}
			catch(Throwable error) {
				fail(error);
			}
			if(terminal) {
				_sources.countDown();
				if(_sources.getCount() == 0) {
					onComplete();
					_complete.countDown();
				}
			}
		}

		@Override
		protected void inferPatternsInternal() {
			_pattern = OOCAccessPattern.ANY;
			for(OOCPrimitive child : getChildren())
				child.requestPattern(OOCAccessPattern.ROW_MAJOR);
		}

		@Override
		protected void requestPatternInternal(OOCAccessPattern accessPattern) {
			_pattern = accessPattern;
		}
	}

	private static final class TestPrimitive extends OOCPrimitive {
		private int _executions;

		private TestPrimitive(List<OOCPrimitive> children) {
			super(null, children);
		}

		@Override
		protected void startExecution() {
			_executions++;
			onComplete();
		}

		@Override
		protected void inferPatternsInternal() {
			_pattern = OOCAccessPattern.ANY;
			inferParentPatterns();
		}

		@Override
		protected void requestPatternInternal(OOCAccessPattern accessPattern) {
			_pattern = accessPattern;
		}
	}
}
