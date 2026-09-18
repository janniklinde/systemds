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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.AggOp;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.Direction;
import org.apache.sysds.common.Types.OpOpData;
import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.common.Types.OpOp1;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.RewriteInjectOOCTee;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.LocalVariableMap;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.ooc.AppendOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.BinaryOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.MMultOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.CtableOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.DataGenOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.ParameterizedBuiltinOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.cp.IntObject;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.primitives.BroadcastOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.BroadcastStreamingOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.GeneralMMultOOCPrimitive;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.ooc.primitives.FanoutOOCPrimitive;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.store.StateTable;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.StateTableUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class OOCInstructionUtilsTest {
	@Test
	public void testBandFanoutForRowLocalCentroidUpdate() {
		Hop x = new DataOp("X", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			40000, 200, -1, 100);
		Hop tee = HopRewriteUtils.createDataOp("tee", x, OpOpData.TEE);
		Hop c = new DataOp("Ct", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			200, 10, -1, 100);
		Hop d = HopRewriteUtils.createMatrixMultiply(tee, c);
		Hop mins = HopRewriteUtils.createAggUnaryOp(d, AggOp.MIN, Direction.Row);
		Hop p = HopRewriteUtils.createBinary(d, mins, OpOp2.LESSEQUAL);
		Hop normalized = HopRewriteUtils.createBinary(p,
			HopRewriteUtils.createAggUnaryOp(p, AggOp.SUM, Direction.Row), OpOp2.DIV);
		Hop assignments = HopRewriteUtils.createDataOp("P", normalized, OpOpData.TEE);
		Hop transposed = HopRewriteUtils.createTranspose(assignments);
		HopRewriteUtils.createAggUnaryOp(assignments, AggOp.SUM, Direction.Col);
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandFanoutDirection(assignments));
		Hop update = HopRewriteUtils.createMatrixMultiply(transposed, tee);
		Assert.assertEquals(List.of(d, update), HopRewriteUtils.getBandFanoutConsumers(tee, Direction.Row));
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandFanoutDirection(tee));
		Hop global = HopRewriteUtils.createAggUnaryOp(d, AggOp.SUM, Direction.Col);
		Hop invalid = HopRewriteUtils.createBinary(d, global, OpOp2.MULT);
		HopRewriteUtils.createMatrixMultiply(HopRewriteUtils.createTranspose(invalid), tee);
		Assert.assertEquals(List.of(d, update), HopRewriteUtils.getBandFanoutConsumers(tee, Direction.Row));
	}

	@Test
	public void testBandFanoutThroughSkinnyMatmulAndSquare() {
		Hop x = new DataOp("X", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			40000, 200, -1, 100);
		Hop tee = HopRewriteUtils.createDataOp("tee", x, OpOpData.TEE);
		Hop c = new DataOp("Ct", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			200, 10, -1, 100);
		Hop product = HopRewriteUtils.createMatrixMultiply(tee, c);
		Hop squared = HopRewriteUtils.createUnary(product, OpOp1.POW2);
		Hop sums = HopRewriteUtils.createAggUnaryOp(squared, AggOp.SUM, Direction.Row);
		Hop result = HopRewriteUtils.createBinary(tee, sums, OpOp2.MULT);
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandStreamingDirection(tee, sums));
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandStreamingDirection(tee,
			HopRewriteUtils.createAggUnaryOp(product, AggOp.SUM_SQ, Direction.Row)));
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandFanoutDirection(tee));
		Assert.assertEquals(List.of(result, product), HopRewriteUtils.getBandFanoutConsumers(tee, Direction.Row));
		Hop store = HopRewriteUtils.createDataOp("X", tee, OpOpData.TRANSIENTWRITE);
		RewriteInjectOOCTee.injectBandFanouts(new ArrayList<>(List.of(result, store)));
		Hop group = result.getInput(0);
		Assert.assertSame(group, product.getInput(0));
		Assert.assertSame(tee, store.getInput(0));
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandFanoutDirection(group));
	}

	@Test
	public void testBandStreamingRejectsWideAndColumnMatmulPaths() {
		Hop x = new DataOp("X", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			40000, 200, -1, 100);
		Hop c = new DataOp("Ct", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			200, 200, -1, 100);
		Hop product = HopRewriteUtils.createMatrixMultiply(x, c);
		Assert.assertNull(HopRewriteUtils.getBandStreamingDirection(x,
			HopRewriteUtils.createAggUnaryOp(product, AggOp.SUM, Direction.Row)));
		Assert.assertNull(HopRewriteUtils.getBandStreamingDirection(x,
			HopRewriteUtils.createAggUnaryOp(product, AggOp.SUM, Direction.Col)));
	}
	@Test(timeout = 20000)
	public void testFanoutFallsBackForInactiveConsumer() {
		checkLateFanoutReader(true);
	}

	@Test(timeout = 20000)
	public void testFanoutFallsBackForReaderOpenedLater() {
		checkLateFanoutReader(false);
	}

	@SuppressWarnings("unchecked")
	private void checkLateFanoutReader(boolean registerSecond) {
		OOCStreamable<IndexedMatrixValue> input = Mockito.mock(OOCStreamable.class);
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> replay = new SubscribableTaskQueue<>();
		Mockito.doReturn(matrixObject(2, 2, 2)).when(input).getData();
		Mockito.when(input.getReservedReadStream()).thenReturn(source, replay);
		FanoutOOCPrimitive fanout = new FanoutOOCPrimitive(input, true, 2, new StreamContext());
		OOCStream<IndexedMatrixValue> first = fanout.getReadStream();
		OOCStream<IndexedMatrixValue> second = registerSecond ? fanout.getReadStream() : null;
		if(!registerSecond)
			fanout.reserveLazyHandle();
		AtomicInteger received = new AtomicInteger();
		AtomicInteger terminals = new AtomicInteger();
		AtomicInteger releases = new AtomicInteger();
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> consumer = callback -> {
			try(callback) {
				if(callback.isEos())
					terminals.incrementAndGet();
				else {
					Assert.assertEquals(7, callback.get().getValue().get(0, 0), 0);
					received.incrementAndGet();
				}
			}
		};
		first.setSubscriber(consumer);
		first.start();
		fanout.scheduleMaterializedStoreDeletion();
		IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 2, 7d));
		source.enqueue(new MaterializedCallback<>(StoreLease.create(value, releases::incrementAndGet)));
		source.closeInput();
		Assert.assertEquals(1, received.get());
		Assert.assertEquals(1, terminals.get());
		Assert.assertEquals(1, releases.get());
		Assert.assertFalse(fanout.isProcessed());
		Mockito.verify(input, Mockito.never()).discardHandle();
		replay.enqueue(new MaterializedCallback<>(StoreLease.create(value, releases::incrementAndGet)));
		replay.closeInput();
		if(!registerSecond)
			second = fanout.getReservedReadStream();
		second.setSubscriber(consumer);
		Assert.assertEquals(2, received.get());
		Assert.assertEquals(2, terminals.get());
		Assert.assertEquals(2, releases.get());
		Assert.assertTrue(fanout.isProcessed());
		Mockito.verify(input, Mockito.times(2)).reserveLazyHandle();
		Mockito.verify(input, Mockito.times(2)).getReservedReadStream();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testUnusedFanoutReleasesReaderClaims() {
		OOCStreamable<IndexedMatrixValue> input = Mockito.mock(OOCStreamable.class);
		FanoutOOCPrimitive fanout = new FanoutOOCPrimitive(input, true, 2, new StreamContext());
		fanout.scheduleMaterializedStoreDeletion();
		Mockito.verify(input, Mockito.times(2)).reserveLazyHandle();
		Mockito.verify(input, Mockito.times(2)).discardHandle();
		Assert.assertTrue(fanout.isProcessed());
	}

	@Test
	public void testBandFanoutGroupsMatchingConsumersAndPreservesOtherEdges() {
		Hop x = new DataOp("X", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			40000, 200, -1, 100);
		Hop tee = HopRewriteUtils.createDataOp("tee", x, OpOpData.TEE);
		Hop sums = HopRewriteUtils.createAggUnaryOp(tee, AggOp.SUM, Direction.Row);
		Hop first = HopRewriteUtils.createBinary(tee, sums, OpOp2.MULT);
		Hop second = HopRewriteUtils.createBinary(tee, sums, OpOp2.DIV);
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandFanoutDirection(tee));
		Hop store = HopRewriteUtils.createDataOp("X", tee, OpOpData.TRANSIENTWRITE);
		Assert.assertNull(HopRewriteUtils.getBandFanoutDirection(tee));
		Assert.assertNull(HopRewriteUtils.getBandFanoutDirection(x));
		RewriteInjectOOCTee.injectBandFanouts(new ArrayList<>(List.of(first, second, store)));
		Hop group = first.getInput(0);
		Assert.assertNotSame(tee, group);
		Assert.assertSame(group, second.getInput(0));
		Assert.assertSame(group, sums.getInput(0));
		Assert.assertSame(tee, store.getInput(0));
		Assert.assertSame(tee, group.getInput(0));
		Assert.assertEquals(3, group.getParent().size());
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandFanoutDirection(group));
		RewriteInjectOOCTee.injectBandFanouts(new ArrayList<>(List.of(first, second, store)));
		Assert.assertSame(group, first.getInput(0));
		Assert.assertSame(tee, group.getInput(0));
	}

	@Test
	public void testCachedFanoutSharesLeaseAfterPlanStartup() {
		MatrixObject matrix = matrixObject(2, 2, 2);
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		input.setData(matrix);
		FanoutOOCPrimitive fanout = new FanoutOOCPrimitive(input, true, 2, new StreamContext());
		OOCStream<IndexedMatrixValue> first = fanout.getReadStream();
		AtomicReference<OOCStream.QueueCallback<IndexedMatrixValue>> held = new AtomicReference<>();
		AtomicInteger terminals = new AtomicInteger();
		first.setSubscriber(callback -> {
			try(callback) {
				if(callback.isEos())
					terminals.incrementAndGet();
				else
					held.set(callback.keepOpen());
			}
		});
		fanout.tryStartExecution();
		IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 2, 7d));
		AtomicInteger releases = new AtomicInteger();
		input.enqueue(new MaterializedCallback<>(StoreLease.create(value, releases::incrementAndGet)));
		Assert.assertNull(held.get());
		OOCStream<IndexedMatrixValue> second = fanout.getReadStream();
		AtomicInteger received = new AtomicInteger();
		second.setSubscriber(callback -> {
			try(callback) {
				if(callback.isEos())
					terminals.incrementAndGet();
				else {
					Assert.assertSame(value, callback.get());
					received.incrementAndGet();
				}
			}
		});
		Assert.assertNull(held.get());
		fanout.onPlanStarted();
		Assert.assertSame(value, held.get().get());
		Assert.assertEquals(1, received.get());
		Assert.assertEquals(0, releases.get());
		held.get().close();
		Assert.assertEquals(1, releases.get());
		input.closeInput();
		Assert.assertEquals(2, terminals.get());
		Assert.assertTrue(fanout.isProcessed());
	}

	@Test
	public void testLiveFanoutSupportsThreeConsumers() {
		MatrixObject matrix = matrixObject(2, 2, 2);
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		input.setData(matrix);
		FanoutOOCPrimitive fanout = new FanoutOOCPrimitive(input, true, 3, new StreamContext());
		IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 2, 7d));
		AtomicInteger received = new AtomicInteger();
		AtomicInteger terminals = new AtomicInteger();
		AtomicInteger releases = new AtomicInteger();
		for(int i = 0; i < 3; i++) {
			OOCStream<IndexedMatrixValue> output = fanout.getReadStream();
			output.setSubscriber(callback -> {
				try(callback) {
					if(callback.isEos())
						terminals.incrementAndGet();
					else {
						Assert.assertSame(value, callback.get());
						received.incrementAndGet();
					}
				}
			});
			if(i == 0)
				output.start();
			if(i < 2)
				Assert.assertEquals(0, received.get());
		}
		input.enqueue(new MaterializedCallback<>(StoreLease.create(value, releases::incrementAndGet)));
		Assert.assertEquals(3, received.get());
		Assert.assertEquals(1, releases.get());
		input.closeInput();
		Assert.assertEquals(3, terminals.get());
	}

	@Test(timeout = 20000)
	public void testFanoutRejectsNonMaterializedCallbacks() {
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		input.setData(matrixObject(2, 2, 2));
		FanoutOOCPrimitive fanout = new FanoutOOCPrimitive(input, true, 2, new StreamContext());
		OOCStream<IndexedMatrixValue> first = fanout.getReadStream();
		OOCStream<IndexedMatrixValue> second = fanout.getReadStream();
		first.setSubscriber(OOCStream.QueueCallback::close);
		second.setSubscriber(OOCStream.QueueCallback::close);
		first.start();
		input.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 2, 7d)));
		Assert.assertNotNull(fanout.getFailure());
		Assert.assertTrue(fanout.getFailure().getMessage().contains("requires materialized callbacks"));
		Assert.assertTrue(fanout.isProcessed());
	}

	@Test(timeout = 20000)
	public void testStateInsertionTransfersManagedPayloadWithoutReservation() throws Exception {
		OOCCacheManager.reset();
		SyncMemoryAllowance owner = new SyncMemoryAllowance(GlobalMemoryBroker.get()) {
			@Override
			public OOCFuture<Void> reserveAsync(long bytes) {
				throw new AssertionError("Insertion attempted asynchronous admission");
			}

			@Override
			public void reserveBlocking(long bytes) {
				throw new AssertionError("Insertion attempted blocking admission");
			}
		};
		SyncMemoryAllowance reader = new SyncMemoryAllowance(GlobalMemoryBroker.get());
		try(StateTable<IndexedMatrixValue> table = new StateTable<>()) {
			IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 2, 7d));
			Assert.assertTrue(owner.tryReserve(value.size()));
			try(InMemoryQueueCallback<IndexedMatrixValue> callback = new InMemoryQueueCallback<>(value, null, owner,
				value.size())) {
				StateTableUtils.put(table, 0, callback, owner);
			}
			try(StoreLease<IndexedMatrixValue> lease = table.acquire(0, reader).get()) {
				Assert.assertNotNull(lease);
				Assert.assertEquals(7, ((MatrixBlock) lease.value().getValue()).get(0, 0), 0);
			}
		}
		finally {
			reader.destroy();
			owner.destroy();
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testShapeAwareTileUpperBound() {
		long[][] shapes = {{1, 2}, {2, 1000}, {1000, 2}, {2000, 2000}, {-1, 2}, {-1, -1}};
		for(long[] shape : shapes) {
			long rows = shape[0] > 0 ? Math.min(1000, shape[0]) : 1000;
			long cols = shape[1] > 0 ? Math.min(1000, shape[1]) : 1000;
			long expected = Math.max(MatrixBlock.estimateSizeDenseInMemory(rows, cols),
				MatrixBlock.estimateSizeSparseInMemory(rows, cols, 1.0));
			Assert.assertEquals(expected, OOCUtils.estimateFullTileBytes(new MatrixCharacteristics(shape[0], shape[1], 1000)));
		}
		Assert.assertTrue(OOCUtils.estimateFullTileBytes(new MatrixCharacteristics(1000, 2, 1000)) <
			OOCUtils.estimateFullTileBytes(new MatrixCharacteristics(1000, 1000, 1000)) / 10);
	}

	@Test(timeout = 20000)
	public void testStreamingMatmulCompletesTilesBeforeEos() throws Exception {
		OOCCacheManager.reset();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> left = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> right = new SubscribableTaskQueue<>();
			MatrixObject a = matrixObject(3, 5, 2);
			MatrixObject b = matrixObject(5, 3, 2);
			MatrixObject output = matrixObject(3, 3, 2);
			a.setStreamHandle(left);
			b.setStreamHandle(right);
			ec.setVariable("A", a);
			ec.setVariable("B", b);
			ec.setVariable("Y", output);
			MMultOOCInstruction.parseInstruction("OOC°ba+*°A·MATRIX·FP64°B·MATRIX·FP64°Y·MATRIX·FP64°1")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = output.getStreamHandle();
			Assert.assertTrue(result.getPrimitive().requiredMaterializedInputs().isEmpty());
			CountDownLatch emitted = new CountDownLatch(4);
			CompletableFuture<Void> complete = new CompletableFuture<>();
			result.setSubscriber(callback -> {
				try(callback) {
					if(callback.isFailure())
						callback.get();
					else if(callback.isEos())
						complete.complete(null);
					else {
						MatrixBlock block = (MatrixBlock) callback.get().getValue();
						for(int row = 0; row < block.getNumRows(); row++)
							for(int col = 0; col < block.getNumColumns(); col++)
								Assert.assertEquals(30, block.get(row, col), 0);
						emitted.countDown();
					}
				}
				catch(Throwable failure) {
					complete.completeExceptionally(failure);
				}
			});
			result.start();
			for(int row = 1; row <= 2; row++)
				for(int inner = 1; inner <= 3; inner++)
					left.enqueue(new IndexedMatrixValue(new MatrixIndexes(row, inner),
						new MatrixBlock(row == 2 ? 1 : 2, inner == 3 ? 1 : 2, 2d)));
			for(int col = 2; col >= 1; col--)
				for(int inner = 3; inner >= 1; inner--)
					right.enqueue(new IndexedMatrixValue(new MatrixIndexes(inner, col),
						new MatrixBlock(inner == 3 ? 1 : 2, col == 2 ? 1 : 2, 3d)));
			Assert.assertTrue("Output waited for input EOS", emitted.await(10, TimeUnit.SECONDS));
			Assert.assertFalse(complete.isDone());
			left.closeInput();
			right.closeInput();
			complete.get(10, TimeUnit.SECONDS);
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testStreamingMatmulSizeSelection() {
		Assert.assertTrue(GeneralMMultOOCPrimitive.shouldStream(new MatrixCharacteristics(5, 10000, 2),
			new MatrixCharacteristics(10000, 7, 2)));
		Assert.assertFalse(GeneralMMultOOCPrimitive.shouldStream(new MatrixCharacteristics(5, 100000000, 500),
			new MatrixCharacteristics(100000000, 7, 500)));
		Assert.assertFalse(GeneralMMultOOCPrimitive.shouldStream(new MatrixCharacteristics(1000000, 1000, 500),
			new MatrixCharacteristics(1000, 1000000, 500)));
		Assert.assertFalse(GeneralMMultOOCPrimitive.shouldStream(new MatrixCharacteristics(-1, 1000, 500),
			new MatrixCharacteristics(1000, 10, 500)));
	}

	@Test
	public void testBroadcastSelectsStreamingWhenRequired() {
		for(boolean row : new boolean[] {true, false}) {
			for(boolean requireStreaming : new boolean[] {true, false}) {
				ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
				MatrixObject matrix = matrixObject(4, 4, 2);
				MatrixObject summary = matrixObject(row ? 4 : 1, row ? 1 : 4, 2);
				MatrixObject output = matrixObject(4, 4, 2);
				matrix.setStreamHandle(new SubscribableTaskQueue<>());
				summary.setStreamHandle(new SubscribableTaskQueue<>());
				ec.setVariable("X", matrix);
				ec.setVariable("S", summary);
				ec.setVariable("Y", output);
				String streaming = requireStreaming ? "°band=" + (row ? "Row" : "Col") : "";
				BinaryOOCInstruction.parseInstruction("OOC°*°X·MATRIX·FP64°S·MATRIX·FP64°Y·MATRIX·FP64" + streaming)
					.processInstruction(ec);
				Assert.assertEquals(requireStreaming ? BroadcastStreamingOOCPrimitive.class : BroadcastOOCPrimitive.class,
					output.getStreamHandle().getPrimitive().getClass());
			}
		}
	}

	@Test
	public void testStreamingBroadcastEmitsBeforeInputCompletion() throws Exception {
		OOCCacheManager.reset();
		SyncMemoryAllowance parkedOwner = new SyncMemoryAllowance(GlobalMemoryBroker.get());
		SubscribableTaskQueue<IndexedMatrixValue> tiles = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<IndexedMatrixValue> summaries = new SubscribableTaskQueue<>();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject matrix = matrixObject(4, 4, 2);
			MatrixObject summary = matrixObject(4, 1, 2);
			MatrixObject output = matrixObject(4, 4, 2);
			matrix.setStreamHandle(tiles);
			summary.setStreamHandle(summaries);
			ec.setVariable("X", matrix);
			ec.setVariable("S", summary);
			ec.setVariable("Y", output);
			BinaryOOCInstruction.parseInstruction("OOC°*°X·MATRIX·FP64°S·MATRIX·FP64°Y·MATRIX·FP64°band=Row")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = output.getStreamHandle();
			Assert.assertTrue(result.getPrimitive().requiredMaterializedInputs().isEmpty());
			AtomicInteger pendingEvents = new AtomicInteger();
			AtomicLong pendingBytes = new AtomicLong();
			((BroadcastStreamingOOCPrimitive) result.getPrimitive()).setPendingListener((band, bytes) -> {
				pendingBytes.addAndGet(bytes);
				pendingEvents.incrementAndGet();
			});
			CountDownLatch firstOutput = new CountDownLatch(1);
			CountDownLatch parked = new CountDownLatch(1);
			AtomicInteger blocks = new AtomicInteger();
			CompletableFuture<Void> complete = new CompletableFuture<>();
			result.setSubscriber(callback -> {
				try(callback) {
					if(callback.isFailure())
						callback.get();
					else if(callback.isEos())
						complete.complete(null);
					else {
						IndexedMatrixValue value = callback.get();
						double expected = value.getIndexes().getRowIndex() == 1 ? 16 : 36;
						MatrixBlock block = (MatrixBlock) value.getValue();
						for(int row = 0; row < 2; row++)
							for(int col = 0; col < 2; col++)
								Assert.assertEquals(expected, block.get(row, col), 0);
						blocks.incrementAndGet();
						firstOutput.countDown();
					}
				}
				catch(Throwable failure) {
					complete.completeExceptionally(failure);
				}
			});
			result.start();
			IndexedMatrixValue waiting = new IndexedMatrixValue(new MatrixIndexes(2, 2), new MatrixBlock(2, 2, 3d));
			tiles.enqueue(new MaterializedCallback<>(StoreLease.create(waiting, parked::countDown)));
			Assert.assertTrue("Unmatched input callback was not released", parked.await(10, TimeUnit.SECONDS));
			summaries.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 1, 8d)));
			tiles.enqueue(new IndexedMatrixValue(new MatrixIndexes(1, 2), new MatrixBlock(2, 2, 2d)));
			Assert.assertTrue("Broadcast waited for input completion", firstOutput.await(10, TimeUnit.SECONDS));
			Assert.assertFalse(complete.isDone());
			summaries.enqueue(new IndexedMatrixValue(new MatrixIndexes(2, 1), new MatrixBlock(2, 1, 12d)));
			tiles.enqueue(new IndexedMatrixValue(new MatrixIndexes(2, 1), new MatrixBlock(2, 2, 3d)));
			IndexedMatrixValue parkedValue = new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 2, 2d));
			parkedOwner.reserveBlocking(parkedValue.size());
			InMemoryQueueCallback<IndexedMatrixValue> parkedCallback = new InMemoryQueueCallback<>(parkedValue, null,
				parkedOwner, parkedValue.size());
			Assert.assertTrue(parkedCallback.tryPark(OOCCacheManager.getGlobalCache()) > 0);
			Assert.assertTrue(parkedCallback.isParked());
			tiles.enqueue(parkedCallback);
			tiles.closeInput();
			summaries.closeInput();
			complete.get(10, TimeUnit.SECONDS);
			Assert.assertEquals(4, blocks.get());
			Assert.assertEquals(8, pendingEvents.get());
			Assert.assertEquals(0, pendingBytes.get());
			Assert.assertEquals(0, parkedOwner.getUsedMemory());
		}
		finally {
			parkedOwner.destroy();
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testBandStreamingRecognitionThroughTees() {
		Hop x = new DataOp("X", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			40000, 200, -1, 100);
		Hop tee = HopRewriteUtils.createDataOp("tee", x, OpOpData.TEE);
		Hop nested = HopRewriteUtils.createDataOp("nested", tee, OpOpData.TEE);
		Hop sums = HopRewriteUtils.createAggUnaryOp(nested, AggOp.SUM, Direction.Row);
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandStreamingDirection(tee, sums));
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandStreamingDirection(x,
			HopRewriteUtils.createDataOp("summaryTee", sums, OpOpData.TEE)));
		Assert.assertEquals(Direction.Col, HopRewriteUtils.getBandStreamingDirection(nested,
			HopRewriteUtils.createAggUnaryOp(x, AggOp.SUM, Direction.Col)));
		Assert.assertNull(HopRewriteUtils.getBandStreamingDirection(x,
			HopRewriteUtils.createAggUnaryOp(x, AggOp.SUM, Direction.RowCol)));
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandStreamingDirection(x,
			HopRewriteUtils.createAggUnaryOp(x, AggOp.MIN, Direction.Row)));
		Hop other = new DataOp("X", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			40000, 200, -1, 100);
		Assert.assertNull(HopRewriteUtils.getBandStreamingDirection(other, sums));
	}

	@Test
	public void testSingleContributorAppendPropagatesDimensions() {
		MatrixObject empty = matrixObject(0, 4, 2);
		MatrixObject input = matrixObject(2, 4, 2);
		MatrixObject output = matrixObject(-1, -1, 2);
		empty.setStreamHandle(new SubscribableTaskQueue<>());
		input.setStreamHandle(new SubscribableTaskQueue<>());

		AppendOOCInstruction.bind(List.of(empty, input), output, false, new StreamContext());

		Assert.assertEquals(2, output.getNumRows());
		Assert.assertEquals(4, output.getNumColumns());
		Assert.assertEquals(2, output.getBlocksize());
	}

	@Test
	public void testZeroRowDataGenPropagatesRuntimeDimensions() {
		ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
		ec.setScalarOutput("rows", new IntObject(0));
		ec.setScalarOutput("cols", new IntObject(3));
		ec.setVariable("R", matrixObject(-1, -1, 2));

		DataGenOOCInstruction
			.parseInstruction(
				"OOC°rand°rows·SCALAR·INT64·false°cols·SCALAR·INT64·false°2°0°0°1.0°-1°uniform°1.0°1°R·MATRIX·FP64")
			.processInstruction(ec);

		Assert.assertEquals(0, ec.getMatrixObject("R").getNumRows());
		Assert.assertEquals(3, ec.getMatrixObject("R").getNumColumns());
		Assert.assertEquals(0, ec.getMatrixObject("R").getNnz());
	}

	@Test
	public void testDimensionPropagationThroughElementwiseChain() {
		ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
		MatrixObject input = matrixObject(7, 3, 2);
		input.setStreamHandle(new SubscribableTaskQueue<>());
		ec.setVariable("A", input);
		for(String name : List.of("B", "C", "D", "E"))
			ec.setVariable(name, matrixObject(-1, -1, 2));

		BinaryOOCInstruction.parseInstruction("OOC°/°A·MATRIX·FP64°2·SCALAR·FP64·true°B·MATRIX·FP64")
			.processInstruction(ec);
		BinaryOOCInstruction.parseInstruction("OOC°-°B·MATRIX·FP64°1·SCALAR·FP64·true°C·MATRIX·FP64")
			.processInstruction(ec);
		BinaryOOCInstruction.parseInstruction("OOC°*°C·MATRIX·FP64°0.5·SCALAR·FP64·true°D·MATRIX·FP64")
			.processInstruction(ec);
		ParameterizedBuiltinOOCInstruction
			.parseInstruction("OOC°replace°pattern=NaN°replacement=0°target=D°E·MATRIX·FP64").processInstruction(ec);

		Assert.assertEquals(7, ec.getMatrixObject("E").getNumRows());
		Assert.assertEquals(3, ec.getMatrixObject("E").getNumColumns());
		Assert.assertEquals(2, ec.getMatrixObject("E").getBlocksize());
	}

	@Test
	public void testUpperTriangular() {
		ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
		SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
		MatrixObject source = matrixObject(4, 4, 2);
		MatrixObject output = matrixObject(-1, -1, 2);
		source.setStreamHandle(input);
		ec.setVariable("A", source);
		ec.setVariable("R", output);
		for(int row = 1; row <= 2; row++)
			for(int col = 1; col <= 2; col++)
				input.enqueue(new IndexedMatrixValue(new MatrixIndexes(row, col), new MatrixBlock(2, 2, 1d)));
		input.closeInput();

		ParameterizedBuiltinOOCInstruction
			.parseInstruction("OOC°uppertri°target=A°diag=false°values=true°R·MATRIX·FP64")
			.processInstruction(ec);
		OOCStream<IndexedMatrixValue> result = output.getStreamHandle();
		result.start();
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = result.dequeueCB()) != null)
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				IndexedMatrixValue value = current.get();
				MatrixIndexes indexes = value.getIndexes();
				MatrixBlock block = (MatrixBlock) value.getValue();
				if(indexes.getRowIndex() > indexes.getColumnIndex())
					Assert.assertEquals(0, block.getNonZeros());
				else if(indexes.getRowIndex() < indexes.getColumnIndex())
					Assert.assertEquals(4, block.getNonZeros());
				else {
					Assert.assertEquals(0, block.get(0, 0), 0);
					Assert.assertEquals(1, block.get(0, 1), 0);
					Assert.assertEquals(0, block.get(1, 0), 0);
					Assert.assertEquals(0, block.get(1, 1), 0);
				}
			}
	}

	@Test
	public void testSliceLineCtable() {
		OOCCacheManager.reset();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> rows = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> cols = new SubscribableTaskQueue<>();
			MatrixObject rowInput = matrixObject(4, 1, 2);
			MatrixObject colInput = matrixObject(4, 1, 2);
			MatrixObject output = matrixObject(4, 4, 2);
			rowInput.setStreamHandle(rows);
			colInput.setStreamHandle(cols);
			ec.setVariable("A", rowInput);
			ec.setVariable("B", colInput);
			ec.setVariable("R", output);
			for(int blockIndex = 1; blockIndex <= 2; blockIndex++) {
				MatrixBlock categories = new MatrixBlock(2, 1, false);
				categories.set(0, 0, 2 * blockIndex - 1);
				categories.set(1, 0, 2 * blockIndex);
				rows.enqueue(new IndexedMatrixValue(new MatrixIndexes(blockIndex, 1), categories));
				cols.enqueue(new IndexedMatrixValue(new MatrixIndexes(blockIndex, 1), new MatrixBlock(categories)));
			}
			rows.closeInput();
			cols.closeInput();

			CtableOOCInstruction.parseInstruction(
				"OOC°ctable°A·MATRIX·FP64°B·MATRIX·FP64°1·SCALAR·FP64·true" + "°4·true°4·true°R·MATRIX·FP64°false°22")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = output.getStreamHandle();
			result.start();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					MatrixBlock block = (MatrixBlock) value.getValue();
					if(value.getIndexes().getRowIndex() == value.getIndexes().getColumnIndex()) {
						Assert.assertEquals(1, block.get(0, 0), 0);
						Assert.assertEquals(1, block.get(1, 1), 0);
					}
					else
						Assert.assertEquals(0, block.getNonZeros());
					blocks++;
				}
			Assert.assertEquals(4, blocks);
			Assert.assertNull(OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testSubmitTasksClosesCallbacksAfterCompletion() throws Exception {
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		AtomicInteger processed = new AtomicInteger();
		AtomicInteger released = new AtomicInteger();
		CompletableFuture<Void> completion = OOCInstructionUtils.submitOOCTasks(source, callback -> {
			Assert.assertEquals(1, callback.get().getIndexes().getRowIndex());
			processed.incrementAndGet();
		}, new StreamContext().addOutStream());

		IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, 1.0));
		source.enqueue(new MaterializedCallback<>(StoreLease.create(value, released::incrementAndGet)));
		source.closeInput();
		completion.get(10, TimeUnit.SECONDS);

		Assert.assertEquals(1, processed.get());
		Assert.assertEquals(1, released.get());
	}

	@Test
	public void testSubmitCloseableOOCTasks() throws Exception {
		SubscribableTaskQueue<OwnedTask> source = new SubscribableTaskQueue<>();
		AtomicInteger processed = new AtomicInteger();
		AtomicInteger closed = new AtomicInteger();
		CompletableFuture<Void> completion = OOCInstructionUtils.submitCloseableOOCTasks(source,
			(OwnedTask work) -> processed.addAndGet(work._value), new StreamContext().addOutStream());

		source.enqueue(new OwnedTask(1, closed));
		source.enqueue(new OwnedTask(2, closed));
		source.closeInput();
		completion.get(10, TimeUnit.SECONDS);

		Assert.assertEquals(3, processed.get());
		Assert.assertEquals(2, closed.get());
	}

	private static MatrixObject matrixObject(long rows, long cols, int blocksize) {
		return new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(rows, cols, blocksize, rows * cols), FileFormat.BINARY));
	}

	private static final class OwnedTask implements AutoCloseable {
		private final int _value;
		private final AtomicInteger _closed;

		private OwnedTask(int value, AtomicInteger closed) {
			_value = value;
			_closed = closed;
		}

		@Override
		public void close() {
			_closed.incrementAndGet();
		}
	}

	@Test
	public void testSubmitTasksWaitsForAllStreams() throws Exception {
		SubscribableTaskQueue<Integer> first = new SubscribableTaskQueue<>();
		SubscribableTaskQueue<Integer> second = new SubscribableTaskQueue<>();
		AtomicInteger processed = new AtomicInteger();
		CompletableFuture<Void> completion = OOCInstructionUtils.submitOOCTasks(List.of(first, second),
			(index, callback) -> processed.addAndGet(callback.get()), new StreamContext().addOutStream());

		first.enqueue(1);
		first.closeInput();
		second.enqueue(2);
		Assert.assertFalse(completion.isDone());
		second.closeInput();
		completion.get(10, TimeUnit.SECONDS);
		Assert.assertEquals(3, processed.get());
	}

	@Test
	public void testSubmitTaskPropagatesFailure() throws Exception {
		SubscribableTaskQueue<IndexedMatrixValue> output = new SubscribableTaskQueue<>();
		output.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(1, 1, 1), FileFormat.BINARY)));
		AtomicReference<DMLRuntimeException> propagated = new AtomicReference<>();
		output.setSubscriber(callback -> {
			try(callback) {
				if(callback.isFailure()) {
					try {
						callback.get();
					}
					catch(DMLRuntimeException failure) {
						propagated.compareAndSet(null, failure);
					}
				}
			}
		});
		try {
			output.closeInput();
			Assert.fail("Expected block-count failure");
		}
		catch(DMLRuntimeException expected) {
		}

		OOCFuture<Void> completion = OOCInstructionUtils.submitOOCTask(() -> {
			throw new DMLRuntimeException("injected failure");
		}, new StreamContext().addOutStream(output));
		try {
			completion.get(10, TimeUnit.SECONDS);
			Assert.fail("Expected task failure");
		}
		catch(ExecutionException expected) {
			Assert.assertTrue(expected.getCause() instanceof DMLRuntimeException);
		}
		Assert.assertNotNull(propagated.get());
		Assert.assertEquals("injected failure", propagated.get().getMessage());
	}
}
