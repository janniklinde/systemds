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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.AggOp;
import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.Direction;
import org.apache.sysds.common.Types.OpOpData;
import org.apache.sysds.common.Types.OpOp1;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
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

public class OOCInstructionUtilsTest {
	@Test
	public void testSubscriberDoesNotRunUnderQueueMonitor() {
		SubscribableTaskQueue<Integer> queue = new SubscribableTaskQueue<>();
		AtomicInteger delivered = new AtomicInteger();
		queue.setSubscriber(callback -> {
			try(callback) {
				Assert.assertFalse("A subscriber must not block purge by holding its queue monitor",
					Thread.holdsLock(queue));
				delivered.incrementAndGet();
			}
		});
		queue.enqueueTask(new OOCStream.SimpleQueueCallback<>(1, null));
		queue.closeInput();
		Assert.assertEquals(2, delivered.get());
	}

	@Test(timeout = 10000)
	public void testSpillableQueueDoesNotBlockAtLegacyItemLimit() throws Exception {
		SubscribableTaskQueue<Integer> queue = new SubscribableTaskQueue<>();
		AtomicReference<Throwable> failure = new AtomicReference<>();
		Thread producer = new Thread(() -> {
			try {
				for(int i = 0; i < 100005; i++)
					queue.enqueue(i);
				queue.closeInput();
			}
			catch(Throwable error) {
				failure.set(error);
			}
		});
		producer.setDaemon(true);
		producer.start();
		producer.join(5000);
		Assert.assertFalse("OOC producers must not block at the in-memory queue item limit", producer.isAlive());
		Assert.assertNull(failure.get());
		AtomicInteger delivered = new AtomicInteger();
		AtomicInteger terminals = new AtomicInteger();
		queue.setSubscriber(callback -> {
			try(callback) {
				if(callback.isEos())
					terminals.incrementAndGet();
				else
					Assert.assertEquals(delivered.getAndIncrement(), callback.get().intValue());
			}
		});
		Assert.assertEquals(100005, delivered.get());
		Assert.assertEquals(1, terminals.get());
	}

	@Test
	public void testSubscriberDrainKeepsBacklogPurgeVisible() {
		class InspectableQueue extends SubscribableTaskQueue<Integer> {
			synchronized int buffered() {
				return _data.size();
			}
		}
		InspectableQueue queue = new InspectableQueue();
		queue.enqueue(1);
		queue.enqueue(2);
		queue.closeInput();
		queue.setSubscriber(callback -> {
			try(callback) {
				if(!callback.isEos() && callback.get() == 1)
					Assert.assertEquals("Undelivered callbacks must remain visible to purge", 1, queue.buffered());
			}
		});
		Assert.assertEquals(0, queue.buffered());
	}

	@Test
	public void testBandStreamingThroughSkinnyMatmulAndSquare() {
		Hop x = new DataOp("X", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			40000, 200, -1, 100);
		Hop c = new DataOp("Ct", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			200, 10, -1, 100);
		Hop product = HopRewriteUtils.createMatrixMultiply(x, c);
		Hop squared = HopRewriteUtils.createUnary(product, OpOp1.POW2);
		Hop sums = HopRewriteUtils.createAggUnaryOp(squared, AggOp.SUM, Direction.Row);
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandStreamingDirection(x, sums));
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
			Assert.assertEquals(expected + 72,
				OOCUtils.estimateFullTileBytes(new MatrixCharacteristics(shape[0], shape[1], 1000)));
		}
		Assert.assertTrue(OOCUtils.estimateFullTileBytes(new MatrixCharacteristics(1000, 2, 1000)) <
			OOCUtils.estimateFullTileBytes(new MatrixCharacteristics(1000, 1000, 1000)) / 10);
	}

	@Test
	public void testFullTileEstimateCoversResidentCharge() {
		int[][] shapes = {{1, 1}, {2, 2}, {3, 3}, {8, 8}, {50, 50}, {100, 100}, {1000, 1},
			{1, 1000}, {1000, 2}, {2, 1000}, {1000, 1000}};
		for(int[] shape : shapes) {
			int rows = shape[0];
			int cols = shape[1];
			long bound = OOCUtils.estimateFullTileBytes(new MatrixCharacteristics(rows, cols, 1000));
			MatrixBlock dense = new MatrixBlock(rows, cols, false);
			dense.allocateDenseBlock();
			Assert.assertTrue(bound >= OOCUtils.memoryCharge(new IndexedMatrixValue(new MatrixIndexes(1, 1), dense)));
			MatrixBlock sparse = new MatrixBlock(rows, cols, true);
			for(int row = 0; row < rows; row++)
				sparse.set(row, row % cols, 1);
			sparse.recomputeNonZeros();
			Assert.assertTrue(bound >= OOCUtils.memoryCharge(new IndexedMatrixValue(new MatrixIndexes(1, 1), sparse)));
		}
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
	public void testBandStreamingRecognition() {
		Hop x = new DataOp("X", DataType.MATRIX, ValueType.FP64, OpOpData.TRANSIENTREAD, null,
			40000, 200, -1, 100);
		Hop sums = HopRewriteUtils.createAggUnaryOp(x, AggOp.SUM, Direction.Row);
		Assert.assertEquals(Direction.Row, HopRewriteUtils.getBandStreamingDirection(x, sums));
		Assert.assertEquals(Direction.Col, HopRewriteUtils.getBandStreamingDirection(x,
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
