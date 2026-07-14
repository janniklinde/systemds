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

package org.apache.sysds.test.component.ooc.memory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedView;
import org.apache.sysds.runtime.ooc.planning.OOCRegionBinding;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.primitives.JoinOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.store.IndexedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.OrderedMaterializedStoreReader;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.stream.message.OOCStreamMessage;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.util.IndexRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Correctness of the migrated equi-join (StateTable rendezvous over the global cache via
 * TableRendezvous) through the real planner pipeline.
 */
public class JoinPrimitiveParityTest {
	private static final int ROWS = 2000;
	private static final int COLS = 2000;
	private static final int BLEN = 250;
	private static final int ROW_BLOCKS = ROWS / BLEN;
	private static final int COL_BLOCKS = COLS / BLEN;
	private static final long WAIT_TIMEOUT_SEC = 60;

	@After
	public void tearDown() {
		OOCCacheManager.reset();
	}

	@Test
	public void testTablePathProducesExpectedJoin() throws Exception {
		Map<MatrixIndexes, Double> table = run();

		Assert.assertEquals(ROW_BLOCKS * COL_BLOCKS, table.size());
		for(int rb = 1; rb <= ROW_BLOCKS; rb++) {
			for(int cb = 1; cb <= COL_BLOCKS; cb++) {
				Double sum = table.get(new MatrixIndexes(rb, cb));
				Assert.assertNotNull("Missing output tile (" + rb + "," + cb + ")", sum);
				Assert.assertEquals(8.0 * BLEN * BLEN, sum, 1e-9);
			}
		}
		//every rendezvous slot was taken when its pair resolved and the table closed
		awaitOwnedCache(OOCCacheManager.getGlobalCache(), 0);
	}

	@Test
	public void testTablePathUsesExactOutputCallbackOwnership() throws Exception {
		final int tiles = 3;
		final int rows = tiles * BLEN;
		final int cols = BLEN;
		final long bytes = new MatrixBlock(BLEN, BLEN, 1.0).getExactSerializedSize();
		CappedAllowance region = new CappedAllowance(32 * bytes);
		CappedAllowance producer = new CappedAllowance(16 * bytes);
		OOCStream<IndexedMatrixValue> left = createMatrixStream(rows, cols);
		OOCStream<IndexedMatrixValue> right = createMatrixStream(rows, cols);
		OOCStream<IndexedMatrixValue> out = createMatrixStream(rows, cols);
		AtomicInteger outputs = new AtomicInteger();
		CompletableFuture<Void> done = new CompletableFuture<>();

		try {
			for(int i = 1; i <= tiles; i++) {
				enqueueManaged(left, i, 3.0, producer, bytes);
				enqueueManaged(right, i, 5.0, producer, bytes);
			}
			left.closeInput();
			right.closeInput();
			out.setSubscriber(cb -> {
				try {
					if(cb.isEos()) {
						done.complete(null);
						return;
					}
					outputs.incrementAndGet();
				}
				catch(Throwable t) {
					done.completeExceptionally(t);
				}
				finally {
					cb.close();
				}
			});

			JoinOOCPrimitive primitive = new JoinOOCPrimitive(List.of(left, right), out, blocks -> {
				BinaryOperator plus = new BinaryOperator(Plus.getPlusFnObject());
				return blocks.get(0).binaryOperations(plus, blocks.get(1));
			}, new StreamContext(0, "op_join_prepaid_regression").addOutStream(out));
			primitive.bindRegion(new OOCRegionBinding(region, new AtomicInteger(1)));
			primitive.startExecution();

			done.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertEquals(tiles, outputs.get());
			awaitOwnedCache(OOCCacheManager.getGlobalCache(), 0);
			waitFor(() -> region.getUsedMemory() == 0);
			Assert.assertEquals(0, region.getUsedMemory());
			waitFor(() -> producer.getUsedMemory() == 0);
			Assert.assertEquals(0, producer.getUsedMemory());
		}
		finally {
			region.shutdown();
			producer.shutdown();
		}
	}

	@Test
	public void testJoinKeepsArrivingInputChargedToProducer() throws Exception {
		final int rows = BLEN;
		final int cols = BLEN;
		final long bytes = new MatrixBlock(BLEN, BLEN, 1.0).getExactSerializedSize();
		CappedAllowance region = new CappedAllowance(8 * bytes);
		CappedAllowance producer = new CappedAllowance(4 * bytes);
		OOCStream<IndexedMatrixValue> left = createMatrixStream(rows, cols);
		OOCStream<IndexedMatrixValue> right = createMatrixStream(rows, cols);
		OOCStream<IndexedMatrixValue> out = createMatrixStream(rows, cols);
		AtomicLong producerBytesDuringJoin = new AtomicLong(-1);
		CompletableFuture<Void> done = new CompletableFuture<>();

		try {
			enqueueManaged(left, 1, 3.0, producer, bytes);
			enqueueManaged(right, 1, 5.0, producer, bytes);
			left.closeInput();
			right.closeInput();
			out.setSubscriber(cb -> {
				try {
					if(cb.isEos())
						done.complete(null);
				}
				catch(Throwable t) {
					done.completeExceptionally(t);
				}
				finally {
					cb.close();
				}
			});

			JoinOOCPrimitive primitive = new JoinOOCPrimitive(List.of(left, right), out, blocks -> {
				producerBytesDuringJoin.set(producer.getUsedMemory());
				BinaryOperator plus = new BinaryOperator(Plus.getPlusFnObject());
				return blocks.get(0).binaryOperations(plus, blocks.get(1));
			}, new StreamContext(0, "op_join_input_owner_regression").addOutStream(out));
			primitive.bindRegion(new OOCRegionBinding(region, new AtomicInteger(1)));
			primitive.startExecution();

			done.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertTrue("Join moved its arriving input reservation away from the producer allowance.",
				producerBytesDuringJoin.get() > 0);
			waitFor(() -> region.getUsedMemory() == 0);
			waitFor(() -> producer.getUsedMemory() == 0);
			Assert.assertEquals(0, region.getUsedMemory());
			Assert.assertEquals(0, producer.getUsedMemory());
		}
		finally {
			region.shutdown();
			producer.shutdown();
		}
	}

	@Test
	public void testJoinAddsPolicyToAlreadyMaterializedInputWithoutReleasingView() throws Exception {
		final int rows = 2 * BLEN;
		final int cols = 2 * BLEN;
		final long bytes = new MatrixBlock(BLEN, BLEN, 1.0).getExactSerializedSize();
		CappedAllowance region = new CappedAllowance(8 * bytes);
		SubscribableTaskQueue<IndexedMatrixValue> left = (SubscribableTaskQueue<IndexedMatrixValue>)
			createMatrixStream(rows, cols);
		SubscribableTaskQueue<IndexedMatrixValue> right = (SubscribableTaskQueue<IndexedMatrixValue>)
			createMatrixStream(rows, cols);
		OOCStream<IndexedMatrixValue> out = createMatrixStream(rows, cols);
		RecordingMaterializedView view = new RecordingMaterializedView();
		MaterializedPolicyStreamable materializedLeft = new MaterializedPolicyStreamable(left, view);
		CompletableFuture<Void> done = new CompletableFuture<>();
		CappedAllowance producer = new CappedAllowance(16 * bytes);
		AtomicInteger outputs = new AtomicInteger();

		try {
			for(int r = 1; r <= 2; r++) {
				for(int c = 1; c <= 2; c++) {
					enqueueManaged(left, r, c, 3.0, producer, bytes);
					enqueueManaged(right, r, c, 5.0, producer, bytes);
				}
			}
			left.closeInput();
			right.closeInput();
			out.setSubscriber(cb -> {
				try {
					if(cb.isEos())
						done.complete(null);
					else
						outputs.incrementAndGet();
				}
				catch(Throwable t) {
					done.completeExceptionally(t);
				}
				finally {
					cb.close();
				}
			});

			JoinOOCPrimitive primitive = new JoinOOCPrimitive(List.of(materializedLeft, right), out,
			blocks -> blocks.get(0), new StreamContext(0, "op_join_policy_injection").addOutStream(out));
		primitive.requestPattern(OOCAccessPattern.COL_MAJOR);
		primitive.bindRegion(new OOCRegionBinding(region, new AtomicInteger(1)));
		primitive.startExecution();

			done.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.assertEquals(4, outputs.get());
			Assert.assertEquals("Join should attach a consumer policy to an already-materialized input.",
				1, view.policyCount.get());
			Assert.assertEquals("Join did not declare this materialized input, so it must not release the view.",
				0, view.closeCount.get());
			Assert.assertNotNull(view.policy);
			Assert.assertEquals("The injected policy should follow the planner-requested column-major rank.",
				2, view.policy.applyAsLong(new MatrixIndexes(1, 2)));
		}
		finally {
			region.shutdown();
			producer.shutdown();
		}
	}

	private Map<MatrixIndexes, Double> run() throws Exception {
		OOCStream<IndexedMatrixValue> left = createMatrixStream(ROWS, COLS);
		OOCInstructionUtils.dataGen(left, ix -> new MatrixBlock(OOCUtils.getNumRowsOfTile(ix, ROWS, BLEN),
			OOCUtils.getNumColsOfTile(ix, COLS, BLEN), 3.0),
			new StreamContext(0, "op_datagen_left").addOutStream(left));

		OOCStream<IndexedMatrixValue> right = createMatrixStream(ROWS, COLS);
		OOCInstructionUtils.dataGen(right, ix -> new MatrixBlock(OOCUtils.getNumRowsOfTile(ix, ROWS, BLEN),
			OOCUtils.getNumColsOfTile(ix, COLS, BLEN), 5.0),
			new StreamContext(0, "op_datagen_right").addOutStream(right));

		OOCStream<IndexedMatrixValue> out = createMatrixStream(ROWS, COLS);
		BinaryOperator plus = new BinaryOperator(Plus.getPlusFnObject());
		OOCInstructionUtils.equiJoin(List.of(left, right), out,
			blocks -> blocks.get(0).binaryOperations(plus, blocks.get(1)),
			new StreamContext(0, "op_join").addOutStream(out));

		Map<MatrixIndexes, Double> results = new ConcurrentHashMap<>();
		CompletableFuture<Void> done = new CompletableFuture<>();
		out.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					done.complete(null);
					return;
				}
				IndexedMatrixValue imv = cb.get();
				results.put(new MatrixIndexes(imv.getIndexes()), ((MatrixBlock) imv.getValue()).sum());
			}
			catch(Throwable t) {
				done.completeExceptionally(t);
			}
			finally {
				cb.close();
			}
		});

		out.start();
		done.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		return new HashMap<>(results);
	}

	private static OOCStream<IndexedMatrixValue> createMatrixStream(int rows, int cols) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixCharacteristics dc = new MatrixCharacteristics(rows, cols, BLEN, -1);
		stream.setData(new MatrixObject(ValueType.FP64, null, new MetaDataFormat(dc, FileFormat.BINARY)));
		return stream;
	}

	private static void enqueueManaged(OOCStream<IndexedMatrixValue> stream, int row, double value,
		MemoryAllowance allowance, long bytes) {
		allowance.reserveBlocking(bytes);
		stream.enqueue(new InMemoryQueueCallback(new IndexedMatrixValue(new MatrixIndexes(row, 1),
			new MatrixBlock(BLEN, BLEN, value)), null, allowance, bytes));
	}

	private static void enqueueManaged(OOCStream<IndexedMatrixValue> stream, int row, int col, double value,
		MemoryAllowance allowance, long bytes) {
		allowance.reserveBlocking(bytes);
		stream.enqueue(new InMemoryQueueCallback(new IndexedMatrixValue(new MatrixIndexes(row, col),
			new MatrixBlock(BLEN, BLEN, value)), null, allowance, bytes));
	}

	private static void awaitOwnedCache(OOCCache cache, long expected) throws Exception {
		waitFor(() -> cache.getOwnedCacheSize() == expected);
		Assert.assertEquals(expected, cache.getOwnedCacheSize());
	}

	private static void waitFor(BooleanSupplier condition) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_TIMEOUT_SEC);
		while(!condition.getAsBoolean() && System.nanoTime() < deadline)
			Thread.sleep(1);
	}

	private static final class CappedAllowance implements MemoryAllowance {
		private final long _limit;
		private long _used;
		private boolean _shutdown;

		private CappedAllowance(long limit) {
			_limit = limit;
		}

		@Override
		public synchronized boolean tryReserve(long bytes) {
			if(_shutdown)
				return false;
			if(bytes < 0 || bytes > _limit || _used + bytes > _limit)
				return false;
			_used += bytes;
			notifyAll();
			return true;
		}

		@Override
		public synchronized void reserveBlocking(long bytes) {
			while(!tryReserve(bytes)) {
				if(_shutdown)
					throw new IllegalStateException("Cannot reserve memory on closed allowance.");
				try {
					wait();
				}
				catch(InterruptedException ex) {
					Thread.currentThread().interrupt();
					throw new DMLRuntimeException(ex);
				}
			}
		}

		@Override
		public CompletableFuture<Void> reserve(long bytes) {
			return CompletableFuture.runAsync(() -> reserveBlocking(bytes));
		}

		@Override
		public synchronized void release(long bytes) {
			if(bytes < 0 || bytes > _used)
				throw new IllegalArgumentException("Invalid allowance release: " + bytes + ", used=" + _used);
			_used -= bytes;
			notifyAll();
		}

		@Override
		public synchronized long getUsedMemory() {
			return _used;
		}

		@Override
		public long getGrantedMemory() {
			return _limit;
		}

		@Override
		public long getTargetMemory() {
			return _limit;
		}

		@Override
		public void setTargetMemory(long targetMemory) {
			// Fixed-capacity test allowance.
		}

		@Override
		public synchronized void shutdown() {
			_shutdown = true;
			notifyAll();
		}

		@Override
		public synchronized boolean isShutdown() {
			return _shutdown;
		}
	}

	private static final class RecordingMaterializedView extends OOCMaterializedView {
		private final AtomicInteger policyCount = new AtomicInteger();
		private final AtomicInteger closeCount = new AtomicInteger();
		private volatile ToLongFunction<MatrixIndexes> policy;

		public RecordingMaterializedView() {
			super(null, null, -1, OOCStoreLayout.of(t -> 0,
				i -> new MatrixIndexes(0, 0)), null, 0, 1);
		}

		@Override
		public OOCFuture<Void> completion() {
			return OOCFuture.completed(null);
		}

		@Override
		public OOCFuture<Void> readersSealed() {
			return OOCFuture.completed(null);
		}

		@Override
		public void addEvictionPolicy(ToLongFunction<MatrixIndexes> policy) {
			this.policy = policy;
			policyCount.incrementAndGet();
		}

		@Override
		public OrderedMaterializedStoreReader<IndexedMatrixValue> openReader(MaterializedStore.AccessPattern pattern,
			MemoryAllowance allowance, int maxPrefetch) {
			throw new UnsupportedOperationException();
		}

		@Override
		public IndexedMaterializedStoreReader<IndexedMatrixValue> openIndexedReader(
			MaterializedStore.Liveness liveness) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() {
			closeCount.incrementAndGet();
		}
	}

	private static final class MaterializedPolicyStreamable implements OOCStreamable<IndexedMatrixValue> {
		private final OOCStream<IndexedMatrixValue> stream;
		private final OOCMaterializedView view;

		private MaterializedPolicyStreamable(OOCStream<IndexedMatrixValue> stream, OOCMaterializedView view) {
			this.stream = stream;
			this.view = view;
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReadStream() {
			return stream;
		}

		@Override
		public OOCStream<IndexedMatrixValue> getWriteStream() {
			return stream;
		}

		@Override
		public boolean hasStreamCache() {
			return stream.hasStreamCache();
		}

		@Override
		public org.apache.sysds.runtime.instructions.ooc.CachingStream getStreamCache() {
			return stream.getStreamCache();
		}

		@Override
		public boolean hasMaterializedView() {
			return true;
		}

		@Override
		public OOCMaterializedView materializedView() {
			return view;
		}

		@Override
		public boolean isProcessed() {
			return stream.isProcessed();
		}

		@Override
		public DataCharacteristics getDataCharacteristics() {
			return stream.getDataCharacteristics();
		}

		@Override
		public CacheableData<?> getData() {
			return stream.getData();
		}

		@Override
		public void setData(CacheableData<?> data) {
			stream.setData(data);
		}

		@Override
		public void messageUpstream(OOCStreamMessage msg) {
			stream.messageUpstream(msg);
		}

		@Override
		public void messageDownstream(OOCStreamMessage msg) {
			stream.messageDownstream(msg);
		}

		@Override
		public void setUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
			stream.setUpstreamMessageRelay(relay);
		}

		@Override
		public void setDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
			stream.setDownstreamMessageRelay(relay);
		}

		@Override
		public void addUpstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
			stream.addUpstreamMessageRelay(relay);
		}

		@Override
		public void addDownstreamMessageRelay(Consumer<OOCStreamMessage> relay) {
			stream.addDownstreamMessageRelay(relay);
		}

		@Override
		public void clearUpstreamMessageRelays() {
			stream.clearUpstreamMessageRelays();
		}

		@Override
		public void clearDownstreamMessageRelays() {
			stream.clearDownstreamMessageRelays();
		}

		@Override
		public void setIXTransform(BiFunction<Boolean, IndexRange, IndexRange> transform) {
			stream.setIXTransform(transform);
		}

		@Override
		public BiFunction<Boolean, IndexRange, IndexRange> getIXTransform() {
			return stream.getIXTransform();
		}

		@Override
		public OOCPrimitive getPrimitive() {
			return stream.getPrimitive();
		}
	}
}
