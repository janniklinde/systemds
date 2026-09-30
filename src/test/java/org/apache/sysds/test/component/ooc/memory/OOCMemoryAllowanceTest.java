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

import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.ooc.OOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.MemoryBroker;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.OwnedGroupQueueCallback;
import org.apache.sysds.test.component.ooc.cache.OOCCacheTestUtils;
import org.apache.sysds.utils.stats.InfrastructureAnalyzer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import scala.Tuple3;

import java.util.ArrayList;
import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class OOCMemoryAllowanceTest {
	private DMLConfig _previousConfig;

	@Before
	public void configureSmallBrokers() {
		_previousConfig = ConfigurationManager.getDMLConfig();
		DMLConfig config = new DMLConfig();
		config.setTextValue(DMLConfig.OOC_MEM_BROKER_STRICT_FREE, "0");
		config.setTextValue(DMLConfig.OOC_MEM_BROKER_STRICT_FRACTION, "0.8");
		config.setTextValue(DMLConfig.OOC_MEM_BROKER_PURGE_FREE, "0");
		ConfigurationManager.setLocalConfig(config);
	}

	@After
	public void restoreConfig() {
		ConfigurationManager.setLocalConfig(_previousConfig);
	}

	@Test
	public void testConfiguredStrictThresholds() {
		long mb = 1L << 20;
		DMLConfig config = ConfigurationManager.getDMLConfig();
		config.setTextValue(DMLConfig.OOC_MEM_BROKER_STRICT_FREE, String.valueOf(100 * mb));
		config.setTextValue(DMLConfig.OOC_MEM_BROKER_STRICT_FRACTION, "0.85");
		for(long capacity : new long[] {1000 * mb, 256 * mb}) {
			GlobalMemoryBroker broker = new GlobalMemoryBroker(capacity);
			SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
			long threshold = capacity == 1000 * mb ? 850 * mb : 156 * mb;
			try {
				allowance.setTargetMemory(threshold - mb);
				allowance.reserveBlocking(threshold - mb);
				Assert.assertFalse(broker.isStrictMode());
				allowance.setTargetMemory(threshold);
				allowance.reserveBlocking(mb);
				Assert.assertTrue(broker.isStrictMode());
				allowance.release(threshold);
				Assert.assertFalse(broker.isStrictMode());
			}
			finally {
				allowance.destroy();
			}
		}
	}

	@Test(timeout = 10000)
	public void testFirstTaskPurgeRetriesBelowPressure() throws Exception {
		long mb = 1L << 20;
		DMLConfig config = ConfigurationManager.getDMLConfig();
		config.setTextValue(DMLConfig.OOC_MEM_BROKER_STRICT_FREE, String.valueOf(8 * mb));
		config.setTextValue(DMLConfig.OOC_MEM_BROKER_STRICT_FRACTION, "0.85");
		config.setTextValue(DMLConfig.OOC_MEM_BROKER_PURGE_FREE, String.valueOf(8 * mb));
		GlobalMemoryBroker broker = new GlobalMemoryBroker(128 * mb);
		Field global = GlobalMemoryBroker.class.getDeclaredField("BROKER");
		global.setAccessible(true);
		Object previous = global.get(null);
		Field revive = InMemoryQueueCallback.class.getDeclaredField("REVIVE_ALLOWANCE");
		revive.setAccessible(true);
		Object previousRevive = revive.get(null);
		revive.set(null, null);
		global.set(null, broker);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance consumer = new SyncMemoryAllowance(broker);
		CountDownLatch firstPurge = new CountDownLatch(1);
		SubscribableTaskQueue<IndexedMatrixValue> queue = new SubscribableTaskQueue<>();
		InMemoryQueueCallback<IndexedMatrixValue> callback = null;
		try {
			IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(1, 1),
				new MatrixBlock(2048, 2048, 1.0));
			long bytes = value.size();
			producer.setTargetMemory(bytes);
			producer.reserveBlocking(bytes);
			callback = new InMemoryQueueCallback<>(value, null, producer, bytes) {
				@Override
				public long tryPark(OOCCache cache) {
					long freed = super.tryPark(cache);
					firstPurge.countDown();
					return freed;
				}
			};
			queue.enqueue(callback);
			callback = callback.keepOpen();
			consumer.setTargetMemory(112 * mb);
			Assert.assertTrue(broker.getAllowedMemory() - broker.getUsedMemory() > 8 * mb);
			Assert.assertFalse(broker.isStrictMode());
			OOCFuture<Void> reservation = consumer.reserveTaskAsync(112 * mb);
			Assert.assertTrue(firstPurge.await(5, TimeUnit.SECONDS));
			Assert.assertFalse(reservation.isDone());
			Assert.assertFalse(callback.isParked());
			callback.close();
			callback = null;
			reservation.get(5, TimeUnit.SECONDS);
			consumer.release(112 * mb);
			try(OOCStream.QueueCallback<IndexedMatrixValue> parked = queue.dequeueCB()) {
				Assert.assertTrue(((InMemoryQueueCallback<?>) parked).isParked());
				Assert.assertEquals(1.0, parked.get().getValue().get(0, 0), 0);
			}
			Assert.assertEquals(0, producer.getUsedMemory());
			Assert.assertEquals(0, producer.getPassiveMemory());
		}
		finally {
			if(callback != null)
				callback.close();
			queue.closeInput();
			OOCStream.QueueCallback<IndexedMatrixValue> queued;
			while((queued = queue.pollCB()) != null)
				queued.close();
			consumer.destroy();
			producer.destroy();
			MemoryAllowance revived = (MemoryAllowance) revive.get(null);
			if(revived != null)
				revived.destroy();
			OOCCacheTestUtils.await(() -> broker.describeAllowances().contains("reclaimerArmed=false"), 5);
			revive.set(null, previousRevive);
			global.set(null, previous);
		}
	}

	@Test(timeout = 10000)
	public void largeTaskReclaimsIdleGrantsEvenBelowStrictPressure() throws Exception {
		long mb = 1L << 20;
		GlobalMemoryBroker broker = new GlobalMemoryBroker(128 * mb);
		SyncMemoryAllowance first = new SyncMemoryAllowance(broker, 30 * mb);
		SyncMemoryAllowance second = new SyncMemoryAllowance(broker, 30 * mb);
		SyncMemoryAllowance busy = new SyncMemoryAllowance(broker, 30 * mb);
		SyncMemoryAllowance consumer = new SyncMemoryAllowance(broker, 70 * mb);
		try {
			first.reserveBlocking(30 * mb);
			second.reserveBlocking(30 * mb);
			busy.reserveBlocking(30 * mb);
			OOCFuture<Void> task = consumer.reserveTaskAsync(70 * mb);
			Assert.assertFalse(task.isDone());
			Assert.assertEquals(0, consumer.getGrantedMemory());
			first.release(30 * mb);
			second.release(30 * mb);
			task.get(5, TimeUnit.SECONDS);
			Assert.assertEquals(30 * mb, busy.getUsedMemory());
			consumer.release(70 * mb);
			busy.release(30 * mb);
		}
		finally {
			first.destroy();
			second.destroy();
			busy.destroy();
			consumer.destroy();
		}
	}

	private static final int TILES = 20000;

	@Test
	public void testAllocatedStreamSizesResidentAndParkedCallbacks() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		AtomicInteger reads = new AtomicInteger();
		AtomicInteger delivered = new AtomicInteger();
		SubscribableTaskQueue<Integer> source = new SubscribableTaskQueue<>();
		source.enqueue(new OOCStream.SimpleQueueCallback<Integer>(2, null) {
			@Override
			public long getBytes() {
				return 2;
			}
		});
		source.enqueue(new OOCStream.QueueCallback<Integer>() {
			@Override
			public long getBytes() {
				return 6;
			}

			@Override
			public Integer get() {
				reads.incrementAndGet();
				return 1;
			}

			@Override
			public OOCStream.QueueCallback<Integer> keepOpen() {
				return this;
			}

			@Override
			public void close() {}

			@Override
			public void fail(DMLRuntimeException failure) {}

			@Override
			public boolean isEos() {
				return false;
			}

			@Override
			public boolean isFailure() {
				return false;
			}
		});
		source.enqueue(new OwnedGroupQueueCallback<>(List.of(
			new OOCStream.SimpleQueueCallback<Integer>(1, null) {
				@Override
				public long getBytes() {
					return 1;
				}
			},
			new OOCStream.SimpleQueueCallback<Integer>(3, null) {
				@Override
				public long getBytes() {
					return 3;
				}
			})));
		source.closeInput();
		try {
			AllocatedOOCStream<Integer> allocated = new AllocatedOOCStream<>(source, allowance,
				bytes -> bytes * 10L, true);
			allocated.setSubscriber(callback -> {
				try(callback) {
					if(!callback.isEos()) {
						int index = delivered.getAndIncrement();
						Assert.assertEquals(index == 0 ? 20 : index == 1 ? 60 : 30, allowance.getUsedMemory());
						Assert.assertEquals(0, reads.get());
						if(index < 2)
							AllocatedOOCStream.detachBudget(callback).close();
					}
				}
			});
			Assert.assertEquals(3, delivered.get());
			Assert.assertEquals(0, reads.get());
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			allowance.destroy();
		}
	}

	@Test
	public void testAllocatedStreamInstallsConsumerBeforeStartingInput() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		AtomicInteger delivered = new AtomicInteger();
		SubscribableTaskQueue<Integer> source = new SubscribableTaskQueue<>() {
			@Override
			public void setSubscriber(Consumer<OOCStream.QueueCallback<Integer>> subscriber) {
				super.setSubscriber(subscriber);
				enqueue(1);
				Assert.assertEquals("Input started before the admitted stream could drain", 1, delivered.get());
				enqueue(2);
				closeInput();
			}
		};
		try {
			AllocatedOOCStream<Integer> allocated = new AllocatedOOCStream<>(source, allowance, 60, false);
			Assert.assertEquals(0, delivered.get());
			allocated.setSubscriber(callback -> {
				try(callback) {
					if(!callback.isEos()) {
						delivered.incrementAndGet();
						AllocatedOOCStream.detachBudget(callback).close();
					}
				}
			});
			Assert.assertEquals(2, delivered.get());
			Assert.assertEquals(0, allowance.getUsedMemory());
			Field waiting = AllocatedOOCStream.class.getDeclaredField("_waiting");
			waiting.setAccessible(true);
			Assert.assertNull(waiting.get(allocated));
		}
		finally {
			allowance.destroy();
		}
	}

	@Test(timeout = 10000)
	public void testAllocatedStreamQueuesOnlyOneReservation() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		AtomicInteger requests = new AtomicInteger();
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker) {
			@Override
			public OOCFuture<Void> reserveAsync(long bytes) {
				requests.incrementAndGet();
				return super.reserveAsync(bytes);
			}
		};
		AtomicInteger delivered = new AtomicInteger();
		CountDownLatch complete = new CountDownLatch(1);
		SubscribableTaskQueue<Integer> source = new SubscribableTaskQueue<>();
		AllocatedOOCStream<Integer> allocated = new AllocatedOOCStream<>(source, allowance, 1, false);
		try {
			allowance.reserveBlocking(100);
			requests.set(0);
			allocated.setSubscriber(callback -> {
				try(callback) {
					if(callback.isEos())
						complete.countDown();
					else {
						Assert.assertEquals(delivered.getAndIncrement(), callback.get().intValue());
						AllocatedOOCStream.detachBudget(callback).close();
					}
				}
			});
			for(int i = 0; i < 10000; i++)
				source.enqueue(i);
			source.closeInput();
			Assert.assertEquals(0, delivered.get());
			Assert.assertEquals(1, requests.get());
			allowance.release(100);
			Assert.assertTrue(complete.await(5, TimeUnit.SECONDS));
			Assert.assertEquals(10000, delivered.get());
			Assert.assertEquals(10000, requests.get());
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			allowance.destroy();
		}
	}

	@Test(timeout = 10000)
	public void testAllocatedStreamClosesWaitingPayloadsOnFailure() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(new GlobalMemoryBroker(100));
		SubscribableTaskQueue<Integer> source = new SubscribableTaskQueue<>();
		AllocatedOOCStream<Integer> allocated = new AllocatedOOCStream<>(source, allowance, 60, false);
		allocated.setSubscriber(callback -> {
			try(callback) {
				ReservationBudget budget = AllocatedOOCStream.detachBudget(callback);
				if(budget != null)
					budget.close();
			}
		});
		try {
			allowance.reserveBlocking(100);
			for(int i = 0; i < 2; i++) {
				producer.reserveBlocking(10);
				source.enqueue(new InMemoryQueueCallback<>(i, null, producer, 10));
			}
			Assert.assertEquals(20, producer.getUsedMemory());
			allocated.propagateFailure(new DMLRuntimeException("injected failure"));
			Assert.assertEquals(0, producer.getUsedMemory());
			allowance.release(100);
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			producer.destroy();
			allowance.destroy();
		}
	}

	@Test(timeout = 10000)
	public void testAllocatedStreamConcurrentWaiters() throws Exception {
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(new GlobalMemoryBroker(100));
		SubscribableTaskQueue<Integer> source = new SubscribableTaskQueue<>();
		AllocatedOOCStream<Integer> allocated = new AllocatedOOCStream<>(source, allowance, 60, false);
		AtomicIntegerArray seen = new AtomicIntegerArray(4000);
		CountDownLatch complete = new CountDownLatch(1);
		CountDownLatch queued = new CountDownLatch(4);
		CountDownLatch resume = new CountDownLatch(1);
		ExecutorService pool = Executors.newFixedThreadPool(4);
		try {
			allowance.reserveBlocking(100);
			allocated.setSubscriber(callback -> {
				try(callback) {
					if(callback.isEos())
						complete.countDown();
					else {
						Assert.assertEquals(0, seen.getAndIncrement(callback.get()));
						AllocatedOOCStream.detachBudget(callback).close();
					}
				}
			});
			List<Future<?>> producers = new ArrayList<>();
			for(int p = 0; p < 4; p++) {
				int start = p * 1000;
				producers.add(pool.submit(() -> {
					source.enqueue(start);
					queued.countDown();
					Assert.assertTrue(resume.await(5, TimeUnit.SECONDS));
					for(int i = start + 1; i < start + 1000; i++)
						source.enqueue(i);
					return null;
				}));
			}
			Assert.assertTrue(queued.await(5, TimeUnit.SECONDS));
			CompletableFuture<Void> release = CompletableFuture.runAsync(() -> allowance.release(100));
			resume.countDown();
			for(Future<?> producer : producers)
				producer.get(5, TimeUnit.SECONDS);
			release.get(5, TimeUnit.SECONDS);
			source.closeInput();
			Assert.assertTrue(complete.await(5, TimeUnit.SECONDS));
			for(int i = 0; i < seen.length(); i++)
				Assert.assertEquals(1, seen.get(i));
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			pool.shutdownNow();
			allowance.destroy();
		}
	}

	@Test
	public void testAllocatedStreamAdmitsWhilePreviousTasksAreRunning() {
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(new GlobalMemoryBroker(100));
		SubscribableTaskQueue<Integer> source = new SubscribableTaskQueue<>();
		AllocatedOOCStream<Integer> allocated = new AllocatedOOCStream<>(source, allowance, 40, false);
		List<ReservationBudget> tasks = new ArrayList<>();
		allocated.setSubscriber(callback -> {
			try(callback) {
				if(!callback.isEos())
					tasks.add(AllocatedOOCStream.detachBudget(callback));
			}
		});
		try {
			for(int i = 0; i < 3; i++)
				source.enqueue(i);
			source.closeInput();
			Assert.assertEquals(2, tasks.size());
			Assert.assertEquals(80, allowance.getUsedMemory());
			tasks.get(0).close();
			Assert.assertEquals(3, tasks.size());
			Assert.assertEquals(80, allowance.getUsedMemory());
		}
		finally {
			for(ReservationBudget task : tasks)
				task.close();
			allowance.destroy();
		}
	}

	@Test
	public void testReclaimRetriesAcrossStrictModeThreshold() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		CountDownLatch firstPass = new CountDownLatch(1);
		SyncMemoryAllowance holder = new SyncMemoryAllowance(broker) {
			@Override
			public long reclaimUnused() {
				long reclaimed = super.reclaimUnused();
				firstPass.countDown();
				return reclaimed;
			}
		};
		SyncMemoryAllowance waiter = new SyncMemoryAllowance(broker);
		try {
			holder.setTargetMemory(21);
			waiter.setTargetMemory(61);
			holder.reserveBlocking(21);
			waiter.reserveBlocking(61);
			Assert.assertEquals(82, broker.getUsedMemory());
			Assert.assertTrue(broker.isStrictMode());
			waiter.setTargetMemory(62);
			OOCFuture<Void> reservation = waiter.reserveAsync(1);
			Assert.assertTrue(firstPass.await(5, TimeUnit.SECONDS));
			holder.release(12);
			reservation.get(5, TimeUnit.SECONDS);
			Assert.assertEquals(62, waiter.getUsedMemory());
		}
		finally {
			holder.release(holder.getUsedMemory());
			waiter.release(waiter.getUsedMemory());
			holder.destroy();
			waiter.destroy();
		}
	}

	@Test
	public void testOptimal() {
		test(true, 0, 1);
	}

	@Test
	public void testWorstCase() {
		test(false, 0, 1);
	}

	@Test
	public void testBlockedReservationReclaims() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance holder = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance waiter = new SyncMemoryAllowance(broker);
		try {
			holder.reserveBlocking(100);
			OOCFuture<Void> reservation = waiter.reserveAsync(50);
			Assert.assertFalse(reservation.isDone());

			holder.release(50);
			reservation.get(10, TimeUnit.SECONDS);
			Assert.assertEquals(50, holder.getGrantedMemory());
			Assert.assertEquals(50, waiter.getUsedMemory());
		}
		finally {
			holder.release(holder.getUsedMemory());
			waiter.release(waiter.getUsedMemory());
			holder.destroy();
			waiter.destroy();
		}
	}

	@Test
	public void testStrictFairShareWakesBlockedReservation() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance holder = new SyncMemoryAllowance(broker, 85);
		SyncMemoryAllowance waiter = new SyncMemoryAllowance(broker);
		try {
			waiter.setTargetMemory(0);
			OOCFuture<Void> reservation = waiter.reserveAsync(15);
			Assert.assertFalse(reservation.isDone());

			holder.reserveBlocking(85);
			reservation.get(10, TimeUnit.SECONDS);
			Assert.assertTrue(broker.isStrictMode());
			Assert.assertEquals(15, waiter.getUsedMemory());
		}
		finally {
			holder.release(holder.getUsedMemory());
			waiter.release(waiter.getUsedMemory());
			holder.destroy();
			waiter.destroy();
		}
	}

	@Test
	public void testStrictFairShareAllowsOneCrossingRequest() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		SyncMemoryAllowance peer = new SyncMemoryAllowance(broker);
		try {
			allowance.reserveBlocking(40);
			Assert.assertTrue(broker.isStrictMode());
			Assert.assertTrue(allowance.tryReserve(20));
			Assert.assertFalse(allowance.tryReserve(1));
		}
		finally {
			allowance.release(allowance.getUsedMemory());
			peer.release(peer.getUsedMemory());
			allowance.destroy();
			peer.destroy();
		}
	}

	@Test
	public void testReservationWaiters() throws Exception {
		CoordinatedBroker broker = new CoordinatedBroker(new GlobalMemoryBroker(100));
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		try {
			allowance.reserveBlocking(100);
			OOCFuture<Void> first = allowance.reserveAsync(60);
			Assert.assertFalse(first.isDone());

			allowance.release(20);
			OOCFuture<Void> second = allowance.reserveAsync(20);
			Assert.assertFalse(second.isDone());

			allowance.release(60);
			first.get(10, TimeUnit.SECONDS);
			second.get(10, TimeUnit.SECONDS);
			Assert.assertEquals(100, allowance.getUsedMemory());
		}
		finally {
			allowance.release(allowance.getUsedMemory());
			allowance.destroy();
			broker.destroy();
		}
	}

	@Test
	public void testPassiveBudgetAccounting() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		try {
			allowance.reserveBlocking(100);
			ReservationBudget budget = new ReservationBudget(allowance, 100);
			budget.reserveBlocking(60);
			Assert.assertEquals(100, allowance.getActiveMemory());
			Assert.assertEquals(0, allowance.getPassiveMemory());

			budget.close();
			Assert.assertEquals(0, allowance.getActiveMemory());
			Assert.assertEquals(60, allowance.getPassiveMemory());
			budget.release(20);
			Assert.assertEquals(40, allowance.getPassiveMemory());
			budget.release(40);
			Assert.assertEquals(0, allowance.getPassiveMemory());
			Assert.assertEquals(0, allowance.getUsedMemory());

			allowance.reserveBlocking(100);
			ReservationBudget root = new ReservationBudget(allowance, 100);
			root.reserveBlocking(60);
			ReservationBudget child = new ReservationBudget(root, 60);
			child.reserveBlocking(40);
			child.close();
			Assert.assertEquals(0, allowance.getPassiveMemory());
			root.close();
			Assert.assertEquals(40, allowance.getPassiveMemory());
			child.release(40);
			Assert.assertEquals(0, allowance.getPassiveMemory());
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			if(allowance.getUsedMemory() > 0)
				allowance.release(allowance.getUsedMemory());
			allowance.destroy();
		}
	}

	@Test
	public void testPassiveTaskAdmission() {
		int parallelism = InfrastructureAnalyzer.getLocalParallelism();
		GlobalMemoryBroker broker = new GlobalMemoryBroker(Math.max(1000, 4L * parallelism));
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		List<ReservationBudget> budgets = new ArrayList<>();
		try {
			while(allowance.tryReserveTask(1)) {
				ReservationBudget budget = new ReservationBudget(allowance, 1);
				budget.reserveBlocking(1);
				budgets.add(budget);
			}
			Assert.assertEquals(2 * parallelism, budgets.size());
			for(ReservationBudget budget : budgets)
				budget.close();
			Assert.assertEquals(0, allowance.getActiveMemory());
			Assert.assertEquals(2L * parallelism, allowance.getPassiveMemory());
			Assert.assertFalse(allowance.tryReserveTask(1));
			for(ReservationBudget budget : budgets)
				budget.release(1);
			Assert.assertEquals(0, allowance.getPassiveMemory());
			Assert.assertTrue(allowance.tryReserveTask(1));
			allowance.release(1);
		}
		finally {
			if(allowance.getUsedMemory() > 0)
				allowance.release(allowance.getUsedMemory());
			allowance.destroy();
		}
	}

	@Test
	public void testAllocatedStreamReservations() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		SubscribableTaskQueue<Integer> source = new SubscribableTaskQueue<>();
		AllocatedOOCStream<Integer> allocated = new AllocatedOOCStream<>(source, allowance, 60, false);
		try {
			allowance.reserveBlocking(100);
			source.enqueue(1);
			Assert.assertEquals(100, allowance.getUsedMemory());

			allowance.release(100);
			OOCStream.QueueCallback<Integer> first = allocated.dequeueCB();
			ReservationBudget budget = AllocatedOOCStream.detachBudget(first);
			Assert.assertNotNull(budget);
			first.close();
			Assert.assertEquals(60, allowance.getUsedMemory());
			budget.reserveBlocking(20);
			budget.release(20);
			Assert.assertEquals(40, allowance.getUsedMemory());
			budget.close();
			Assert.assertEquals(0, allowance.getUsedMemory());

			allowance.reserveBlocking(60);
			ReservationBudget reusable = new ReservationBudget(allowance, 60).enableReuse();
			reusable.reserveBlocking(40);
			reusable.release(40);
			reusable.reserveBlocking(40);
			reusable.release(40);
			reusable.close();
			Assert.assertEquals(0, allowance.getUsedMemory());

			source.enqueue(2);
			OOCStream.QueueCallback<Integer> second = allocated.dequeueCB();
			OOCStream.QueueCallback<Integer> retained = second.keepOpen();
			second.close();
			Assert.assertEquals(60, allowance.getUsedMemory());
			retained.close();
			Assert.assertEquals(0, allowance.getUsedMemory());
			source.closeInput();
			Assert.assertNull(allocated.dequeueCB());
		}
		finally {
			if(allowance.getUsedMemory() > 0)
				allowance.release(allowance.getUsedMemory());
			allowance.destroy();
		}
	}

	@Test
	public void testGrowingReusableBudget() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		try {
			allowance.reserveBlocking(20);
			ReservationBudget budget = new ReservationBudget(allowance, 20).enableReuse().enableGrowth();
			Assert.assertTrue(budget.tryReserve(60));
			Assert.assertEquals(60, budget.getGrantedMemory());
			Assert.assertEquals(60, allowance.getUsedMemory());
			Assert.assertFalse(budget.tryReserve(50));
			budget.release(60);
			budget.close();
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			if(allowance.getUsedMemory() > 0)
				allowance.release(allowance.getUsedMemory());
			allowance.destroy();
		}
	}

	@Test
	public void testInsufficientBudgetFailsWithoutParentReservation() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		try {
			allowance.reserveBlocking(20);
			ReservationBudget budget = new ReservationBudget(allowance, 20);
			try {
				budget.reserveBlocking(21);
				Assert.fail("An admitted task must not grow its budget implicitly");
			}
			catch(IllegalStateException expected) {
				Assert.assertTrue(expected.getMessage().contains("20 bytes available"));
			}
			OOCFuture<Void> rejected = budget.reserveAsync(21);
			Assert.assertTrue(rejected.isDone());
			rejected.whenComplete((ignored, error) ->
				Assert.assertTrue(error instanceof IllegalStateException));
			Assert.assertEquals(20, budget.getGrantedMemory());
			Assert.assertEquals(20, allowance.getUsedMemory());
			budget.close();
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			if(allowance.getUsedMemory() > 0)
				allowance.release(allowance.getUsedMemory());
			allowance.destroy();
		}
	}

	@Test
	public void testAllocatedStreamFailure() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(100);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		SubscribableTaskQueue<Integer> source = new SubscribableTaskQueue<>();
		AllocatedOOCStream<Integer> allocated = new AllocatedOOCStream<>(source, allowance, 60, false);
		allocated.setSubscriber(callback -> {
			try(callback) {
				ReservationBudget budget = AllocatedOOCStream.detachBudget(callback);
				if(budget != null)
					budget.close();
			}
		});
		try {
			allowance.reserveBlocking(100);
			source.enqueue(1);
			source.propagateFailure(new DMLRuntimeException("injected failure"));
			allowance.release(100);
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			if(allowance.getUsedMemory() > 0)
				allowance.release(allowance.getUsedMemory());
			allowance.destroy();
		}
	}

	public void test(boolean optimal, int nWarmup, int nMeasure) {
		//DMLScript.OOC_STATISTICS = true;
		long millis;
		for(int i = 0; i < nWarmup; i++) {
			testNew(optimal);
		}
		millis = 0;
		for(int i = 0; i < nMeasure; i++) {
			millis += testNew(optimal);
		}
		//System.out.println("New: " + millis + "ms");
		//System.out.println(Statistics.displayOOCEvictionStats());
		OOCCacheManager.reset();

		/*for(int i = 0; i < 10; i++) {
			testOld(optimal);
		}
		millis = 0;
		for(int i = 0; i < 10; i++) {
			millis += testOld(optimal);
		}
		//System.out.println("Old: " + millis + "ms");
		//System.out.println(Statistics.displayOOCEvictionStats());
		OOCCacheManager.reset();*/
	}

	public long testNew(boolean optimal) {
		// We emulate the expression (A + 2) + B with limited memory
		MemoryBroker parentBroker = new GlobalMemoryBroker(500000000L);
		CoordinatedBroker broker = new CoordinatedBroker(parentBroker);
		TestInstruction test = new TestInstruction();

		MemoryAllowance leftAllowance = new SyncMemoryAllowance(broker);
		MemoryAllowance rightAllowance = new SyncMemoryAllowance(broker);
		MemoryAllowance joinAllowance = new SyncMemoryAllowance(broker);
		CachedAllowance cache = new CachedAllowance(broker);

		OOCStream<Integer> leftStream = new SubscribableTaskQueue<>();
		OOCStream<Integer> rightStream = new SubscribableTaskQueue<>();
		OOCStream<InMemoryQueueCallback<IndexedMatrixValue>> outStream = new SubscribableTaskQueue<>();

		long startMillis = System.currentTimeMillis();

		// Left producer reservation thread
		new Thread(() -> {
			for(int i = 0; i < TILES; i++) {
				leftAllowance.reserveBlocking(8 * 1000 + /* Working memory */ 8 * 1000);
				leftStream.enqueue(i);
			}
			leftStream.closeInput();
		}).start();

		// Right producer reservation thread
		new Thread(() -> {
			for(int i = 0; i < TILES; i++) {
				rightAllowance.reserveBlocking(8 * 1000); // Needs no working memory
				if(optimal)
					rightStream.enqueue(i);
				else
					rightStream.enqueue(TILES-i-1);
			}
			rightStream.closeInput();
		}).start();

		OOCStream<InMemoryQueueCallback<IndexedMatrixValue>> leftStreamOut = new SubscribableTaskQueue<>();
		OOCStream<InMemoryQueueCallback<IndexedMatrixValue>> leftStreamOutOut = new SubscribableTaskQueue<>();
		OOCStream<InMemoryQueueCallback<IndexedMatrixValue>> rightStreamOut = new SubscribableTaskQueue<>();

		test.map(leftStream, leftStreamOut, i -> {
			var imv = new IndexedMatrixValue(new MatrixIndexes(i.longValue(), 1L), new MatrixBlock(1000, 1, 5.0));
			return new InMemoryQueueCallback<>(imv, null, leftAllowance, 8 * 1000);
		});
		test.map(leftStreamOut, leftStreamOutOut, cb -> {
			try(cb) {
				var imv = new IndexedMatrixValue(cb.get().getIndexes(), cb.get().getValue()
					.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 2.0), new MatrixBlock()));
				return new InMemoryQueueCallback<>(imv, null, leftAllowance, 8 * 1000);
			}
		});
		test.map(rightStream, rightStreamOut, i -> {
			var imv = new IndexedMatrixValue(new MatrixIndexes(i.longValue(), 1L), new MatrixBlock(1000, 1, 3.0));
			return new InMemoryQueueCallback<>(imv, null, rightAllowance, 8 * 1000);
		});

		test.join(leftStreamOutOut, rightStreamOut, outStream, () -> joinAllowance.reserveBlocking(8 * 1000), cache,
			(l, r) -> {
				var imv = new IndexedMatrixValue(l.getIndexes(), ((MatrixBlock)l.getValue()).binaryOperations(new BinaryOperator(
					Plus.getPlusFnObject()), r.getValue()));
				return new InMemoryQueueCallback<>(imv, null, joinAllowance, 8 * 1000);
		});

		CompletableFuture<Void> future = new CompletableFuture<>();
		AtomicInteger ctr = new AtomicInteger();
		outStream.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					future.complete(null);
					return;
				}
				InMemoryQueueCallback<IndexedMatrixValue> inner = cb.get();
				try(cb; inner) {
					ctr.incrementAndGet();
					double checksum =((MatrixBlock)inner.get().getValue()).sum();
					if(checksum < 10000.0 - 1e-9 || checksum > 10000.0 + 1e-9)
						future.completeExceptionally(new AssertionError("Wrong checksum: " + checksum));
					//System.out.println(cb.get().get().getIndexes());
				}
			}
			catch(Exception e) {
				future.completeExceptionally(e);
			}
		});
		future.join();

		Assert.assertEquals(TILES, ctr.get());
		return System.currentTimeMillis() - startMillis;
	}

	public long testOld(boolean optimal) {
		// We emulate the expression (A + 2) + B with limited memory
		TestInstruction test = new TestInstruction();

		OOCStream<Integer> leftStream = new SubscribableTaskQueue<>();
		OOCStream<Integer> rightStream = new SubscribableTaskQueue<>();
		OOCStream<IndexedMatrixValue> outStream = new SubscribableTaskQueue<>();

		long startMillis = System.currentTimeMillis();

		// Left producer reservation thread
		new Thread(() -> {
			for(int i = 0; i < TILES; i++) {
				leftStream.enqueue(i);
			}
			leftStream.closeInput();
		}).start();

		// Right producer reservation thread
		new Thread(() -> {
			for(int i = 0; i < TILES; i++) {
				if(optimal)
					rightStream.enqueue(i);
				else
					rightStream.enqueue(TILES-i-1);
			}
			rightStream.closeInput();
		}).start();

		OOCStream<IndexedMatrixValue> leftStreamOut = new SubscribableTaskQueue<>();
		OOCStream<IndexedMatrixValue> leftStreamOutOut = new SubscribableTaskQueue<>();
		OOCStream<IndexedMatrixValue> rightStreamOut = new SubscribableTaskQueue<>();

		test.map(leftStream, leftStreamOut, i -> {
			var imv = new IndexedMatrixValue(new MatrixIndexes(i.longValue(), 1L), new MatrixBlock(1000, 1, 5.0));
			return imv;
		});
		test.map(leftStreamOut, leftStreamOutOut, v -> {
			var imv = new IndexedMatrixValue(v.getIndexes(), v.getValue()
				.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 2.0), new MatrixBlock()));
			return imv;
		});
		test.map(rightStream, rightStreamOut, i -> {
			var imv = new IndexedMatrixValue(new MatrixIndexes(i.longValue(), 1L), new MatrixBlock(1000, 1, 3.0));
			return imv;
		});

		test.joinOOC(leftStreamOutOut, rightStreamOut, outStream,
			(l, r) -> {
				var imv = new IndexedMatrixValue(l.getIndexes(), ((MatrixBlock)l.getValue()).binaryOperations(new BinaryOperator(
					Plus.getPlusFnObject()), r.getValue()));
				return imv;
			});

		CompletableFuture<Void> future = new CompletableFuture<>();
		outStream.setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					future.complete(null);
					return;
				}
				try(cb) {
					//System.out.println(cb.get().getIndexes());
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		});
		future.join();
		return System.currentTimeMillis() - startMillis;
	}

	static class TestInstruction extends OOCInstruction {
		protected TestInstruction() {
			super(null, "test", "test");
		}

		@Override
		public void processInstruction(ExecutionContext ec) {
		}

		public <T, R> CompletableFuture<Void> map(OOCStream<T> qIn, OOCStream<R> qOut, Function<T, R> mapper) {
			return mapOOC(qIn, qOut, mapper);
		}

		public CompletableFuture<Void> joinOOC(OOCStream<IndexedMatrixValue> l, OOCStream<IndexedMatrixValue> r,
			OOCStream<IndexedMatrixValue> out, BiFunction<IndexedMatrixValue, IndexedMatrixValue, IndexedMatrixValue> joinFn) {

			return super.joinOOC(l, r, out, joinFn, IndexedMatrixValue::getIndexes);
		}

		public CompletableFuture<Void> join(OOCStream<InMemoryQueueCallback<IndexedMatrixValue>> l,
			OOCStream<InMemoryQueueCallback<IndexedMatrixValue>> r,
			OOCStream<InMemoryQueueCallback<IndexedMatrixValue>> out, Runnable memoryReserver, CachedAllowance cache,
			BiFunction<IndexedMatrixValue, IndexedMatrixValue, InMemoryQueueCallback<IndexedMatrixValue>> joinFn) {

			OOCStream<Tuple3<OOCStream.QueueCallback<IndexedMatrixValue>, OOCStream.QueueCallback<IndexedMatrixValue>, Integer>> intermediate = createWritableStream();

			new Thread(() -> {
				InMemoryQueueCallback<IndexedMatrixValue> next;
				IndexedMatrixValue nextValue;
				boolean nextLeft = true;
				AtomicInteger pendingRequests = new AtomicInteger(1);

				while((next = (nextLeft ? l : r).dequeue()) != null) {
					try {
						nextValue = next.get();
						int idx = (int) nextValue.getIndexes().getRowIndex();
						var future = cache.get(idx);
						if(future.isDone()) {
							var cb = future.getNow(null);
							if(cb == null) {
								cache.handover(next, idx);
							}
							else {
								try(cb) {
									memoryReserver.run(); // reserve memory for future pipeline
									intermediate.enqueue(nextLeft ? new Tuple3<>(next.keepOpen(), cb.keepOpen(), idx) :
										new Tuple3<>(cb.keepOpen(), next.keepOpen(), idx));
								}
							}
						}
						else {
							pendingRequests.incrementAndGet();
							final var pinned = next.keepOpen();
							final boolean isLeft = nextLeft;
							future.thenAccept(cb -> {
								try(cb; pinned) {
									intermediate.enqueue(
										isLeft ? new Tuple3<>(pinned.keepOpen(), cb.keepOpen(), idx) :
											new Tuple3<>(cb.keepOpen(), pinned.keepOpen(), idx));
								}
								if(pendingRequests.decrementAndGet() == 0)
									intermediate.closeInput();
							});
						}

						nextLeft = !nextLeft;
					}
					finally {
						next.close();
					}
				}

				if(pendingRequests.decrementAndGet() == 0)
					intermediate.closeInput();
			}).start();

			return mapOOC(intermediate, out, t -> {
				var qL = t._1();
				var qR = t._2();
				try(qL; qR) {
					return joinFn.apply(qL.get(), qR.get());
				}
				finally {
					cache.clear(t._3());
				}
			});
		}
	}

	static class CoordinatedBroker extends SyncMemoryAllowance implements MemoryBroker {
		private final List<MemoryAllowance> _children;
		private final Map<MemoryAllowance, Long> _credits;
		private record TargetUpdate(MemoryAllowance allowance, long target) {}

		CoordinatedBroker(MemoryBroker parentBroker) {
			super(parentBroker);
			_children = new ArrayList<>();
			_credits = new IdentityHashMap<>();
		}

		@Override
		public void attachAllowance(MemoryAllowance allowance) {
			List<TargetUpdate> updates;
			synchronized(this) {
				_children.add(allowance);
				_credits.put(allowance, 0L);
				updates = rebalanceTargetsLocked();
			}
			applyTargetUpdates(updates);
		}

		@Override
		public void reservationBlocked(MemoryAllowance allowance, long bytes) {
		}

		@Override
		public long requestMemory(MemoryAllowance allowance, long minSize, long maxSize) {
			if(!_credits.containsKey(allowance))
				throw new UnsupportedOperationException("Allowance is not attached to CoordinatedBroker.");
			List<TargetUpdate> updates;
			long granted;
			synchronized(this) {
				granted = requestGrantLocked(allowance, minSize);
				updates = rebalanceTargetsLocked();
			}
			applyTargetUpdates(updates);
			return granted;
		}

		@Override
		public void freeMemory(MemoryAllowance allowance, long freedMemory) {
			if(!_credits.containsKey(allowance))
				throw new UnsupportedOperationException("Allowance is not attached to CoordinatedBroker.");
			if(freedMemory <= 0)
				return;
			List<TargetUpdate> updates;
			synchronized(this) {
				release(freedMemory);
				updates = rebalanceTargetsLocked();
			}
			applyTargetUpdates(updates);
		}

		@Override
		public void shutdownAllowance(MemoryAllowance allowance) {
			if(!_credits.containsKey(allowance))
				throw new UnsupportedOperationException("Allowance is not attached to CoordinatedBroker.");
			List<TargetUpdate> updates;
			synchronized(this) {
				updates = rebalanceTargetsLocked();
			}
			applyTargetUpdates(updates);
		}

		@Override
		public void destroyAllowance(MemoryAllowance allowance, long freedMemory) {
			if(!_credits.containsKey(allowance))
				throw new UnsupportedOperationException("Allowance is not attached to CoordinatedBroker.");
			List<TargetUpdate> updates;
			synchronized(this) {
				_children.remove(allowance);
				_credits.remove(allowance);
				if(freedMemory > 0)
					release(freedMemory);
				updates = rebalanceTargetsLocked();
			}
			applyTargetUpdates(updates);
		}

		private long requestGrantLocked(MemoryAllowance requester, long minSize) {
			int n = _children.size();
			if(n == 0)
				return 0;
			long credit = _credits.getOrDefault(requester, 0L);
			if(credit >= minSize) {
				_credits.put(requester, credit - minSize);
				return minSize;
			}

			long granted = credit;
			long missing = minSize - granted;
			long total = n * missing;
			if(!tryReserve(total))
				return 0;
			_credits.put(requester, 0L);
			for(MemoryAllowance child : _children) {
				if(child == requester)
					continue;
				_credits.put(child, _credits.getOrDefault(child, 0L) + missing);
			}
			return minSize;
		}

		private List<TargetUpdate> rebalanceTargetsLocked() {
			List<TargetUpdate> updates = new ArrayList<>(_children.size());
			long target = getTargetMemory();
			int n = _children.size();
			long share = n == 0 ? 0 : target / n;
			for(MemoryAllowance child : _children)
				updates.add(new TargetUpdate(child, share));
			return updates;
		}

		private static void applyTargetUpdates(List<TargetUpdate> updates) {
			for(TargetUpdate update : updates)
				update.allowance.setTargetMemory(update.target);
		}
	}
}
