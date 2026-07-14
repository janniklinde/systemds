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

package org.apache.sysds.test.component.ooc.store;

import java.util.concurrent.TimeUnit;

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.MultiplicityLiveness;
import org.apache.sysds.runtime.ooc.store.SequentialAccessPattern;
import org.junit.Assert;
import org.junit.Test;

public class MaterializedStoreIndexedReaderTest {
	private static final long TILE_BYTES = 1000;
	private static final long STREAM_ID = 7;
	private static final int BLOCKS = 16;

	@Test
	public void testTargetedRequestsWithMultiplicity() throws Exception {
		Fixture f = new Fixture(1L << 30);
		try {
			MaterializedStore.IndexedReader<IndexedMatrixValue> reader =
				f.store.openIndexedReader(new MultiplicityLiveness(BLOCKS, 2));
			f.store.sealReaders();

			//demand-driven access in arbitrary order, each index twice
			for(int pass = 0; pass < 2; pass++) {
				for(int i = BLOCKS - 1; i >= 0; i--) {
					try(MaterializedStore.Lease<IndexedMatrixValue> lease =
						reader.request(i, f.reader).get(10, TimeUnit.SECONDS)) {
						Assert.assertEquals(i, lease.index());
						Assert.assertEquals(i + 1L, lease.value().getIndexes().getRowIndex());
					}
				}
			}

			//multiplicity exhausted: all entries forgotten, further requests are an error
			Assert.assertEquals(0, f.reader.getUsedMemory());
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
			try {
				reader.request(0, f.reader);
				Assert.fail("Request beyond multiplicity must fail");
			}
			catch(IllegalStateException expected) {
				//expected
			}
			reader.close();
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testForgettingWaitsForAllReaders() throws Exception {
		Fixture f = new Fixture(1L << 30);
		try {
			MaterializedStore.IndexedReader<IndexedMatrixValue> indexed =
				f.store.openIndexedReader(new MultiplicityLiveness(BLOCKS, 1));
			MaterializedStore.Reader<IndexedMatrixValue> ordered =
				f.store.openReader(new SequentialAccessPattern(BLOCKS), f.reader, 4);
			f.store.sealReaders();

			//consume the ordered reader fully; indexed liveness must keep all entries alive
			int count = 0;
			while(ordered.hasNext()) {
				try(MaterializedStore.Lease<IndexedMatrixValue> lease = ordered.next()) {
					Assert.assertEquals(count++, lease.index());
				}
			}
			Assert.assertEquals(BLOCKS, count);
			ordered.close();
			Assert.assertTrue(f.cache.getOwnedCacheSize() > 0);

			//indexed consumption forgets each entry immediately afterwards
			for(int i = 0; i < BLOCKS; i++) {
				try(MaterializedStore.Lease<IndexedMatrixValue> lease =
					indexed.request(i, f.reader).get(10, TimeUnit.SECONDS)) {
					Assert.assertEquals(i + 1L, lease.value().getIndexes().getRowIndex());
				}
			}
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
			indexed.close();
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testAdmissionRetryCompletesAfterRelease() throws Exception {
		Fixture f = new Fixture(TILE_BYTES); //reader allowance admits exactly one tile
		try {
			MaterializedStore.IndexedReader<IndexedMatrixValue> reader =
				f.store.openIndexedReader(new MultiplicityLiveness(BLOCKS, 1));
			f.store.sealReaders();

			MaterializedStore.Lease<IndexedMatrixValue> first = reader.request(0, f.reader).get(10, TimeUnit.SECONDS);
			OOCFuture<MaterializedStore.Lease<IndexedMatrixValue>> second = reader.request(1, f.reader);
			Thread.sleep(20);
			Assert.assertFalse("Second request must wait for admission", second.isDone());

			first.close();
			try(MaterializedStore.Lease<IndexedMatrixValue> lease = second.get(10, TimeUnit.SECONDS)) {
				Assert.assertEquals(2L, lease.value().getIndexes().getRowIndex());
			}
			reader.close();
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testRequestIfLive() throws Exception {
		Fixture f = new Fixture(1L << 30);
		try {
			MaterializedStore.IndexedReader<IndexedMatrixValue> reader =
				f.store.openIndexedReader(new MultiplicityLiveness(BLOCKS, 1));
			f.store.sealReaders();

			try(MaterializedStore.Lease<IndexedMatrixValue> lease = reader.requestIfLive(3, f.reader)) {
				Assert.assertNotNull(lease);
				Assert.assertEquals(4L, lease.value().getIndexes().getRowIndex());
			}
			Assert.assertEquals(0, f.reader.getUsedMemory());
			reader.close();
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testConcurrentRequestsCannotExceedMultiplicity() throws Exception {
		Fixture f = new Fixture(1L << 30);
		try {
			MaterializedStore.IndexedReader<IndexedMatrixValue> reader =
				f.store.openIndexedReader(new MultiplicityLiveness(BLOCKS, 1));
			f.store.sealReaders();

			//hold the only use of index 0 in flight; a second request must be rejected up front
			MaterializedStore.Lease<IndexedMatrixValue> lease = reader.request(0, f.reader).get(10, TimeUnit.SECONDS);
			try {
				reader.request(0, f.reader);
				Assert.fail("A second request must not pass while the only use is reserved");
			}
			catch(IllegalStateException expected) {
				//expected
			}
			//the in-flight lease keeps the index alive until it actually closes
			Assert.assertEquals(1L, lease.value().getIndexes().getRowIndex());
			lease.close();
			reader.close();
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testCloseTriggersForgetting() throws Exception {
		Fixture f = new Fixture(1L << 30);
		try {
			MaterializedStore.IndexedReader<IndexedMatrixValue> indexed =
				f.store.openIndexedReader(new MultiplicityLiveness(BLOCKS, 1));
			MaterializedStore.Reader<IndexedMatrixValue> ordered =
				f.store.openReader(new SequentialAccessPattern(BLOCKS), f.reader, 4);
			f.store.sealReaders();

			while(ordered.hasNext()) {
				try(MaterializedStore.Lease<IndexedMatrixValue> lease = ordered.next()) {
					Assert.assertNotNull(lease.value());
				}
			}
			ordered.close();
			//the unconsumed indexed liveness still retains every entry
			Assert.assertTrue(f.cache.getOwnedCacheSize() > 0);

			//early close must release the retained range immediately, not at store closure
			indexed.close();
			Assert.assertEquals(0, f.cache.getOwnedCacheSize());
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testCompleteDetectsHoles() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 32);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker);
		producer.setTargetMemory(1L << 30);
		OOCCacheImpl cache = new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30);
		MaterializedStore<IndexedMatrixValue> store = new MaterializedStore<>(cache, STREAM_ID + 1);
		try {
			for(int i : new int[] {0, 2}) {
				producer.reserveBlocking(TILE_BYTES);
				store.publishPinned(i,
					new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1), new MatrixBlock(4, 4, 1.0)),
					TILE_BYTES, producer);
			}
			try {
				store.complete();
				Assert.fail("Completion must detect the hole at index 1");
			}
			catch(IllegalStateException expected) {
				//expected
			}
		}
		finally {
			store.close();
			cache.shutdown();
			producer.destroy();
		}
	}

	@Test
	public void testPublishManagedPayload() throws Exception {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 32);
		SyncMemoryAllowance producer = new SyncMemoryAllowance(broker);
		producer.setTargetMemory(1L << 30);
		OOCCacheImpl cache = new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30);
		MaterializedStore<IndexedMatrixValue> store = new MaterializedStore<>(cache, STREAM_ID + 2);
		CappedAllowance reader = new CappedAllowance(1L << 30);
		try {
			producer.reserveBlocking(TILE_BYTES);
			ManagedPayload<IndexedMatrixValue> payload = new ManagedPayload<>(
				new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(4, 4, 5.0)),
				TILE_BYTES, producer);
			store.publishPinned(0, payload);
			Assert.assertTrue(payload.isSettled());
			//release after transfer is a no-op; the bytes followed the cache pin/unpin protocol
			payload.release();
			Assert.assertEquals(0, producer.getUsedMemory());
			store.complete();

			MaterializedStore.IndexedReader<IndexedMatrixValue> indexed =
				store.openIndexedReader(new MultiplicityLiveness(1, 1));
			store.sealReaders();
			try(MaterializedStore.Lease<IndexedMatrixValue> lease = indexed.request(0, reader).get(10, TimeUnit.SECONDS)) {
				Assert.assertEquals(5.0, ((MatrixBlock)lease.value().getValue()).get(0, 0), 0.0);
			}
			indexed.close();
		}
		finally {
			store.close();
			cache.shutdown();
			producer.destroy();
		}
	}

	@Test
	public void testRequestCanProceedBeforeSeal() throws Exception {
		Fixture f = new Fixture(1L << 30);
		try {
			MaterializedStore.IndexedReader<IndexedMatrixValue> reader =
				f.store.openIndexedReader(new MultiplicityLiveness(BLOCKS, 1));
			try(MaterializedStore.Lease<IndexedMatrixValue> lease =
				reader.request(0, f.reader).get(10, TimeUnit.SECONDS)) {
				Assert.assertNotNull(lease);
				Assert.assertEquals(new MatrixIndexes(1, 1), lease.value().getIndexes());
			}
			f.store.sealReaders();
			reader.close();
		}
		finally {
			f.close();
		}
	}

	/**
	 * A deterministic allowance with a hard reservation cap. The shared GlobalMemoryBroker redistributes
	 * targets across attached allowances, so SyncMemoryAllowance cannot model a fixed small reader budget
	 * in a unit test.
	 */
	private static final class CappedAllowance implements MemoryAllowance {
		private final long _capacity;
		private long _used;

		private CappedAllowance(long capacity) {
			_capacity = capacity;
		}

		@Override
		public synchronized boolean tryReserve(long bytes) {
			if(_used + bytes > _capacity)
				return false;
			_used += bytes;
			return true;
		}

		@Override
		public synchronized void reserveBlocking(long bytes) {
			while(!tryReserve(bytes)) {
				try {
					wait();
				}
				catch(InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

		@Override
		public java.util.concurrent.CompletableFuture<Void> reserve(long bytes) {
			reserveBlocking(bytes);
			return java.util.concurrent.CompletableFuture.completedFuture(null);
		}

		@Override
		public synchronized void release(long bytes) {
			if(_used < bytes)
				throw new IllegalArgumentException("Allowance underflow");
			_used -= bytes;
			notifyAll();
		}

		@Override
		public synchronized long getUsedMemory() {
			return _used;
		}

		@Override
		public long getGrantedMemory() {
			return _capacity;
		}

		@Override
		public long getTargetMemory() {
			return _capacity;
		}

		@Override
		public void setTargetMemory(long targetMemory) {
			//fixed capacity
		}

		@Override
		public void shutdown() {
			//nothing to do
		}

		@Override
		public boolean isShutdown() {
			return false;
		}
	}

	private static final class Fixture {
		private final GlobalMemoryBroker broker;
		private final SyncMemoryAllowance producer;
		private final MemoryAllowance reader;
		private final OOCCacheImpl cache;
		private final MaterializedStore<IndexedMatrixValue> store;

		private Fixture(long readerCapacityBytes) {
			broker = new GlobalMemoryBroker(1L << 32);
			producer = new SyncMemoryAllowance(broker);
			producer.setTargetMemory(1L << 30);
			reader = new CappedAllowance(readerCapacityBytes);
			cache = new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30);
			store = new MaterializedStore<>(cache, STREAM_ID);
			for(int i = 0; i < BLOCKS; i++) {
				producer.reserveBlocking(TILE_BYTES);
				IndexedMatrixValue imv =
					new IndexedMatrixValue(new MatrixIndexes(i + 1L, 1), new MatrixBlock(4, 4, 1.0));
				store.publishPinned(i, imv, TILE_BYTES, producer);
			}
			store.complete();
			Assert.assertEquals(0, producer.getUsedMemory());
			Assert.assertEquals(BLOCKS, store.size());
		}

		private void close() {
			store.close();
			cache.shutdown();
			producer.destroy();
		}
	}
}
