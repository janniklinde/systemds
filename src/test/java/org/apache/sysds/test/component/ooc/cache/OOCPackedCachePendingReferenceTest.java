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

package org.apache.sysds.test.component.ooc.cache;

import java.util.concurrent.TimeUnit;

import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.io.OOCMatrixIOHandler;
import org.apache.sysds.runtime.ooc.cache.packed.OOCPackedCache;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.junit.Assert;
import org.junit.Test;

/**
 * Lifetime references and forgetting on tiles that still sit in open pack builders. A pipelined
 * consumer that parks references (state table reference slots, store readers) must not force
 * per-tile seals, otherwise packing degenerates into single-tile physical entries.
 */
public class OOCPackedCachePendingReferenceTest {
	private static final long BYTES = 1000;
	private static final long STREAM_ID = 31;

	@Test
	public void testPendingReferencesDoNotSealPacks() throws Exception {
		Fixture f = new Fixture();
		try {
			//producer unpins of pending tiles stay deferred until the pack seals and transfers
			OOCCache.UnpinHandle[] unpins = new OOCCache.UnpinHandle[3];
			for(int i = 0; i < 3; i++) {
				//pipelined live consumer: publish, park a reference, unpin, forget the canonical lifetime
				f.producer.reserveBlocking(BYTES);
				BlockEntry tile = f.cache.putPinned(STREAM_ID, i, value(i + 1.0), BYTES, f.producer);
				f.cache.reference(tile);
				unpins[i] = f.cache.unpin(tile, f.producer);
				f.cache.dereference(new BlockKey(STREAM_ID, i));
				Assert.assertEquals("Parking a reference on a pending tile must not seal the pack.",
					0, f.cache.getPackGroupCount());
			}

			f.cache.flushPacks();
			for(OOCCache.UnpinHandle unpin : unpins)
				await(unpin);
			Assert.assertEquals("All pipelined tiles must share one pack.", 1, f.cache.getPackGroupCount());
			for(int i = 0; i < 3; i++)
				Assert.assertEquals(3, f.cache.getPackGroup(STREAM_ID, i).size());

			for(int i = 0; i < 3; i++) {
				BlockEntry pinned = f.cache.pin(STREAM_ID, i, f.reader).get(10, TimeUnit.SECONDS);
				Assert.assertNotNull(pinned);
				Assert.assertEquals(i + 1.0, scalar((IndexedMatrixValue)pinned.getData()), 0.0);
				await(f.cache.unpin(pinned, f.reader));
				f.cache.dereference(new BlockKey(STREAM_ID, i));
			}
			awaitUsedMemory(f.reader, 0);
			awaitOwnedCache(f.cache, 0);
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testForgetWhilePendingDropsTileWithoutSeal() throws Exception {
		Fixture f = new Fixture();
		try {
			f.producer.reserveBlocking(2 * BYTES);
			BlockEntry first = f.cache.putPinned(STREAM_ID, 0, value(5.0), BYTES, f.producer);
			OOCCache.UnpinHandle firstUnpin = f.cache.unpin(first, f.producer);
			f.cache.dereference(new BlockKey(STREAM_ID, 0));
			Assert.assertEquals("Forgetting a pending tile must not seal the pack.",
				0, f.cache.getPackGroupCount());

			BlockEntry second = f.cache.putPinned(STREAM_ID, 1, value(7.0), BYTES, f.producer);
			f.cache.reference(second);
			OOCCache.UnpinHandle secondUnpin = f.cache.unpin(second, f.producer);
			f.cache.flushPacks();
			await(firstUnpin);
			await(secondUnpin);

			Assert.assertNull("The tile forgotten while pending must not be addressable.",
				f.cache.getPackGroup(STREAM_ID, 0));
			Assert.assertNull(f.cache.pin(STREAM_ID, 0, f.reader).get(10, TimeUnit.SECONDS));
			BlockEntry pinned = f.cache.pin(STREAM_ID, 1, f.reader).get(10, TimeUnit.SECONDS);
			Assert.assertNotNull(pinned);
			Assert.assertEquals(7.0, scalar((IndexedMatrixValue)pinned.getData()), 0.0);
			await(f.cache.unpin(pinned, f.reader));

			f.cache.dereference(new BlockKey(STREAM_ID, 1)); //canonical
			f.cache.dereference(new BlockKey(STREAM_ID, 1)); //parked
			awaitUsedMemory(f.reader, 0);
			awaitOwnedCache(f.cache, 0);
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testAllSlotsForgottenWhilePendingReleasesPack() throws Exception {
		Fixture f = new Fixture();
		try {
			f.producer.reserveBlocking(BYTES);
			BlockEntry entry = f.cache.putPinned(STREAM_ID, 0, value(9.0), BYTES, f.producer);
			OOCCache.UnpinHandle unpin = f.cache.unpin(entry, f.producer);
			f.cache.dereference(new BlockKey(STREAM_ID, 0));
			f.cache.flushPacks();
			await(unpin);
			Assert.assertNull(f.cache.getPackGroup(STREAM_ID, 0));
			awaitUsedMemory(f.producer, 0);
			awaitOwnedCache(f.cache, 0);
		}
		finally {
			f.close();
		}
	}

	@Test
	public void testReferenceAfterForgetWhilePendingThrows() throws Exception {
		Fixture f = new Fixture();
		try {
			f.producer.reserveBlocking(BYTES);
			BlockEntry entry = f.cache.putPinned(STREAM_ID, 0, value(2.0), BYTES, f.producer);
			f.cache.dereference(new BlockKey(STREAM_ID, 0));
			try {
				f.cache.reference(entry);
				Assert.fail("Referencing a tile forgotten while pending must fail");
			}
			catch(IllegalStateException expected) {
				//expected
			}
			OOCCache.UnpinHandle unpin = f.cache.unpin(entry, f.producer);
			f.cache.flushPacks();
			await(unpin);
			awaitUsedMemory(f.producer, 0);
			awaitOwnedCache(f.cache, 0);
		}
		finally {
			f.close();
		}
	}

	private static IndexedMatrixValue value(double scalar) {
		return new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(1, 1, scalar));
	}

	private static double scalar(IndexedMatrixValue value) {
		return ((MatrixBlock)value.getValue()).get(0, 0);
	}

	private static void await(OOCCache.UnpinHandle handle) throws Exception {
		if(!handle.isCommitted())
			handle.getCompletionFuture().get(10, TimeUnit.SECONDS);
	}

	private static void awaitUsedMemory(SyncMemoryAllowance allowance, long expected) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while(allowance.getUsedMemory() != expected && System.nanoTime() < deadline)
			Thread.sleep(1);
		Assert.assertEquals(expected, allowance.getUsedMemory());
	}

	private static void awaitOwnedCache(OOCCache cache, long expected) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while(cache.getOwnedCacheSize() != expected && System.nanoTime() < deadline)
			Thread.sleep(1);
		Assert.assertEquals(expected, cache.getOwnedCacheSize());
	}

	private static final class Fixture {
		private final GlobalMemoryBroker broker;
		private final SyncMemoryAllowance producer;
		private final SyncMemoryAllowance reader;
		private final OOCPackedCache cache;

		private Fixture() {
			broker = new GlobalMemoryBroker(1L << 32);
			producer = new SyncMemoryAllowance(broker);
			producer.setTargetMemory(1L << 30);
			reader = new SyncMemoryAllowance(broker);
			reader.setTargetMemory(1L << 30);
			//threshold above tile size so tiles are packed; large target and disabled seal timer so
			//sealing happens only through the paths under test
			cache = new OOCPackedCache(new OOCCacheImpl(new OOCMatrixIOHandler(), 1L << 30, 1L << 30),
				2 * BYTES, 100 * BYTES, -1, 0);
		}

		private void close() {
			cache.shutdown();
			producer.destroy();
			reader.destroy();
		}
	}
}
