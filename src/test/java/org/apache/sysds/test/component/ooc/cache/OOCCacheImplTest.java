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

import static org.apache.sysds.test.component.ooc.cache.OOCCacheTestUtils.await;

import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.ooc.cache.BlockEntry;
import org.apache.sysds.runtime.ooc.cache.BlockKey;
import org.apache.sysds.runtime.ooc.cache.BlockState;
import org.apache.sysds.runtime.ooc.cache.collections.MaskedOnceArrayList;
import org.apache.sysds.runtime.ooc.cache.eviction.EvictController;
import org.apache.sysds.runtime.ooc.cache.eviction.IndexedObjectPair;
import org.apache.sysds.runtime.ooc.cache.OOCCache;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.test.component.ooc.cache.OOCCacheTestUtils.RecordingOOCIOHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OOCCacheImplTest {
	private static final long STREAM_ID = 7;
	private static final long BLOCK_ID = 3;
	private static final long BYTES = 1_000;
	private static final long WAIT_TIMEOUT_SEC = 10;

	private RecordingOOCIOHandler _io;
	private GlobalMemoryBroker _broker;
	private SyncMemoryAllowance _producer;
	private SyncMemoryAllowance _reader;
	private OOCCacheImpl _cache;

	@Before
	public void setUp() {
		_io = new RecordingOOCIOHandler();
		_broker = new GlobalMemoryBroker(8 * BYTES);
		_producer = new SyncMemoryAllowance(_broker, 4 * BYTES);
		_reader = new SyncMemoryAllowance(_broker, 4 * BYTES);
		_cache = new OOCCacheImpl(_io, 4 * BYTES, 4 * BYTES);
	}

	@After
	public void tearDown() {
		if(_cache != null)
			_cache.shutdown();
		if(_producer != null)
			_producer.destroy();
		if(_reader != null)
			_reader.destroy();
	}

	@Test
	public void testResidentPinCannotClearAnUnfinishedBackingRead() throws Exception {
		_cache.shutdown();
		_io = new RecordingOOCIOHandler() {
			@Override
			public OOCFuture<BlockEntry> scheduleRead(BlockEntry block) {
				OOCFuture<BlockEntry> read = super.scheduleRead(block);
				BlockEntry other = _cache.pin(block.getKey(), _producer).getNow(null);
				Assert.assertSame(block, other);
				_cache.unpin(other, _producer);
				return read;
			}
		};
		_cache = new OOCCacheImpl(_io, 0, 0);
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);
		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "pending-read", BYTES, _producer);
		_io.scheduleEviction(entry).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		_cache.markBacked(entry);
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		Assert.assertNull(BlockEntryTestAccess.getDataUnsafe(entry));
		BlockEntry pinned = _cache.pin(key, _reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		Assert.assertEquals("pending-read", pinned.getData());
		Assert.assertEquals(1, pinned.getPinCount());
		await(_cache.unpin(pinned, _reader), WAIT_TIMEOUT_SEC);
		Assert.assertEquals(0, _reader.getUsedMemory());
		Assert.assertEquals(0, _producer.getUsedMemory());
	}

	@Test
	public void testPinMissingEntryReturnsNullWithoutReservation() throws Exception {
		BlockEntry pinned = _cache.pin(new BlockKey(STREAM_ID, BLOCK_ID), _reader).get(WAIT_TIMEOUT_SEC,
			TimeUnit.SECONDS);

		Assert.assertNull(pinned);
		Assert.assertNull(_cache.pinIfLive(STREAM_ID, BLOCK_ID, _reader));
		Assert.assertEquals(0, _reader.getUsedMemory());
		Assert.assertEquals(0, _io.readCount());
	}

	@Test
	public void testResidentWeightedEvictionPreservesIndexOrder() {
		MaskedOnceArrayList<BlockEntry> large = new MaskedOnceArrayList<>();
		MaskedOnceArrayList<BlockEntry> small = new MaskedOnceArrayList<>();
		BlockEntry first = new BlockEntry(new BlockKey(1, 3998), 8000, "first", BlockState.HOT);
		BlockEntry last = new BlockEntry(new BlockKey(1, 3999), 8000, "last", BlockState.HOT);
		BlockEntry tiny = new BlockEntry(new BlockKey(2, 15999), 128, "tiny", BlockState.HOT);
		large.put(3998, first);
		large.put(3999, last);
		small.put(15999, tiny);
		EvictController largeController = new EvictController();
		EvictController smallController = new EvictController();
		largeController.updateResidentBytes(8_000_000, 3999);
		smallController.updateResidentBytes(512_000, 15999);
		PriorityQueue<IndexedObjectPair<BlockEntry>> candidates = new PriorityQueue<>();
		smallController.findEvictionCandidates(small, candidates, 1, 0);
		Assert.assertEquals(2, largeController.findEvictionCandidates(large, candidates, 1, 0));
		Assert.assertSame(last, candidates.peek().obj());
		Assert.assertEquals(7_998_000, candidates.peek().idx(), 0);
		largeController.updateResidentBytes(-7_999_000, 3999);
		candidates.clear();
		largeController.findEvictionCandidates(large, candidates, 1, 0);
		smallController.findEvictionCandidates(small, candidates, 1, 0);
		Assert.assertSame(tiny, candidates.peek().obj());
		largeController.addEvictionPolicy(index -> index - 4000);
		smallController.addEvictionPolicy(index -> index - 16000);
		candidates.clear();
		largeController.findEvictionCandidates(large, candidates, 1, 0);
		smallController.findEvictionCandidates(small, candidates, 1, 0);
		Assert.assertSame(tiny, candidates.peek().obj());
		Assert.assertEquals(-1d / 16000 / 512000, candidates.peek().idx(), 0);
	}

	@Test
	public void testProtectedSmallStreamSurvivesLargeContributor() {
		MaskedOnceArrayList<BlockEntry> large = new MaskedOnceArrayList<>();
		MaskedOnceArrayList<BlockEntry> small = new MaskedOnceArrayList<>();
		BlockEntry big = new BlockEntry(new BlockKey(1, 3000), 8000, "large", BlockState.HOT);
		BlockEntry tiny = new BlockEntry(new BlockKey(2, 3999), 128, "small", BlockState.HOT);
		large.put(3000, big);
		small.put(3999, tiny);
		EvictController largeController = new EvictController();
		EvictController smallController = new EvictController();
		largeController.updateResidentBytes(2_000_000_000, 3999);
		smallController.updateResidentBytes(128, 3999);
		largeController.addEvictionPolicy(index -> index - 4000);
		smallController.addEvictionPolicy(index -> index - 4000);
		PriorityQueue<IndexedObjectPair<BlockEntry>> candidates = new PriorityQueue<>();
		largeController.findEvictionCandidates(large, candidates, 1, 0);
		smallController.findEvictionCandidates(small, candidates, 1, 0);
		Assert.assertSame(big, candidates.peek().obj());
		Assert.assertEquals(-1000d / 4000 / 2_000_000_000, candidates.peek().idx(), 0);
	}

	@Test
	public void testEvictionSelectionStatsWithoutEvictions() {
		String stats = _cache.displayEvictionSelectionStats();
		Assert.assertTrue(stats.contains("0 passes, 0 scanned, 0 evicted (scan 0.000 sec, 0.00 scanned/evicted)"));
		Assert.assertTrue(stats.contains("eviction commit:\t0.000 sec (including cache-lock wait)"));
	}

	@Test
	public void testWeightedCacheSpillsLargeStreamBeforeSmallHighIndexTile() throws Exception {
		_cache.updateLimits(10 * BYTES, 10 * BYTES);
		_producer.reserveBlocking(3 * BYTES);
		BlockEntry large = _cache.putPinned(new BlockKey(1, 3999), "large", 3 * BYTES, _producer);
		await(_cache.unpin(large, _producer), WAIT_TIMEOUT_SEC);
		_producer.reserveBlocking(128);
		BlockEntry small = _cache.putPinned(new BlockKey(2, 15999), "small", 128, _producer);
		await(_cache.unpin(small, _producer), WAIT_TIMEOUT_SEC);
		_cache.updateLimits(10 * BYTES, 2 * BYTES);
		await(() -> _io.evictionCount() == 1 && BlockEntryTestAccess.getDataUnsafe(large) == null,
			WAIT_TIMEOUT_SEC);
		Assert.assertEquals(128, _cache.getOwnedCacheSize());
		String stats = _cache.displayEvictionSelectionStats();
		Assert.assertTrue(stats, stats.contains("1 passes, 2 scanned, 1 evicted"));
		Assert.assertTrue(stats, stats.contains("2.00 scanned/evicted"));
		BlockEntry pinned = _cache.pin(small.getKey(), _reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		Assert.assertEquals("small", pinned.getData());
		Assert.assertEquals(0, _io.readCount());
		await(_cache.unpin(pinned, _reader), WAIT_TIMEOUT_SEC);
		pinned = _cache.pin(large.getKey(), _reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		Assert.assertEquals("large", pinned.getData());
		Assert.assertEquals(1, _io.readCount());
		_cache.dereference(pinned);
		await(_cache.unpin(pinned, _reader), WAIT_TIMEOUT_SEC);
		Assert.assertEquals(128, _cache.getOwnedCacheSize());
	}

	@Test
	public void testResidentPinTransfersOwnershipBetweenCacheAndAllowance() throws Exception {
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);
		String payload = "resident";

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, payload, BYTES, _producer);
		Assert.assertEquals(0, _cache.getOwnedCacheSize());
		Assert.assertEquals(BYTES, _producer.getUsedMemory());

		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		Assert.assertEquals(BYTES, _cache.getOwnedCacheSize());
		Assert.assertEquals(0, _producer.getUsedMemory());

		BlockEntry pinned = _cache.pin(key, _reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		Assert.assertSame(entry, pinned);
		Assert.assertEquals(payload, pinned.getData());
		Assert.assertEquals(0, _cache.getOwnedCacheSize());
		Assert.assertEquals(BYTES, _reader.getUsedMemory());
		Assert.assertEquals(0, _io.readCount());

		await(_cache.unpin(pinned, _reader), WAIT_TIMEOUT_SEC);
		Assert.assertEquals(BYTES, _cache.getOwnedCacheSize());
		Assert.assertEquals(0, _reader.getUsedMemory());
	}

	@Test
	public void testPinReloadsColdBackedEntry() throws Exception {
		useEvictingCache();
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);
		String payload = "payload";

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, payload, BYTES, _producer);
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		await(() -> _io.evictionCount() == 1 && BlockEntryTestAccess.getDataUnsafe(entry) == null, WAIT_TIMEOUT_SEC);
		Assert.assertEquals(0, _producer.getUsedMemory());
		Assert.assertEquals(128, _cache.getMetadataSize());

		BlockEntry pinned = _cache.pin(key, _reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

		Assert.assertNotNull(pinned);
		Assert.assertEquals(entry.getKey(), pinned.getKey());
		Assert.assertEquals(payload, pinned.getData());
		Assert.assertEquals(1, _io.readCount());
		Assert.assertEquals(BYTES, _reader.getUsedMemory());

		await(_cache.unpin(pinned, _reader), WAIT_TIMEOUT_SEC);
		Assert.assertEquals(0, _reader.getUsedMemory());
		_cache.dereference(entry);
		Assert.assertEquals(0, _cache.getMetadataSize());
	}

	@Test
	public void testSharedReadProtectsPendingPinsFromEarlyUnpin() throws Exception {
		_cache.shutdown();
		OOCFuture<BlockEntry> read = new OOCFuture<>();
		BlockEntry[] reading = new BlockEntry[1];
		_io = new RecordingOOCIOHandler() {
			@Override
			public OOCFuture<BlockEntry> scheduleRead(BlockEntry block) {
				reading[0] = block;
				return read;
			}
		};
		_cache = new OOCCacheImpl(_io, 0, 0);
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);
		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "payload", BYTES, _producer);
		_cache.markBacked(entry);
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		Assert.assertNull(BlockEntryTestAccess.getDataUnsafe(entry));

		OOCFuture<BlockEntry> first = _cache.pin(key, _reader);
		OOCFuture<BlockEntry> second = _cache.pin(key, _reader);
		second.thenAccept(pinned -> _cache.unpin(pinned, _reader));
		Assert.assertEquals(2 * BYTES, _reader.getUsedMemory());
		BlockEntryTestAccess.setDataUnsafe(reading[0], "payload");
		read.complete(reading[0]);

		BlockEntry pinned = first.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		Assert.assertEquals(key, pinned.getKey());
		Assert.assertSame(pinned, second.get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS));
		Assert.assertEquals("payload", pinned.getData());
		Assert.assertEquals(1, pinned.getPinCount());
		Assert.assertEquals(BYTES, _reader.getUsedMemory());
		await(_cache.unpin(pinned, _reader), WAIT_TIMEOUT_SEC);
		Assert.assertEquals(0, pinned.getPinCount());
		Assert.assertEquals(0, _reader.getUsedMemory());
		Assert.assertNull(BlockEntryTestAccess.getDataUnsafe(pinned));
		_cache.dereference(pinned);
		Assert.assertEquals(0, _cache.getMetadataSize());
	}

	@Test
	public void testRetainedMetadataHasHardLimit() {
		BlockEntry[] entries = new BlockEntry[7];
		for(int i = 0; i < entries.length; i++) {
			_producer.reserveBlocking(1);
			entries[i] = _cache.putPinned(STREAM_ID, i, "tile", 1, _producer);
		}
		Assert.assertEquals(7 * 512, _cache.getMetadataSize());
		_producer.reserveBlocking(1);
		try {
			_cache.putPinned(STREAM_ID, entries.length, "tile", 1, _producer);
			Assert.fail("Expected cache metadata capacity failure");
		}
		catch(DMLRuntimeException expected) {
			Assert.assertTrue(expected.getMessage().contains("metadata exceeds its hard limit"));
		}
		finally {
			_producer.release(1);
		}
		for(BlockEntry entry : entries) {
			_cache.dereference(entry);
			_cache.unpin(entry, _producer);
		}
		Assert.assertEquals(0, _cache.getMetadataSize());
	}

	@Test
	public void testBackedLastUnpinDropsPayloadWhenMetadataFillsCache() throws Exception {
		useZeroHardLimitCache();
		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(STREAM_ID, BLOCK_ID, "backed", BYTES, _producer);
		_cache.markBacked(entry);

		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		Assert.assertNull(BlockEntryTestAccess.getDataUnsafe(entry));
		Assert.assertEquals(128, _cache.getMetadataSize());
		Assert.assertEquals(0, _producer.getUsedMemory());
		_cache.dereference(entry);
		Assert.assertEquals(0, _cache.getMetadataSize());
	}

	@Test
	public void testPinIfLiveDoesNotReadColdBackedEntry() throws Exception {
		useEvictingCache();
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);
		String payload = "cold";

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, payload, BYTES, _producer);
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		await(() -> _io.evictionCount() == 1 && BlockEntryTestAccess.getDataUnsafe(entry) == null, WAIT_TIMEOUT_SEC);

		BlockEntry pinned = _cache.pinIfLive(STREAM_ID, BLOCK_ID, _reader);

		Assert.assertNull(pinned);
		Assert.assertEquals(0, _io.readCount());
		Assert.assertEquals(0, _reader.getUsedMemory());
	}

	@Test
	public void testDeferredUnpinCommitsWhenLimitsGrow() throws Exception {
		useZeroHardLimitCache();
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "deferred", BYTES, _producer);
		OOCCache.UnpinHandle deferred = _cache.unpin(entry, _producer);

		Assert.assertFalse(deferred.isCommitted());
		Assert.assertFalse(deferred.getCompletionFuture().isDone());
		Assert.assertEquals(BYTES, _producer.getUsedMemory());
		Assert.assertEquals(0, _cache.getOwnedCacheSize());

		_cache.updateLimits(BYTES + _cache.getMetadataSize(), BYTES + _cache.getMetadataSize());
		deferred.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

		Assert.assertTrue(deferred.isCommitted());
		Assert.assertEquals(0, _producer.getUsedMemory());
		Assert.assertEquals(BYTES, _cache.getOwnedCacheSize());
	}

	@Test
	public void testDeferredUnpinCanBeAdoptedBySameAllowance() throws Exception {
		useZeroHardLimitCache();
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "adopt", BYTES, _producer);
		OOCCache.UnpinHandle deferred = _cache.unpin(entry, _producer);

		BlockEntry repinned = _cache.pin(key, _producer).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);

		Assert.assertSame(entry, repinned);
		Assert.assertTrue(deferred.getCompletionFuture().isDone());
		Assert.assertFalse(deferred.isCommitted());
		Assert.assertEquals(BYTES, _producer.getUsedMemory());
		Assert.assertEquals(0, _cache.getOwnedCacheSize());

		OOCCache.UnpinHandle cleanup = _cache.unpin(repinned, _producer);
		_cache.updateLimits(BYTES + _cache.getMetadataSize(), BYTES + _cache.getMetadataSize());
		cleanup.getCompletionFuture().get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
		Assert.assertEquals(0, _producer.getUsedMemory());
	}

	@Test
	public void testDereferenceRemovesEntryAfterLastUnpin() throws Exception {
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "drop", BYTES, _producer);

		Assert.assertEquals(0, _cache.dereference(entry));
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);

		Assert.assertEquals(0, _producer.getUsedMemory());
		Assert.assertNull(_cache.pin(key, _reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS));
		Assert.assertEquals(0, _reader.getUsedMemory());
	}

	@Test
	public void testBackingReadFailureReleasesReservedBytes() throws Exception {
		useEvictingCache();
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "fail-read", BYTES, _producer);
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		await(() -> _io.evictionCount() == 1 && BlockEntryTestAccess.getDataUnsafe(entry) == null, WAIT_TIMEOUT_SEC);

		_io.failReads(true);
		try {
			_cache.pin(key, _reader).get(WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
			Assert.fail("A failed backing read must fail the pin future.");
		}
		catch(ExecutionException expected) {
			// expected
		}

		Assert.assertEquals(1, _io.readCount());
		Assert.assertEquals(0, _reader.getUsedMemory());
	}

	@Test
	public void testActivateAdoptsColdEntryWithoutRead() throws Exception {
		useEvictingCache();
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "evicted", BYTES, _producer);
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		await(() -> _io.evictionCount() == 1 && BlockEntryTestAccess.getDataUnsafe(entry) == null, WAIT_TIMEOUT_SEC);

		Assert.assertTrue(_cache.activate(key, "read-ahead"));
		Assert.assertEquals(BYTES, _cache.getOwnedCacheSize());

		BlockEntry pinned = _cache.pinIfLive(STREAM_ID, BLOCK_ID, _reader);
		Assert.assertEquals(entry.getKey(), pinned.getKey());
		Assert.assertEquals("read-ahead", pinned.getData());
		Assert.assertEquals(0, _io.readCount());

		await(_cache.unpin(pinned, _reader), WAIT_TIMEOUT_SEC);
	}

	@Test
	public void testActivateDeclinesUnknownAndResidentEntries() throws Exception {
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);

		Assert.assertFalse(_cache.activate(key, "ghost"));

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "resident", BYTES, _producer);
		Assert.assertFalse(_cache.activate(key, "duplicate"));
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);
		Assert.assertFalse(_cache.activate(key, "duplicate"));
		Assert.assertEquals("resident", BlockEntryTestAccess.getDataUnsafe(entry));
	}

	@Test
	public void testActivateDeclinesWhenHardLimitIsExhausted() throws Exception {
		useZeroHardLimitCache();
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "no-room", BYTES, _producer);
		BlockEntryTestAccess.setDataUnsafe(entry, null);

		Assert.assertEquals(0, _cache.readAheadBudget());
		Assert.assertFalse(_cache.activate(key, "read-ahead"));
		Assert.assertEquals(0, _cache.getOwnedCacheSize());
	}

	@Test
	public void testReadAheadBudgetShrinksWithOwnedBytes() throws Exception {
		BlockKey key = new BlockKey(STREAM_ID, BLOCK_ID);
		long empty = _cache.readAheadBudget();
		Assert.assertTrue(empty > 0);

		_producer.reserveBlocking(BYTES);
		BlockEntry entry = _cache.putPinned(key, "owned", BYTES, _producer);
		await(_cache.unpin(entry, _producer), WAIT_TIMEOUT_SEC);

		Assert.assertEquals(empty - BYTES - _cache.getMetadataSize(), _cache.readAheadBudget());
	}

	private void useEvictingCache() {
		_cache.shutdown();
		_io.reset();
		_cache = new OOCCacheImpl(_io, 4 * BYTES, 0);
	}

	private void useZeroHardLimitCache() {
		_cache.shutdown();
		_io.reset();
		_cache = new OOCCacheImpl(_io, 0, 0);
	}
}
