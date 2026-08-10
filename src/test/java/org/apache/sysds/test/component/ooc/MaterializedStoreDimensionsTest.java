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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheImpl;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.store.OOCStreamMaterializer;
import org.apache.sysds.test.component.ooc.cache.OOCCacheTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaterializedStoreDimensionsTest {
	private static final long MEMORY_LIMIT = 100_000_000;
	private static final long WAIT_SECONDS = 10;
	private static final int BLEN = 4;

	private GlobalMemoryBroker _broker;
	private SyncMemoryAllowance _materializerAllowance;
	private OOCCacheImpl _cache;
	private MaterializedStore<IndexedMatrixValue> _store;

	@Before
	public void setUp() {
		_broker = new GlobalMemoryBroker(1_000_000_000);
		_materializerAllowance = new SyncMemoryAllowance(_broker);
		_materializerAllowance.setTargetMemory(MEMORY_LIMIT);
		_cache = new OOCCacheImpl(new OOCCacheTestUtils.RecordingOOCIOHandler(), MEMORY_LIMIT, MEMORY_LIMIT);
	}

	@After
	public void tearDown() {
		if(_store != null)
			_store.close();
		_cache.shutdown();
		_materializerAllowance.destroy();
	}

	@Test
	public void testDimensionsResolveOnClosure() throws Exception {
		_store = newStore(null);
		OOCStreamMaterializer materializer = newMaterializer(BLEN);
		Assert.assertFalse(_store.dimensions().isDone());

		//out of order, and the boundary blocks are neither first nor last
		publish(materializer, 2, 1, 3, BLEN);
		publish(materializer, 1, 2, BLEN, BLEN);
		publish(materializer, 1, 1, BLEN, BLEN);
		publish(materializer, 2, 2, 3, BLEN);
		Assert.assertFalse(_store.dimensions().isDone());

		materializer.accept(OOCStream.eos(null));
		materializer.completion().get(WAIT_SECONDS, TimeUnit.SECONDS);

		DataCharacteristics resolved = _store.dimensions().get(WAIT_SECONDS, TimeUnit.SECONDS);
		Assert.assertEquals(BLEN + 3, resolved.getRows());
		Assert.assertEquals(BLEN + BLEN, resolved.getCols());
		Assert.assertEquals(BLEN, resolved.getBlocksize());
		Assert.assertEquals(2L * BLEN * BLEN + 2L * 3 * BLEN, resolved.getNonZeros());
	}

	@Test
	public void testDimensionsResolveBeforeCompletionSubscribers() throws Exception {
		_store = newStore(null);
		OOCStreamMaterializer materializer = newMaterializer(BLEN);
		boolean[] resolvedAtCompletion = new boolean[1];
		_store.completion().whenComplete((ignored, error) -> resolvedAtCompletion[0] = _store.dimensions().isDone());

		publish(materializer, 1, 1, 2, BLEN);
		materializer.accept(OOCStream.eos(null));
		materializer.completion().get(WAIT_SECONDS, TimeUnit.SECONDS);

		Assert.assertTrue("dimensions must be resolved before completion subscribers run", resolvedAtCompletion[0]);
	}

	@Test
	public void testKnownDimensionsResolveImmediately() {
		DataCharacteristics known = new MatrixCharacteristics(9, 5, BLEN, 45);
		_store = newStore(known);
		Assert.assertTrue(_store.dimensions().isDone());
	}

	@Test
	public void testEmptyStreamResolvesToEmptyDimensions() throws Exception {
		_store = newStore(null);
		OOCStreamMaterializer materializer = newMaterializer(BLEN);
		materializer.accept(OOCStream.eos(null));
		materializer.completion().get(WAIT_SECONDS, TimeUnit.SECONDS);

		DataCharacteristics resolved = _store.dimensions().get(WAIT_SECONDS, TimeUnit.SECONDS);
		Assert.assertEquals(0, resolved.getRows());
		Assert.assertEquals(0, resolved.getCols());
		Assert.assertEquals(0, resolved.getNonZeros());
	}

	@Test
	public void testFailedMaterializationFailsDimensions() throws Exception {
		_store = newStore(null);
		OOCStreamMaterializer materializer = newMaterializer(BLEN);
		publish(materializer, 1, 1, BLEN, BLEN);
		materializer.accept(OOCStream.eos(new DMLRuntimeException("injected")));

		try {
			_store.dimensions().get(WAIT_SECONDS, TimeUnit.SECONDS);
			Assert.fail("dimensions must not stay pending when materialization fails");
		}
		catch(ExecutionException expected) {
			Assert.assertTrue(expected.getCause().getMessage().contains("injected"));
		}
	}

	@Test
	public void testUntrackedCompletionFailsDimensions() throws Exception {
		_store = newStore(null);
		_store.complete();
		try {
			_store.dimensions().get(WAIT_SECONDS, TimeUnit.SECONDS);
			Assert.fail("dimensions must not stay pending when they were never observed");
		}
		catch(ExecutionException expected) {
			Assert.assertTrue(expected.getCause().getMessage().contains("without observing dimensions"));
		}
	}

	private MaterializedStore<IndexedMatrixValue> newStore(DataCharacteristics characteristics) {
		return new MaterializedStore<>(_cache, CachingStream._streamSeq.getNextID(), -1, 1,
			characteristics != null ? OOCStoreLayout.ROW_MAJOR : null, characteristics);
	}

	private OOCStreamMaterializer newMaterializer(int blocksize) {
		int[] next = new int[1];
		return new OOCStreamMaterializer(_store, indexes -> next[0]++, _materializerAllowance, List.of(), blocksize);
	}

	private static void publish(OOCStreamMaterializer materializer, long rowIndex, long colIndex, int rows, int cols) {
		IndexedMatrixValue value = new IndexedMatrixValue(new MatrixIndexes(rowIndex, colIndex),
			new MatrixBlock(rows, cols, 1.0));
		materializer.accept(new OOCStream.SimpleQueueCallback<>(value, null));
	}
}
