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

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ManagedPayload;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.junit.Assert;
import org.junit.Test;

public class ManagedPayloadExtractionTest {
	private static final long BYTES = 1000;

	@Test
	public void testExtractionDetachesReservation() {
		SyncMemoryAllowance allowance = newAllowance();
		try {
			InMemoryQueueCallback cb = newCallback(allowance);
			Assert.assertEquals(BYTES, allowance.getUsedMemory());

			ManagedPayload<IndexedMatrixValue> payload = cb.extractManagedPayload();
			Assert.assertEquals(BYTES, payload.bytes());
			Assert.assertSame(allowance, payload.owner());
			Assert.assertEquals(1, payload.value().getIndexes().getRowIndex());
			//detached, not released
			Assert.assertEquals(BYTES, allowance.getUsedMemory());

			//closing the emptied callback must not release the transferred bytes
			cb.close();
			Assert.assertEquals(BYTES, allowance.getUsedMemory());

			payload.release();
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			allowance.destroy();
		}
	}

	@Test
	public void testAliasPreventsExtraction() {
		SyncMemoryAllowance allowance = newAllowance();
		try {
			InMemoryQueueCallback cb = newCallback(allowance);
			OOCStream.QueueCallback<IndexedMatrixValue> alias = cb.keepOpen();
			try {
				cb.extractManagedPayload();
				Assert.fail("Extraction must fail while an alias exists");
			}
			catch(IllegalStateException expected) {
				//ownership must be unchanged
				Assert.assertEquals(BYTES, allowance.getUsedMemory());
				Assert.assertNotNull(cb.get());
			}

			alias.close();
			ManagedPayload<IndexedMatrixValue> payload = cb.extractManagedPayload();
			cb.close();
			payload.release();
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			allowance.destroy();
		}
	}

	@Test
	public void testReleaseIsExactlyOnce() {
		SyncMemoryAllowance allowance = newAllowance();
		try {
			InMemoryQueueCallback cb = newCallback(allowance);
			ManagedPayload<IndexedMatrixValue> payload = cb.extractManagedPayload();
			cb.close();

			payload.release();
			payload.release();
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			allowance.destroy();
		}
	}

	@Test
	public void testTransferSuppressesRelease() {
		SyncMemoryAllowance allowance = newAllowance();
		try {
			InMemoryQueueCallback cb = newCallback(allowance);
			ManagedPayload<IndexedMatrixValue> payload = cb.extractManagedPayload();
			cb.close();

			payload.transfer();
			try {
				payload.transfer();
				Assert.fail("A payload must be settled exactly once");
			}
			catch(IllegalStateException expected) {
				//expected
			}
			//release after transfer is a no-op; the bytes follow the cache pin/unpin protocol
			payload.release();
			Assert.assertEquals(BYTES, allowance.getUsedMemory());

			allowance.release(BYTES);
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			allowance.destroy();
		}
	}

	@Test
	public void testExtractionIsOneShot() {
		SyncMemoryAllowance allowance = newAllowance();
		try {
			InMemoryQueueCallback cb = newCallback(allowance);
			ManagedPayload<IndexedMatrixValue> payload = cb.extractManagedPayload();
			try {
				cb.extractManagedPayload();
				Assert.fail("Extraction must be one-shot");
			}
			catch(IllegalStateException expected) {
				//expected
			}
			cb.close();
			payload.release();
			Assert.assertEquals(0, allowance.getUsedMemory());
		}
		finally {
			allowance.destroy();
		}
	}

	private static SyncMemoryAllowance newAllowance() {
		GlobalMemoryBroker broker = new GlobalMemoryBroker(1L << 30);
		SyncMemoryAllowance allowance = new SyncMemoryAllowance(broker);
		allowance.setTargetMemory(1L << 30);
		return allowance;
	}

	private static InMemoryQueueCallback newCallback(SyncMemoryAllowance allowance) {
		allowance.reserveBlocking(BYTES);
		IndexedMatrixValue imv = new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 2, 1.0));
		return new InMemoryQueueCallback(imv, null, allowance, BYTES);
	}
}
