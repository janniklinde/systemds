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

package org.apache.sysds.test.functions.ooc;

import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.stream.message.OOCRequestNoDataPipe;
import org.apache.sysds.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class OOCControlFlowTest {
	@Test
	public void testEnumerateCachedTilesWithoutData() {
		TestUtils.clearAssertionInformation();
		SubscribableTaskQueue<IndexedMatrixValue> source = new SubscribableTaskQueue<>();
		CachingStream cache = new CachingStream(source);
		cache.activateIndexing();

		MatrixIndexes ix1 = new MatrixIndexes(1, 1);
		MatrixIndexes ix2 = new MatrixIndexes(2, 1);
		source.enqueue(new IndexedMatrixValue(ix1, new MatrixBlock(1, 1, false)));
		source.enqueue(new IndexedMatrixValue(ix2, new MatrixBlock(1, 1, false)));
		source.closeInput();

		Set<MatrixIndexes> seen = new HashSet<>();
		OOCRequestNoDataPipe pipe = new OOCRequestNoDataPipe(tmp -> seen.add(tmp.getIndexes()));
		cache.messageUpstream(pipe);

		Assert.assertFalse("Enumeration should not be cancelled for simple flow", pipe.isCancelled());
		Assert.assertTrue(seen.contains(ix1));
		Assert.assertTrue(seen.contains(ix2));
	}

	@Test
	public void testEnumerationCancelledOnIXTransform() {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		stream.setIXTransform((downstream, range) -> range);
		OOCRequestNoDataPipe pipe = new OOCRequestNoDataPipe(tmp -> {});
		stream.messageUpstream(pipe);
		Assert.assertTrue("Enumeration should cancel when IX transform is present", pipe.isCancelled());
	}
}
