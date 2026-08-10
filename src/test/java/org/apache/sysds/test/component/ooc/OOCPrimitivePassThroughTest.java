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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.primitives.MappingOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.MaterializeOOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.junit.Assert;
import org.junit.Test;

public class OOCPrimitivePassThroughTest {
	private static final AtomicLong STREAM_IDS = new AtomicLong(10_000);

	@Test
	public void testMappingPassThrough() {
		OOCCacheManager.reset();
		long streamId = STREAM_IDS.getAndIncrement();
		IndexedMatrixValue source = tile(7);
		MatrixBlock sourceBlock = (MatrixBlock) source.getValue();
		SubscribableTaskQueue<IndexedMatrixValue> input = stream();
		input.enqueue(OOCCacheManager.putAndPin(streamId, 0, source));
		input.closeInput();
		SubscribableTaskQueue<IndexedMatrixValue> output = stream();
		MappingOOCPrimitive primitive = new MappingOOCPrimitive(input, output, value -> (MatrixBlock) value.getValue(),
			new StreamContext());
		output.assignPrimitive(primitive);
		try {
			output.start();
			try(OOCStream.QueueCallback<IndexedMatrixValue> callback = output.dequeueCB()) {
				Assert.assertNotNull(callback);
				OOCCacheManager.forget(streamId, 0);
				Assert.assertSame(source, callback.get());
				Assert.assertSame(sourceBlock, callback.get().getValue());
				Assert.assertEquals(7, callback.get().getValue().get(0, 0), 0);
			}
			Assert.assertNull(output.dequeueCB());
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testMaterializationCopiesOutput() {
		OOCCacheManager.reset();
		long streamId = STREAM_IDS.getAndIncrement();
		IndexedMatrixValue source = tile(7);
		MatrixBlock sourceBlock = (MatrixBlock) source.getValue();
		SubscribableTaskQueue<IndexedMatrixValue> input = stream();
		input.enqueue(OOCCacheManager.putAndPin(streamId, 0, source));
		input.closeInput();
		SubscribableTaskQueue<IndexedMatrixValue> mapped = stream();
		MappingOOCPrimitive mapping = new MappingOOCPrimitive(input, mapped, value -> (MatrixBlock) value.getValue(),
			new StreamContext());
		mapped.assignPrimitive(mapping);
		SubscribableTaskQueue<IndexedMatrixValue> output = stream();
		MaterializeOOCPrimitive materialize = new MaterializeOOCPrimitive(mapped, OOCStoreLayout.ROW_MAJOR,
			new StreamContext());
		materialize.registerRequest(1, callback -> {
			if(callback.isEos()) {
				callback.close();
				output.closeInput();
			}
			else
				output.enqueue(callback.keepOpen());
		});
		try {
			materialize.start();
			try(OOCStream.QueueCallback<IndexedMatrixValue> callback = output.dequeueCB()) {
				OOCCacheManager.forget(streamId, 0);
				Assert.assertNotSame(source, callback.get());
				Assert.assertNotSame(sourceBlock, callback.get().getValue());
				Assert.assertEquals(7, callback.get().getValue().get(0, 0), 0);
			}
			Assert.assertNull(output.dequeueCB());
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testMappingCopiesModifiedOutput() {
		OOCCacheManager.reset();
		long streamId = STREAM_IDS.getAndIncrement();
		IndexedMatrixValue source = tile(7);
		MatrixBlock sourceBlock = (MatrixBlock) source.getValue();
		SubscribableTaskQueue<IndexedMatrixValue> input = stream();
		input.enqueue(OOCCacheManager.putAndPin(streamId, 0, source));
		input.closeInput();
		SubscribableTaskQueue<IndexedMatrixValue> output = stream();
		MappingOOCPrimitive primitive = new MappingOOCPrimitive(input, output,
			value -> new MatrixBlock((MatrixBlock) value.getValue()), new StreamContext());
		output.assignPrimitive(primitive);
		try {
			output.start();
			try(OOCStream.QueueCallback<IndexedMatrixValue> callback = output.dequeueCB()) {
				OOCCacheManager.forget(streamId, 0);
				Assert.assertNotSame(source, callback.get());
				Assert.assertNotSame(sourceBlock, callback.get().getValue());
				Assert.assertEquals(7, callback.get().getValue().get(0, 0), 0);
			}
			Assert.assertNull(output.dequeueCB());
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	private static IndexedMatrixValue tile(double value) {
		return new IndexedMatrixValue(new MatrixIndexes(1, 1), new MatrixBlock(2, 2, value));
	}

	private static SubscribableTaskQueue<IndexedMatrixValue> stream() {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		stream.setData(new MatrixObject(ValueType.FP64, "/dev/null",
			new MetaDataFormat(new MatrixCharacteristics(2, 2, 2, 4), FileFormat.BINARY)));
		return stream;
	}
}
