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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.LocalVariableMap;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.ooc.AppendOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.BinaryOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.CtableOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.DataGenOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
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
import org.apache.sysds.runtime.ooc.store.MaterializedCallback;
import org.apache.sysds.runtime.ooc.store.StoreLease;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.junit.Assert;
import org.junit.Test;

public class OOCInstructionUtilsTest {
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
	public void testSubmitTasksClosesAutoCloseableValues() throws Exception {
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
