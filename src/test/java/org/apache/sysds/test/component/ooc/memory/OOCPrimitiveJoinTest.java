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

import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.BinaryOperator;
import org.apache.sysds.runtime.matrix.operators.RightScalarOperator;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.runtime.util.CommonThreadPool;
import org.apache.sysds.utils.stats.InfrastructureAnalyzer;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class OOCPrimitiveJoinTest {
	private static final int ROWS = 16384;//*2;
	private static final int COLS = 16384;//*2;
	private static final int K = 2;
	private static final int REPS = 1;
	private static int BLEN = 128;
	private static int TILES = ((ROWS-1) / BLEN + 1) * ((COLS-1) / BLEN + 1);
	private static volatile MatrixBlock _benchmarkSink;

	@Test
	public void test() {
		/*System.out.println("BLEN,Time[ms]");
		for(int pow = 4; pow <= 10; pow++) {
			BLEN = 2 << pow;
			TILES = ((ROWS-1) / BLEN + 1) * ((COLS-1) / BLEN + 1);
			for(int i = 0; i < REPS; i++) {
				testDataGenEquiMapChain();
			}

			long millis = System.currentTimeMillis();
			for(int i = 0; i < REPS; i++) {
				testDataGenEquiMapChain();
			}
			System.out.println(BLEN + "," + (System.currentTimeMillis() - millis));
		}*/
		for(int i = 0; i < 3; i++)
			testPrimitiveJoin();
		long millis = System.currentTimeMillis();
		for(int i = 0; i < REPS; i++)
			testPrimitiveJoin();
		System.out.println((System.currentTimeMillis()-millis) + "ms");
	}

	@Test
	public void testPlainMatrixBlockBenchmark() {
		for(int i = 0; i < 3; i++)
			runFullMatrixBlockChain();

		long millis = System.currentTimeMillis();
		for(int i = 0; i < REPS; i++)
			runFullMatrixBlockChain();
		System.out.println("Full MatrixBlock chain: " + (System.currentTimeMillis() - millis) + "ms");
	}

	@Test
	public void testFusedMatrixBlockBenchmark() {
		for(int i = 0; i < 5; i++)
			runFusedMatrixBlockChain();

		long millis = System.currentTimeMillis();
		for(int i = 0; i < REPS; i++)
			runFusedMatrixBlockChain();
		System.out.println("Fused MatrixBlock chain: " + (System.currentTimeMillis() - millis) + "ms");
	}

	private void testPrimitiveJoin() {
		List<List<OOCStream<IndexedMatrixValue>>> streamsList = new ArrayList<>();
		streamsList.add(new ArrayList<>());
		streamsList.add(new ArrayList<>());
		streamsList.add(new ArrayList<>());
		List<List<StreamContext>> scMapsList = new ArrayList<>();
		scMapsList.add(new ArrayList<>());
		scMapsList.add(new ArrayList<>());
		scMapsList.add(new ArrayList<>());
		for(int i = 0; i < 2; i++) {
			var streams = streamsList.get(i);
			var scMaps = scMapsList.get(i);
			for(int k = 0; k < K + 1; k++) {
				streams.add(createMatrixStream());
				scMaps.add(new StreamContext(0, "op_" + k).addOutStream(streams.get(streams.size() - 1)));
				if(k == 0)
					OOCInstructionUtils.dataGen(streams.get(k),
						ix -> new MatrixBlock(OOCUtils.getNumRowsOfTile(ix, ROWS, BLEN),
							OOCUtils.getNumColsOfTile(ix, COLS, BLEN), 5.0), scMaps.get(k));
				else
					OOCInstructionUtils.equiMap(streams.get(k - 1), streams.get(k),
						mb -> mb.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 2.0),
							new MatrixBlock()), scMaps.get(k));
			}
		}

		var streams = streamsList.get(2);
		var scMaps = scMapsList.get(2);
		var l = streamsList.get(0);
		var r = streamsList.get(1);
		r.add(createMatrixStream());
		scMapsList.get(1).add(new StreamContext(0, "op_transpose").addOutStream(r.get(r.size() - 1)));
		OOCInstructionUtils.transposedMap(r.get(r.size() - 2), r.get(r.size() - 1), MatrixBlock::transpose,
			scMapsList.get(1).get(scMapsList.get(1).size() - 1));

		streams.add(createMatrixStream());
		scMaps.add(new StreamContext(0, "op_join").addOutStream(streams.get(0)));
		OOCInstructionUtils.equiJoin(List.of(l.get(l.size()-1), r.get(r.size()-1)), streams.get(0),
			col -> col.get(0).binaryOperations(new BinaryOperator(Plus.getPlusFnObject()), col.get(1)),
			scMaps.get(0));
		for(int k = 1; k < K + 1; k++) {
			streams.add(createMatrixStream());
			scMaps.add(new StreamContext(0, "op_" + k).addOutStream(streams.get(streams.size() - 1)));
			OOCInstructionUtils.equiMap(streams.get(k - 1), streams.get(k),
				mb -> mb.scalarOperations(new RightScalarOperator(Plus.getPlusFnObject(), 2.0),
					new MatrixBlock()), scMaps.get(k));
		}

		streams.add(createMatrixStream(1, COLS));
		scMaps.add(new StreamContext(0, "op_col_reduce").addOutStream(streams.get(streams.size() - 1)));
		OOCInstructionUtils.colGroupedReduce(streams.get(streams.size() - 2), streams.get(streams.size() - 1),
			2, MatrixBlock::colSum,
			(leftBlock, rightBlock) -> leftBlock.binaryOperations(new BinaryOperator(Plus.getPlusFnObject()),
				rightBlock),
			mb -> mb, scMaps.get(scMaps.size() - 1));

		CompletableFuture<Void> future = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger();

		streams.get(streams.size()-1).setSubscriber(cb -> {
			try {
				if(cb.isEos()) {
					future.complete(null);
					return;
				}

				IndexedMatrixValue imv = cb.get();
				double checksum = ((MatrixBlock) imv.getValue()).sum();
				double expectedChecksum = fusedOutputValue() * ROWS * imv.getValue().getNumColumns();
				Assert.assertEquals("Wrong checksum at " + imv.getIndexes(), expectedChecksum, checksum, 1e-9);
				count.incrementAndGet();
			}
			catch(Throwable t) {
				future.completeExceptionally(t);
			}
			finally {
				cb.close();
			}
		});

		streams.get(streams.size()-1).start();
		future.join();

		Assert.assertEquals((COLS - 1) / BLEN + 1, count.get());
		try {
			assertMemoryReturned(streamsList);
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	private static OOCStream<IndexedMatrixValue> createMatrixStream() {
		return createMatrixStream(ROWS, COLS);
	}

	private static OOCStream<IndexedMatrixValue> createMatrixStream(long rows, long cols) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixCharacteristics dc = new MatrixCharacteristics(rows, cols, BLEN, -1);
		stream.setData(new MatrixObject(ValueType.FP64, null, new MetaDataFormat(dc, FileFormat.BINARY)));
		return stream;
	}

	private static void runFullMatrixBlockChain() {
		int numThreads = InfrastructureAnalyzer.getLocalParallelism();
		RightScalarOperator plusTwo = new RightScalarOperator(Plus.getPlusFnObject(), 2.0, numThreads);
		BinaryOperator plus = new BinaryOperator(Plus.getPlusFnObject(), numThreads);
		MatrixBlock left = runFullInputChain(plusTwo);
		MatrixBlock right = runFullInputChain(plusTwo);
		MatrixBlock out = left.binaryOperations(plus, right, new MatrixBlock());
		left = null;
		right = null;
		for(int k = 1; k < K; k++)
			out = out.scalarOperations(plusTwo, new MatrixBlock());
		_benchmarkSink = out;
	}

	private static MatrixBlock runFullInputChain(RightScalarOperator plusTwo) {
		MatrixBlock block = new MatrixBlock(ROWS, COLS, 5.0);
		for(int k = 1; k <= K; k++)
			block = block.scalarOperations(plusTwo, new MatrixBlock());
		return block;
	}

	private static void runFusedMatrixBlockChain() {
		double value = fusedOutputValue();
		MatrixBlock out = new MatrixBlock(ROWS, COLS, false);
		out.allocateDenseBlock(false);
		DenseBlock dense = out.getDenseBlock();
		int numThreads = InfrastructureAnalyzer.getLocalParallelism();
		ExecutorService pool = CommonThreadPool.get(numThreads);
		List<Future<?>> tasks = new ArrayList<>();
		int rowsPerTask = Math.max(1, (ROWS + numThreads - 1) / numThreads);

		for(int rowStart = 0; rowStart < ROWS; rowStart += rowsPerTask) {
			final int start = rowStart;
			final int end = Math.min(ROWS, rowStart + rowsPerTask);
			tasks.add(pool.submit(() -> {
				for(int r = start; r < end; r++) {
					double[] values = dense.values(r);
					int offset = dense.pos(r);
					for(int c = 0; c < COLS; c++)
						values[offset + c] = value;
				}
			}));
		}

		try {
			for(Future<?> task : tasks)
				task.get();
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		out.setAllNonZeros();
		_benchmarkSink = out;
	}

	private static double fusedOutputValue() {
		double left = 5.0;
		double right = 5.0;
		for(int k = 1; k <= K; k++) {
			left += 2.0;
			right += 2.0;
		}
		double out = left + right;
		for(int k = 1; k < K + 1; k++)
			out += 2.0;
		return out;
	}

	private static void assertMemoryReturned(List<List<OOCStream<IndexedMatrixValue>>> streamsList) {
		Set<OOCPrimitive> primitives = Collections.newSetFromMap(new IdentityHashMap<>());
		for(List<OOCStream<IndexedMatrixValue>> streams : streamsList)
			for(OOCStream<IndexedMatrixValue> stream : streams)
				collectPrimitives(stream.getPrimitive(), primitives);

		IdentityHashMap<MemoryAllowance, String> allowances = new IdentityHashMap<>();
		for(OOCPrimitive primitive : primitives) {
			MemoryAllowance allowance = getMemoryAllowance(primitive, "_allowance");
			if(allowance != null)
				allowances.putIfAbsent(allowance, primitive.getClass().getSimpleName());

			MemoryAllowance cache = getMemoryAllowance(primitive, "_cache");
			if(cache != null)
				allowances.putIfAbsent(cache, primitive.getClass().getSimpleName());
		}

		waitForAllowancesReturned(allowances.keySet());
		for(Map.Entry<MemoryAllowance, String> entry : allowances.entrySet())
			assertAllowanceReturned(entry.getValue(), entry.getKey());
	}

	private static void collectPrimitives(OOCPrimitive primitive, Set<OOCPrimitive> primitives) {
		if(primitive == null || !primitives.add(primitive))
			return;
		for(OOCPrimitive child : primitive.getChildren())
			collectPrimitives(child, primitives);
	}

	private static MemoryAllowance getMemoryAllowance(OOCPrimitive primitive, String fieldName) {
		try {
			Field field = findField(primitive.getClass(), fieldName);
			if(field == null)
				return null;
			field.setAccessible(true);
			return (MemoryAllowance) field.get(primitive);
		}
		catch(IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	private static Field findField(Class<?> clazz, String fieldName) {
		Class<?> current = clazz;
		while(current != null) {
			try {
				return current.getDeclaredField(fieldName);
			}
			catch(NoSuchFieldException ignored) {
				current = current.getSuperclass();
			}
		}
		return null;
	}

	private static void waitForAllowancesReturned(Set<MemoryAllowance> allowances) {
		long deadline = System.nanoTime() + 2_000_000_000L;
		while(System.nanoTime() < deadline) {
			boolean returned = true;
			for(MemoryAllowance allowance : allowances)
				returned &= allowance.getUsedMemory() == 0
					&& allowance.getGrantedMemory() == 0
					&& allowance.isShutdown();
			if(returned)
				return;
			try {
				Thread.sleep(10);
			}
			catch(InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(ex);
			}
		}
	}

	private static void assertAllowanceReturned(String primitiveName, MemoryAllowance allowance) {
		Assert.assertEquals("Allowance retained used memory for " + primitiveName, 0, allowance.getUsedMemory());
		Assert.assertEquals("Allowance retained granted memory for " + primitiveName, 0, allowance.getGrantedMemory());
		Assert.assertTrue("Allowance was not shut down for " + primitiveName, allowance.isShutdown());
	}
}
