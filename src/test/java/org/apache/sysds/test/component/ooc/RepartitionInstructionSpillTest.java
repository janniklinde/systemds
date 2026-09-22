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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.IntPredicate;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.ValueType;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.conf.DMLConfig;
import org.apache.sysds.runtime.io.WriterBinaryBlock;
import org.apache.sysds.runtime.ooc.primitives.SparseMatrixVectorOOCPrimitive;
import org.apache.sysds.runtime.instructions.ooc.ReblockOOCInstruction;
import org.apache.sysds.runtime.ooc.store.PartitionedStoreStreamable;
import org.apache.sysds.runtime.controlprogram.LocalVariableMap;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.cp.IndexingCPInstruction;
import org.apache.sysds.runtime.instructions.cp.ScalarObject;
import org.apache.sysds.runtime.instructions.ooc.AggregateTernaryOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.AppendOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.BinaryOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.BuiltinNaryOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.CSVReblockOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.CentralMomentOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.CovarianceOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.IndexingOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.MMultOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.MapMMChainOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.ParameterizedBuiltinOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.QuaternaryOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.SubscribableTaskQueue;
import org.apache.sysds.runtime.instructions.ooc.TSMMOOCInstruction;
import org.apache.sysds.runtime.instructions.ooc.TernaryOOCInstruction;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.ooc.UnaryOOCInstruction;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.functionobjects.Multiply;
import org.apache.sysds.runtime.functionobjects.Plus;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.primitives.MappingOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.GeneralMMultOOCPrimitive;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.primitives.RepartitionOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.UncoordinatedDataGenOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.UnpartitionOOCPrimitive;
import org.apache.sysds.utils.Statistics;
import org.junit.Assert;
import org.junit.Test;

public class RepartitionInstructionSpillTest {
	@Test
	public void testCumulativeUnarySpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			input(ec, "A", 800, 800, 200, 4, 4, true, 1);
			MatrixObject output = matrixObject(800, 800, 200);
			ec.setVariable("R", output);
			UnaryOOCInstruction.parseInstruction("OOC°ucumk+°A·MATRIX·FP64°R·MATRIX·FP64").processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = output.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					Assert.assertEquals((value.getIndexes().getRowIndex() - 1) * 200 + 1, value.getValue().get(0, 0),
						0);
					blocks++;
				}
			Assert.assertEquals(16, blocks);
			Assert.assertNull("Cumulative unary initialized the legacy LRU cache",
				OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testAggregateTernarySpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			input(ec, "A", 400, 400, 200, 2, 2, false, 2);
			MatrixObject second = input(ec, "B", 400, 400, 200, 2, 2, true, 3);
			MatrixObject third = input(ec, "C", 400, 400, 200, 2, 2, true, 4);
			second.getDataCharacteristics().set(-1, -1, -1, -1);
			third.getDataCharacteristics().set(-1, -1, -1, -1);
			MatrixObject output = matrixObject(1, 400, 200);
			ec.setVariable("R", output);
			AggregateTernaryOOCInstruction
				.parseInstruction("OOC°tack+*°A·MATRIX·FP64°B·MATRIX·FP64°C·MATRIX·FP64°R·MATRIX·FP64")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = output.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					Assert.assertEquals(9_600, current.get().getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(2, blocks);
			Assert.assertNull("Aggregate ternary initialized the legacy LRU cache",
				OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testCentralMomentSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			input(ec, "A", 40_000, 1, 200, 200, 1, false, 2);
			input(ec, "W", 40_000, 1, 200, 200, 1, true, 1);
			CentralMomentOOCInstruction
				.parseInstruction("OOC°cm°A·MATRIX·FP64°W·MATRIX·FP64°" + "2·SCALAR·INT64·true°R·SCALAR·FP64°1")
				.processInstruction(ec);
			waitForSpill();
			Assert.assertEquals(0, ((ScalarObject) ec.getVariable("R")).getDoubleValue(), 0);
			Assert.assertNull("Central moment initialized the legacy LRU cache",
				OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testCovarianceSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			input(ec, "A", 40_000, 1, 200, 200, 1, false, 2);
			input(ec, "B", 40_000, 1, 200, 200, 1, true, 3);
			input(ec, "W", 40_000, 1, 200, 200, 1, true, 1);
			CovarianceOOCInstruction
				.parseInstruction("OOC°cov°A·MATRIX·FP64°B·MATRIX·FP64°" + "W·MATRIX·FP64°R·SCALAR·FP64")
				.processInstruction(ec);
			waitForSpill();
			Assert.assertEquals(0, ((ScalarObject) ec.getVariable("R")).getDoubleValue(), 0);
			Assert.assertNull("Covariance initialized the legacy LRU cache", OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testMapMMChainSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> xInput = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> vInput = new SubscribableTaskQueue<>();
			MatrixObject x = matrixObject(400, 400, 200);
			MatrixObject v = matrixObject(400, 1, 200);
			MatrixObject out = matrixObject(400, 1, 200);
			x.setStreamHandle(xInput);
			v.setStreamHandle(vInput);
			ec.setVariable("X", x);
			ec.setVariable("v", v);
			ec.setVariable("R", out);
			for(int row = 1; row <= 2; row++)
				for(int col = 1; col <= 2; col++)
					xInput.enqueue(tile(row, col, 200, 200, 1));
			xInput.closeInput();
			vInput.enqueue(tile(1, 1, 200, 1, 1));
			vInput.enqueue(tile(2, 1, 200, 1, 1));
			vInput.closeInput();
			v.getDataCharacteristics().set(-1, -1, -1, -1);

			MapMMChainOOCInstruction
				.parseInstruction("OOC°mapmmchain°X·MATRIX·FP64°v·MATRIX·FP64°" + "R·MATRIX·FP64°XtXv")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					Assert.assertEquals(160_000, current.get().getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(2, blocks);
			Assert.assertNull("MapMMChain initialized the legacy LRU cache", OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testMapMMChainWeightedSpill() throws InterruptedException {
		// X %*% v is 400 per row, so weighting by two gives 800 and subtracting two gives 398, each summed over the
		// 400 rows of a column
		assertWeightedChain("XtwXv", 2, 320_000);
		assertWeightedChain("XtXvy", 2, 159_200);
	}

	private static void assertWeightedChain(String type, double weight, double expected) throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> xInput = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> vInput = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> wInput = new SubscribableTaskQueue<>();
			MatrixObject x = matrixObject(400, 400, 200);
			MatrixObject v = matrixObject(400, 1, 200);
			MatrixObject w = matrixObject(400, 1, 200);
			MatrixObject out = matrixObject(400, 1, 200);
			x.setStreamHandle(xInput);
			v.setStreamHandle(vInput);
			w.setStreamHandle(wInput);
			ec.setVariable("X", x);
			ec.setVariable("v", v);
			ec.setVariable("w", w);
			ec.setVariable("R", out);
			for(int row = 1; row <= 2; row++)
				for(int col = 1; col <= 2; col++)
					xInput.enqueue(tile(row, col, 200, 200, 1));
			xInput.closeInput();
			for(int row = 1; row <= 2; row++) {
				vInput.enqueue(tile(row, 1, 200, 1, 1));
				wInput.enqueue(tile(row, 1, 200, 1, weight));
			}
			vInput.closeInput();
			wInput.closeInput();
			v.getDataCharacteristics().set(-1, -1, -1, -1);
			w.getDataCharacteristics().set(-1, -1, -1, -1);

			MapMMChainOOCInstruction
				.parseInstruction("OOC\u00b0mapmmchain\u00b0X\u00b7MATRIX\u00b7FP64\u00b0"
					+ "v\u00b7MATRIX\u00b7FP64\u00b0w\u00b7MATRIX\u00b7FP64\u00b0R\u00b7MATRIX\u00b7FP64\u00b0" + type)
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					Assert.assertEquals(expected, current.get().getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(2, blocks);
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testPartitionedMatrixVectorSourceSpill() throws Exception {
		boolean statistics = prepareSpillCache();
		DMLConfig previous = ConfigurationManager.getDMLConfig();
		Path directory = Files.createTempDirectory(Path.of("../data_dir"), "partition-matvec-");
		try {
			ConfigurationManager.setGlobalConfig(DMLConfig.parseDMLConfig(
				"<root>" + "<sysds.ooc.materialized.partition.bytes>8192</sysds.ooc.materialized.partition.bytes>"
					+ "<sysds.localtmpdir>../data_dir/partition-test-tmp</sysds.localtmpdir>"
					+ "<sysds.scratch>../data_dir/partition-test-scratch</sysds.scratch></root>"));
			MatrixBlock matrix = new MatrixBlock(800, 800, true);
			for(int row = 0; row < 800; row++)
				for(int col = row % 50; col < 800; col += 50)
					if((col / 50) % 2 == 0)
						matrix.set(row, col, row % 7 + 1);
			matrix.recomputeNonZeros();
			String path = directory.resolve("X").toString();
			new WriterBinaryBlock(1).writeMatrixToHDFS(matrix, path, 800, 800, 50, matrix.getNonZeros(), false);
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject disk = matrixObject(800, 800, 50, path, FileFormat.BINARY);
			disk.getDataCharacteristics().setNonZeros(matrix.getNonZeros());
			ec.setVariable("disk", disk);
			MatrixObject x = matrixObject(800, 800, 50);
			ec.setVariable("X", x);
			ReblockOOCInstruction.parseInstruction("OOC°rblk°disk·MATRIX·FP64°X·MATRIX·FP64°50").processInstruction(ec);
			Assert.assertTrue(x.getStreamable() instanceof PartitionedStoreStreamable);
			input(ec, "v", 800, 1, 50, 16, 1, false, 3);
			for(int repeat = 0; repeat < 2; repeat++) {
				MatrixObject out = matrixObject(800, 1, 50);
				ec.setVariable("R", out);
				MMultOOCInstruction.parseInstruction("OOC°ba+*°X·MATRIX·FP64°v·MATRIX·FP64°R·MATRIX·FP64°1")
					.processInstruction(ec);
				OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
				Assert.assertTrue(result.getPrimitive() instanceof SparseMatrixVectorOOCPrimitive);
				result.start();
				Assert.assertTrue(((PartitionedStoreStreamable) x.getStreamable()).isPartitioned());
				int rows = 0;
				OOCStream.QueueCallback<IndexedMatrixValue> callback;
				while((callback = result.dequeueCB()) != null) {
					try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
						IndexedMatrixValue value = current.get();
						MatrixBlock block = (MatrixBlock) value.getValue();
						int first = (int) (value.getIndexes().getRowIndex() - 1) * 50;
						for(int row = 0; row < block.getNumRows(); row++)
							Assert.assertEquals(24d * ((first + row) % 7 + 1), block.get(row, 0), 0);
						rows += block.getNumRows();
					}
				}
				Assert.assertEquals(800, rows);
				if(repeat == 0)
					OOCCacheManager.getGlobalCache().updateLimits(100_000, 50_000);
				input(ec, "v", 800, 1, 50, 16, 1, false, 3);
			}
			Assert.assertTrue("Second matvec did not reload evicted source partitions",
				Statistics.getOOCLoadFromDiskCount() > 0);
			x.getStreamable().scheduleMaterializedStoreDeletion();
		}
		finally {
			ConfigurationManager.setGlobalConfig(previous);
			reset(statistics);
		}
	}

	@Test(timeout = 20000)
	public void testPartitionConfigurationDoesNotForceOrdinarySourceConsumer() throws Exception {
		boolean statistics = prepareSpillCache();
		DMLConfig previous = ConfigurationManager.getDMLConfig();
		Path directory = Files.createTempDirectory(Path.of("../data_dir"), "partition-gated-source-");
		try {
			ConfigurationManager.setGlobalConfig(DMLConfig.parseDMLConfig(
				"<root><sysds.ooc.materialized.partition.bytes>8192</sysds.ooc.materialized.partition.bytes>"
					+ "<sysds.localtmpdir>../data_dir/partition-test-tmp</sysds.localtmpdir>"
					+ "<sysds.scratch>../data_dir/partition-test-scratch</sysds.scratch></root>"));
			MatrixBlock matrix = new MatrixBlock(400, 400, 3d);
			String path = directory.resolve("X").toString();
			new WriterBinaryBlock(1).writeMatrixToHDFS(matrix, path, 400, 400, 50, matrix.getNonZeros(), false);
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject disk = matrixObject(400, 400, 50, path, FileFormat.BINARY);
			disk.getDataCharacteristics().setNonZeros(matrix.getNonZeros());
			ec.setVariable("disk", disk);
			MatrixObject x = matrixObject(400, 400, 50);
			ec.setVariable("X", x);
			ReblockOOCInstruction.parseInstruction("OOC°rblk°disk·MATRIX·FP64°X·MATRIX·FP64°50")
				.processInstruction(ec);
			Assert.assertTrue(x.getStreamable() instanceof PartitionedStoreStreamable);
			PartitionedStoreStreamable source = (PartitionedStoreStreamable) x.getStreamable();
			OOCStream<IndexedMatrixValue> replay = source.getReadStream();
			replay.start();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = replay.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					Assert.assertEquals(3, current.get().getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(64, blocks);
			Assert.assertFalse("Configuration alone selected physical partitions", source.isPartitioned());
			source.scheduleMaterializedStoreDeletion();
		}
		finally {
			ConfigurationManager.setGlobalConfig(previous);
			reset(statistics);
		}
	}

	@Test(timeout = 20000)
	public void testLateTileConsumerUsesMemoizedUnpartitionView() throws Exception {
		boolean statistics = prepareSpillCache();
		DMLConfig previous = ConfigurationManager.getDMLConfig();
		Path directory = Files.createTempDirectory(Path.of("../data_dir"), "late-unpartition-");
		try {
			ConfigurationManager.setGlobalConfig(DMLConfig.parseDMLConfig(
				"<root><sysds.ooc.materialized.partition.bytes>8192</sysds.ooc.materialized.partition.bytes>"
					+ "<sysds.localtmpdir>../data_dir/partition-test-tmp</sysds.localtmpdir>"
					+ "<sysds.scratch>../data_dir/partition-test-scratch</sysds.scratch></root>"));
			MatrixBlock matrix = new MatrixBlock(400, 400, true);
			for(int row = 0; row < 400; row++)
				matrix.set(row, row, row + 1);
			matrix.recomputeNonZeros();
			String path = directory.resolve("X").toString();
			new WriterBinaryBlock(1).writeMatrixToHDFS(matrix, path, 400, 400, 50, matrix.getNonZeros(), false);
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject disk = matrixObject(400, 400, 50, path, FileFormat.BINARY);
			disk.getDataCharacteristics().setNonZeros(matrix.getNonZeros());
			ec.setVariable("disk", disk);
			MatrixObject x = matrixObject(400, 400, 50);
			ec.setVariable("X", x);
			ReblockOOCInstruction.parseInstruction("OOC°rblk°disk·MATRIX·FP64°X·MATRIX·FP64°50")
				.processInstruction(ec);

			input(ec, "v", 400, 1, 50, 8, 1, false, 1);
			MatrixObject product = matrixObject(400, 1, 50);
			ec.setVariable("P", product);
			MMultOOCInstruction.parseInstruction("OOC°ba+*°X·MATRIX·FP64°v·MATRIX·FP64°P·MATRIX·FP64°1")
				.processInstruction(ec);
			drain(product.getStreamHandle());
			PartitionedStoreStreamable source = (PartitionedStoreStreamable) x.getStreamable();
			Assert.assertTrue(source.isPartitioned());

			MatrixObject mapped = matrixObject(400, 400, 50);
			ec.setVariable("R", mapped);
			UnaryOOCInstruction.parseInstruction("OOC°abs°X·MATRIX·FP64°R·MATRIX·FP64")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = mapped.getStreamHandle();
			result.start();
			Assert.assertTrue(result.getPrimitive().getInput(0) instanceof MaterializedStoreStreamable);
			Assert.assertTrue(result.getPrimitive().getInput(0).getPrimitive() instanceof UnpartitionOOCPrimitive);
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					MatrixBlock block = (MatrixBlock) value.getValue();
					long first = (value.getIndexes().getRowIndex() - 1) * 50;
					if(value.getIndexes().getRowIndex() == value.getIndexes().getColumnIndex())
						Assert.assertEquals(first + 1, block.get(0, 0), 0);
					else
						Assert.assertTrue(block.isEmptyBlock(false));
					blocks++;
				}
			Assert.assertEquals(64, blocks);

			input(ec, "v2", 400, 1, 50, 8, 1, false, 1);
			MatrixObject secondProduct = matrixObject(400, 1, 50);
			ec.setVariable("P2", secondProduct);
			MMultOOCInstruction.parseInstruction("OOC°ba+*°X·MATRIX·FP64°v2·MATRIX·FP64°P2·MATRIX·FP64°1")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> second = secondProduct.getStreamHandle();
			second.start();
			Assert.assertTrue(second.getPrimitive().getInput(0) instanceof MaterializedStoreStreamable);
			drainStarted(second);
		}
		finally {
			ConfigurationManager.setGlobalConfig(previous);
			reset(statistics);
		}
	}

	@Test(timeout = 20000)
	public void testSmallMatrixMultiplyDoesNotEnableSourcePartitions() throws Exception {
		boolean statistics = prepareSpillCache();
		DMLConfig previous = ConfigurationManager.getDMLConfig();
		Path directory = Files.createTempDirectory(Path.of("../data_dir"), "partition-small-matrix-");
		try {
			ConfigurationManager.setGlobalConfig(DMLConfig.parseDMLConfig(
				"<root><sysds.ooc.materialized.partition.bytes>8192</sysds.ooc.materialized.partition.bytes>"
					+ "<sysds.localtmpdir>../data_dir/partition-test-tmp</sysds.localtmpdir>"
					+ "<sysds.scratch>../data_dir/partition-test-scratch</sysds.scratch></root>"));
			MatrixBlock matrix = new MatrixBlock(15000, 20, 2d);
			String path = directory.resolve("X").toString();
			new WriterBinaryBlock(1).writeMatrixToHDFS(matrix, path, 15000, 20, 50, matrix.getNonZeros(), false);
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject disk = matrixObject(15000, 20, 50, path, FileFormat.BINARY);
			disk.getDataCharacteristics().setNonZeros(matrix.getNonZeros());
			ec.setVariable("disk", disk);
			MatrixObject x = matrixObject(15000, 20, 50);
			ec.setVariable("X", x);
			ReblockOOCInstruction.parseInstruction("OOC°rblk°disk·MATRIX·FP64°X·MATRIX·FP64°50")
				.processInstruction(ec);
			Assert.assertTrue(x.getStreamable() instanceof PartitionedStoreStreamable);
			input(ec, "B", 20, 8, 50, 1, 1, false, 3);
			MatrixObject out = matrixObject(15000, 8, 50);
			ec.setVariable("R", out);
			MMultOOCInstruction.parseInstruction("OOC°ba+*°X·MATRIX·FP64°B·MATRIX·FP64°R·MATRIX·FP64°1")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			Assert.assertTrue(result.getPrimitive() instanceof GeneralMMultOOCPrimitive);
			result.start();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null) {
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					if(current instanceof OOCStream.GroupQueueCallback<?>) {
						@SuppressWarnings("unchecked")
						OOCStream.GroupQueueCallback<IndexedMatrixValue> group =
							(OOCStream.GroupQueueCallback<IndexedMatrixValue>) current;
						for(int i = 0; i < group.size(); i++) {
							try(OOCStream.QueueCallback<IndexedMatrixValue> item = group.getCallback(i)) {
								MatrixBlock block = (MatrixBlock) item.get().getValue();
								Assert.assertEquals(120, block.get(0, 0), 0);
								Assert.assertEquals(120, block.get(block.getNumRows() - 1, 7), 0);
								blocks++;
							}
						}
					}
					else {
						MatrixBlock block = (MatrixBlock) current.get().getValue();
						Assert.assertEquals(120, block.get(0, 0), 0);
						Assert.assertEquals(120, block.get(block.getNumRows() - 1, 7), 0);
						blocks++;
					}
				}
			}
			Assert.assertEquals(300, blocks);
			Assert.assertFalse(((PartitionedStoreStreamable) x.getStreamable()).isPartitioned());
			x.getStreamable().scheduleMaterializedStoreDeletion();
		}
		finally {
			ConfigurationManager.setGlobalConfig(previous);
			reset(statistics);
		}
	}

	@Test(timeout = 20000)
	public void testMappingDoesNotEnableSourcePartitions() throws Exception {
		boolean statistics = prepareSpillCache();
		DMLConfig previous = ConfigurationManager.getDMLConfig();
		Path directory = Files.createTempDirectory(Path.of("../data_dir"), "partition-map-");
		try {
			ConfigurationManager.setGlobalConfig(DMLConfig.parseDMLConfig(
				"<root><sysds.ooc.materialized.partition.bytes>8192</sysds.ooc.materialized.partition.bytes>"
					+ "<sysds.localtmpdir>../data_dir/partition-test-tmp</sysds.localtmpdir>"
					+ "<sysds.scratch>../data_dir/partition-test-scratch</sysds.scratch></root>"));
			MatrixBlock matrix = new MatrixBlock(20000, 20, -2d);
			String path = directory.resolve("X").toString();
			new WriterBinaryBlock(1).writeMatrixToHDFS(matrix, path, 20000, 20, 50, matrix.getNonZeros(), false);
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject disk = matrixObject(20000, 20, 50, path, FileFormat.BINARY);
			disk.getDataCharacteristics().setNonZeros(matrix.getNonZeros());
			ec.setVariable("disk", disk);
			MatrixObject x = matrixObject(20000, 20, 50);
			ec.setVariable("X", x);
			ReblockOOCInstruction.parseInstruction("OOC°rblk°disk·MATRIX·FP64°X·MATRIX·FP64°50")
				.processInstruction(ec);
			MatrixObject out = matrixObject(20000, 20, 50);
			ec.setVariable("R", out);
			UnaryOOCInstruction.parseInstruction("OOC°abs°X·MATRIX·FP64°R·MATRIX·FP64")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			Assert.assertTrue(result.getPrimitive() instanceof MappingOOCPrimitive);
			MaterializedStoreStreamable materialized = new MaterializedStoreStreamable(result, out);
			OOCStream<IndexedMatrixValue> replay = materialized.getReadStream();
			replay.start();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = replay.dequeueCB()) != null) {
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					MatrixBlock block = (MatrixBlock) current.get().getValue();
					Assert.assertEquals(2, block.get(0, 0), 0);
					Assert.assertEquals(2, block.get(block.getNumRows() - 1, 19), 0);
					blocks++;
				}
			}
			Assert.assertEquals(400, blocks);
			Assert.assertFalse(((PartitionedStoreStreamable) x.getStreamable()).isPartitioned());
			waitForSpill();
			materialized.scheduleMaterializedStoreDeletion();
			x.getStreamable().scheduleMaterializedStoreDeletion();
		}
		finally {
			ConfigurationManager.setGlobalConfig(previous);
			reset(statistics);
		}
	}

	@Test(timeout = 20000)
	public void testMaterializedMMultSpill() throws InterruptedException {
		DMLConfig previous = ConfigurationManager.getDMLConfig();
		boolean statistics = DMLScript.OOC_STATISTICS;
		try {
			ConfigurationManager.setGlobalConfig(DMLConfig.parseDMLConfig("<root>"
				+ "<sysds.localtmpdir>../data_dir/streaming-mm-test-tmp</sysds.localtmpdir>"
				+ "<sysds.scratch>../data_dir/streaming-mm-test-scratch</sysds.scratch></root>"));
			prepareSpillCache();
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject a = input(ec, "A", 800, 800, 200, 4, 4, false, 2);
			MatrixObject b = input(ec, "B", 800, 800, 200, 4, 4, true, 3);
			MatrixObject out = matrixObject(800, 800, 200);
			OOCStream<IndexedMatrixValue> result = new SubscribableTaskQueue<>();
			result.setData(out);
			result.assignPrimitive(new GeneralMMultOOCPrimitive(a.getStreamable(), b.getStreamable(), result,
				new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), new AggregateOperator(0, Plus.getPlusFnObject())),
				InstructionUtils.parseBinaryOperator("+"), false, new StreamContext()));
			Assert.assertEquals(2, result.getPrimitive().requiredMaterializedInputs().size());
			result.start();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null) {
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					MatrixBlock block = (MatrixBlock) current.get().getValue();
					for(int row = 0; row < block.getNumRows(); row++)
						for(int col = 0; col < block.getNumColumns(); col++)
							Assert.assertEquals(4800, block.get(row, col), 0);
					blocks++;
				}
			}
			Assert.assertEquals(16, blocks);
			waitForSpill();
		}
		finally {
			ConfigurationManager.setGlobalConfig(previous);
			reset(statistics);
		}
	}

	@Test(timeout = 20000)
	public void testStreamingMMultSpill() throws InterruptedException {
		boolean statistics = DMLScript.OOC_STATISTICS;
		DMLConfig previous = ConfigurationManager.getDMLConfig();
		try {
			ConfigurationManager.setGlobalConfig(DMLConfig.parseDMLConfig("<root>"
				+ "<sysds.localtmpdir>../data_dir/streaming-mm-test-tmp</sysds.localtmpdir>"
				+ "<sysds.scratch>../data_dir/streaming-mm-test-scratch</sysds.scratch></root>"));
			prepareSpillCache();
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			indexedInput(ec, "X", 800, 601, 200);
			input(ec, "B", 601, 405, 200, 4, 3, true, 3);
			MatrixObject out = matrixObject(800, 405, 200);
			ec.setVariable("R", out);
			MMultOOCInstruction.parseInstruction("OOC°ba+*°X·MATRIX·FP64°B·MATRIX·FP64°R·MATRIX·FP64°1")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			Assert.assertTrue(result.getPrimitive() instanceof GeneralMMultOOCPrimitive);
			Assert.assertTrue(result.getPrimitive().requiredMaterializedInputs().isEmpty());
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null) {
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					MatrixBlock block = (MatrixBlock) value.getValue();
					int firstRow = (int) (value.getIndexes().getRowIndex() - 1) * 200;
					for(int row = 0; row < block.getNumRows(); row++)
						for(int col = 0; col < block.getNumColumns(); col++)
							Assert.assertEquals(3 * (601d * (firstRow + row) * 1000 + 601d * 602 / 2),
								block.get(row, col), 0);
					blocks++;
				}
			}
			Assert.assertEquals(12, blocks);
		}
		finally {
			ConfigurationManager.setGlobalConfig(previous);
			reset(statistics);
		}
	}

	@Test(timeout = 20000)
	public void testStreamingJoinMaterializedViewsSpill() throws InterruptedException {
		testJoinMaterializedViewsSpill(true);
	}

	@Test(timeout = 20000)
	public void testBackpressuredJoinMaterializedViewsSpill() throws InterruptedException {
		testJoinMaterializedViewsSpill(false);
	}

	private void testJoinMaterializedViewsSpill(boolean streaming) throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			indexedInput(ec, "A", 800, 601, 200);
			MatrixObject a = ec.getMatrixObject("A");
			MatrixObject b = input(ec, "B", 800, 601, 200, 4, 4, true, 3);
			MaterializedStoreStreamable left = new MaterializedStoreStreamable(a.getStreamHandle(), a);
			MaterializedStoreStreamable right = new MaterializedStoreStreamable(b.getStreamHandle(), b,
				OOCStoreLayout.COL_MAJOR);
			OOCStream<IndexedMatrixValue> prime = left.getReadStream();
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = prime.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					current.get();
				}
			waitForSpill();
			OOCStream<IndexedMatrixValue> result = new SubscribableTaskQueue<>();
			result.setData(matrixObject(800, 601, 200));
			OOCInstructionUtils.equiJoin(left, right, result,
				(l, r) -> l.binaryOperations(InstructionUtils.parseBinaryOperator("+"), r, new MatrixBlock()),
				streaming, new StreamContext());
			result.start();
			int blocks = 0;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					MatrixBlock block = (MatrixBlock) value.getValue();
					int firstRow = (int) (value.getIndexes().getRowIndex() - 1) * 200;
					int firstCol = (int) (value.getIndexes().getColumnIndex() - 1) * 200;
					for(int row = 0; row < block.getNumRows(); row++)
						for(int col = 0; col < block.getNumColumns(); col++)
							Assert.assertEquals(indexedValue(firstRow + row, firstCol + col) + 3,
								block.get(row, col), 0);
					blocks++;
				}
			Assert.assertEquals(16, blocks);
			left.scheduleMaterializedStoreDeletion();
			right.scheduleMaterializedStoreDeletion();
		}
		finally {
			reset(statistics);
		}
	}

	@Test(timeout = 20000)
	public void testStreamingMMultMaterializedViewsSpill() throws InterruptedException {
		boolean statistics = DMLScript.OOC_STATISTICS;
		DMLConfig previous = ConfigurationManager.getDMLConfig();
		try {
			ConfigurationManager.setGlobalConfig(DMLConfig.parseDMLConfig("<root>"
				+ "<sysds.localtmpdir>../data_dir/streaming-mm-test-tmp</sysds.localtmpdir>"
				+ "<sysds.scratch>../data_dir/streaming-mm-test-scratch</sysds.scratch></root>"));
			prepareSpillCache();
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			indexedInput(ec, "A", 800, 601, 200);
			MatrixObject a = ec.getMatrixObject("A");
			MatrixObject b = input(ec, "B", 601, 405, 200, 4, 3, true, 3);
			MaterializedStoreStreamable left = new MaterializedStoreStreamable(a.getStreamHandle(), a);
			MaterializedStoreStreamable right = new MaterializedStoreStreamable(b.getStreamHandle(), b,
				OOCStoreLayout.COL_MAJOR);
			OOCStream<IndexedMatrixValue> prime = left.getReadStream();
			OOCStream.QueueCallback<IndexedMatrixValue> primed;
			while((primed = prime.dequeueCB()) != null) {
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = primed) {
					current.get();
				}
			}
			MatrixObject out = matrixObject(800, 405, 200);
			OOCStream<IndexedMatrixValue> result = new SubscribableTaskQueue<>();
			result.setData(out);
			result.assignPrimitive(new GeneralMMultOOCPrimitive(left, right, result,
				new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), new AggregateOperator(0, Plus.getPlusFnObject())),
				InstructionUtils.parseBinaryOperator("+"), true, new StreamContext()));
			result.start();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null) {
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					MatrixBlock block = (MatrixBlock) value.getValue();
					int firstRow = (int) (value.getIndexes().getRowIndex() - 1) * 200;
					for(int row = 0; row < block.getNumRows(); row++)
						for(int col = 0; col < block.getNumColumns(); col++)
							Assert.assertEquals(3 * (601d * (firstRow + row) * 1000 + 601d * 602 / 2),
								block.get(row, col), 0);
					blocks++;
				}
			}
			Assert.assertEquals(12, blocks);
			waitForSpill();
			left.scheduleMaterializedStoreDeletion();
			right.scheduleMaterializedStoreDeletion();
		}
		finally {
			ConfigurationManager.setGlobalConfig(previous);
			reset(statistics);
		}
	}

	@Test
	public void testSparseMatrixVectorMMultSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			indexedInput(ec, "X", 1600, 1600, 200);
			input(ec, "v", 1600, 1, 200, 8, 1, false, 3);
			MatrixObject out = matrixObject(1600, 1, 200);
			ec.setVariable("R", out);

			MMultOOCInstruction.parseInstruction("OOC°ba+*°X·MATRIX·FP64°v·MATRIX·FP64°R·MATRIX·FP64°1")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					MatrixBlock block = (MatrixBlock) current.get().getValue();
					long row = current.get().getIndexes().getRowIndex() - 1;
					int first = (int) row * 200;
					Assert.assertEquals(3 * (1600d * first * 1000 + 1600d * 1601 / 2), block.get(0, 0), 0);
					blocks++;
				}
			waitForSpill();
			Assert.assertEquals(8, blocks);
			Assert.assertNull("Sparse matrix-vector multiply initialized the legacy LRU cache",
				OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testAliasedMMultSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			input(ec, "X", 400, 400, 200, 2, 2);
			MatrixObject out = matrixObject(400, 400, 200);
			ec.setVariable("R", out);

			MMultOOCInstruction.parseInstruction("OOC°ba+*°X·MATRIX·FP64°X·MATRIX·FP64°R·MATRIX·FP64°1")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					Assert.assertEquals(400, current.get().getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(4, blocks);
			Assert.assertNull("Aliased MMult initialized the legacy LRU cache",
				OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testUnknownGeometryBinarySpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			indexedInput(ec, "A", 400, 400, 200);
			MatrixObject first = ec.getMatrixObject("A");
			MatrixObject second = input(ec, "B", 400, 400, 200, 2, 2, false, 2);
			first.getDataCharacteristics().set(-1, -1, 200, -1);
			second.getDataCharacteristics().set(-1, -1, 200, -1);
			MatrixObject out = matrixObject(400, 400, 200);
			ec.setVariable("R", out);

			BinaryOOCInstruction.parseInstruction("OOC°+°A·MATRIX·FP64°B·MATRIX·FP64°R·MATRIX·FP64")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					int row = (int) (value.getIndexes().getRowIndex() - 1) * 200;
					int col = (int) (value.getIndexes().getColumnIndex() - 1) * 200;
					Assert.assertEquals(indexedValue(row, col) + 2, value.getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(4, blocks);
			Assert.assertNull("Binary initialized the legacy LRU cache", OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testUnknownGeometryTernarySpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject first = input(ec, "A", 400, 400, 200, 2, 2, true, 1);
			MatrixObject second = input(ec, "B", 400, 400, 200, 2, 2, false, 1);
			MatrixObject third = input(ec, "C", 400, 400, 200, 2, 2, true, 1);
			first.getDataCharacteristics().set(-1, -1, 200, -1);
			second.getDataCharacteristics().set(-1, -1, 200, -1);
			third.getDataCharacteristics().set(-1, -1, 200, -1);
			MatrixObject out = matrixObject(400, 400, 200);
			ec.setVariable("R", out);

			TernaryOOCInstruction.parseInstruction("OOC°+*°A·MATRIX·FP64°B·MATRIX·FP64°C·MATRIX·FP64°R·MATRIX·FP64°1")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					Assert.assertEquals(2, current.get().getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(4, blocks);
			Assert.assertNull("Ternary initialized the legacy LRU cache", OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testWDivMMSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			input(ec, "X", 400, 400, 200, 2, 2);
			input(ec, "U", 400, 400, 200, 2, 2);
			MatrixObject v = input(ec, "V", 400, 400, 200, 2, 2);
			v.getDataCharacteristics().set(-1, -1, -1, -1);
			MatrixObject out = matrixObject(400, 400, 200);
			ec.setVariable("R", out);

			QuaternaryOOCInstruction.parseInstruction("OOC°mapwdivmm°X·MATRIX·FP64°U·MATRIX·FP64°"
				+ "V·MATRIX·FP64°-1·SCALAR·INT64·true°R·MATRIX·FP64°MULT_LEFT").processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					Assert.assertEquals(160_000, current.get().getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(4, blocks);
			Assert.assertNull("WDivMM initialized the legacy LRU cache", OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testCSVReblockEmptyBlocks() throws IOException {
		Path csv = Files.createTempFile("systemds-ooc-empty-csv-", ".csv");
		String row = "0,".repeat(29) + "0\n";
		Files.writeString(csv, row.repeat(20));
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject csvInput = matrixObject(-1, -1, 1000, csv.toString(), FileFormat.CSV);
			MatrixObject output = matrixObject(-1, -1, 16);
			ec.setVariable("X", csvInput);
			ec.setVariable("A", output);
			CSVReblockOOCInstruction.parseInstruction("OOC°csvrblk°X·MATRIX·FP64°A·MATRIX·FP64°16°true")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> stream = output.getStreamHandle();
			stream.start();
			int blocks = 0;
			boolean[][] seen = new boolean[2][2];
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = stream.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					int blockRow = (int) value.getIndexes().getRowIndex() - 1;
					int blockCol = (int) value.getIndexes().getColumnIndex() - 1;
					Assert.assertFalse(seen[blockRow][blockCol]);
					seen[blockRow][blockCol] = true;
					Assert.assertEquals(0, value.getValue().getNonZeros());
					blocks++;
				}
			Assert.assertEquals(4, blocks);
			Assert.assertEquals(20, output.getNumRows());
			Assert.assertEquals(30, output.getNumColumns());
		}
		finally {
			OOCCacheManager.reset();
			Files.deleteIfExists(csv);
		}
	}

	@Test
	public void testRightIndexingSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
			MatrixObject in = matrixObject(400, 400, 200);
			MatrixObject out = matrixObject(300, 300, 200);
			in.setStreamHandle(input);
			ec.setVariable("A", in);
			ec.setVariable("C", out);
			input.enqueue(tile(1, 1, 200, 200, 11));
			in.getDataCharacteristics().set(-1, -1, 200, -1);

			IndexingCPInstruction cp = IndexingCPInstruction
				.parseInstruction("CP°rightIndex°A·MATRIX·FP64°51·SCALAR·INT64·true°350·SCALAR·INT64·true°"
					+ "51·SCALAR·INT64·true°350·SCALAR·INT64·true°C·MATRIX·FP64");
			IndexingOOCInstruction.parseInstruction(cp).processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();

			input.enqueue(tile(1, 2, 200, 200, 12));
			input.enqueue(tile(2, 1, 200, 200, 21));
			input.enqueue(tile(2, 2, 200, 200, 22));
			input.closeInput();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					if(value.getIndexes().equals(new MatrixIndexes(1, 1))) {
						MatrixBlock block = (MatrixBlock) value.getValue();
						Assert.assertEquals(11, block.get(0, 0), 0);
						Assert.assertEquals(12, block.get(0, 199), 0);
						Assert.assertEquals(21, block.get(199, 0), 0);
						Assert.assertEquals(22, block.get(199, 199), 0);
					}
					blocks++;
				}
			Assert.assertEquals(4, blocks);
			Assert.assertNull("Right indexing initialized the legacy LRU cache",
				OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testScalarLeftIndexing() {
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> input = new SubscribableTaskQueue<>();
			MatrixObject in = matrixObject(400, 400, 200);
			MatrixObject out = matrixObject(400, 400, 200);
			in.setStreamHandle(input);
			ec.setVariable("A", in);
			ec.setVariable("C", out);
			input.enqueue(tile(1, 1, 200, 200, 1));
			input.enqueue(tile(1, 2, 200, 200, 1));
			input.enqueue(tile(2, 1, 200, 200, 1));
			input.enqueue(tile(2, 2, 200, 200, 1));
			input.closeInput();

			IndexingOOCInstruction.parseInstruction("CP°leftIndex°A·MATRIX·FP64°7·SCALAR·FP64·true°"
				+ "251·SCALAR·INT64·true°251·SCALAR·INT64·true°251·SCALAR·INT64·true°"
				+ "251·SCALAR·INT64·true°C·MATRIX·FP64").processInstruction(ec);
			Assert.assertTrue(out.getStreamable().getPrimitive() instanceof MappingOOCPrimitive);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					MatrixBlock block = (MatrixBlock) value.getValue();
					if(value.getIndexes().equals(new MatrixIndexes(2, 2))) {
						Assert.assertEquals(7, block.get(50, 50), 0);
						Assert.assertEquals(1, block.get(50, 51), 0);
					}
					blocks++;
				}
			Assert.assertEquals(4, blocks);
		}
		finally {
			OOCCacheManager.reset();
		}
	}

	@Test
	public void testLeftIndexingSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> leftInput = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> rightInput = new SubscribableTaskQueue<>();
			MatrixObject left = matrixObject(400, 400, 200);
			MatrixObject right = matrixObject(300, 300, 150);
			MatrixObject out = matrixObject(400, 400, 200);
			left.setStreamHandle(leftInput);
			right.setStreamHandle(rightInput);
			ec.setVariable("A", left);
			ec.setVariable("B", right);
			ec.setVariable("C", out);
			leftInput.enqueue(tile(1, 1, 200, 200, 1));

			IndexingOOCInstruction.parseInstruction("CP°leftIndex°A·MATRIX·FP64°B·MATRIX·FP64°"
				+ "51·SCALAR·INT64·true°350·SCALAR·INT64·true°51·SCALAR·INT64·true°"
				+ "350·SCALAR·INT64·true°C·MATRIX·FP64").processInstruction(ec);
			Assert.assertTrue(out.getStreamable().getPrimitive() instanceof RepartitionOOCPrimitive);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();

			leftInput.enqueue(tile(1, 2, 200, 200, 1));
			leftInput.enqueue(tile(2, 1, 200, 200, 1));
			leftInput.enqueue(tile(2, 2, 200, 200, 1));
			leftInput.closeInput();
			rightInput.enqueue(tile(1, 1, 150, 150, 9));
			rightInput.enqueue(tile(1, 2, 150, 150, 9));
			rightInput.enqueue(tile(2, 1, 150, 150, 9));
			rightInput.enqueue(tile(2, 2, 150, 150, 9));
			rightInput.closeInput();

			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					IndexedMatrixValue value = current.get();
					MatrixBlock block = (MatrixBlock) value.getValue();
					long rowOffset = (value.getIndexes().getRowIndex() - 1) * 200;
					long colOffset = (value.getIndexes().getColumnIndex() - 1) * 200;
					for(int row = 0; row < block.getNumRows(); row += 49)
						for(int col = 0; col < block.getNumColumns(); col += 49) {
							long globalRow = rowOffset + row;
							long globalCol = colOffset + col;
							double expected = globalRow >= 50 && globalRow < 350 && globalCol >= 50 &&
								globalCol < 350 ? 9 : 1;
							Assert.assertEquals(expected, block.get(row, col), 0);
						}
					blocks++;
				}
			Assert.assertEquals(4, blocks);
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testCSVReblockSpill() throws IOException, InterruptedException {
		Path csv = Files.createTempFile("systemds-ooc-csv-", ".csv");
		String row = "1,".repeat(99) + "1\n";
		Files.writeString(csv, row.repeat(200));
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			MatrixObject csvInput = matrixObject(200, 100, 1000, csv.toString(), FileFormat.CSV);
			MatrixObject csvBlocks = matrixObject(200, 100, 200);
			MatrixObject right = matrixObject(200, 100, 200);
			MatrixObject out = matrixObject(200, 200, 200);
			SubscribableTaskQueue<IndexedMatrixValue> rightInput = new SubscribableTaskQueue<>();
			right.setStreamHandle(rightInput);
			ec.setVariable("X", csvInput);
			ec.setVariable("A", csvBlocks);
			ec.setVariable("B", right);
			ec.setVariable("C", out);

			CSVReblockOOCInstruction.parseInstruction("OOC°csvrblk°X·MATRIX·FP64°A·MATRIX·FP64°200°true")
				.processInstruction(ec);
			Assert.assertTrue(csvBlocks.getStreamable().getPrimitive() instanceof UncoordinatedDataGenOOCPrimitive);
			csvBlocks.getStreamable().getPrimitive().inferPatterns();
			Assert.assertEquals(OOCAccessPattern.UNKNOWN, csvBlocks.getStreamable().getPrimitive().getAccessPattern());
			Assert.assertFalse(csvBlocks.getStreamable().getPrimitive().hasStartedExecution());
			AppendOOCInstruction.parseInstruction("OOC°append°A·MATRIX·FP64°B·MATRIX·FP64°C·MATRIX·FP64°true")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();

			rightInput.enqueue(tile(1, 1, 200, 100, 2));
			rightInput.closeInput();
			try(OOCStream.QueueCallback<IndexedMatrixValue> callback = result.dequeueCB()) {
				MatrixBlock block = (MatrixBlock) callback.get().getValue();
				Assert.assertEquals(1, block.get(0, 0), 0);
				Assert.assertEquals(2, block.get(0, 199), 0);
			}
			Assert.assertNull(result.dequeueCB());
		}
		finally {
			reset(statistics);
			Files.deleteIfExists(csv);
		}
	}

	@Test
	public void testAppendSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			SubscribableTaskQueue<IndexedMatrixValue> leftInput = new SubscribableTaskQueue<>();
			SubscribableTaskQueue<IndexedMatrixValue> rightInput = new SubscribableTaskQueue<>();
			MatrixObject left = matrixObject(200, 100, 200);
			MatrixObject right = matrixObject(200, 100, 100);
			MatrixObject out = matrixObject(200, 200, 200);
			left.setStreamHandle(leftInput);
			right.setStreamHandle(rightInput);
			ec.setVariable("A", left);
			ec.setVariable("B", right);
			ec.setVariable("C", out);
			leftInput.enqueue(tile(1, 1, 200, 100, 1));
			leftInput.closeInput();

			AppendOOCInstruction.parseInstruction("OOC°append°A·MATRIX·FP64°B·MATRIX·FP64°C·MATRIX·FP64°true")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();

			rightInput.enqueue(tile(2, 1, 100, 100, 2));
			rightInput.enqueue(tile(1, 1, 100, 100, 2));
			rightInput.closeInput();
			try(OOCStream.QueueCallback<IndexedMatrixValue> callback = result.dequeueCB()) {
				MatrixBlock block = (MatrixBlock) callback.get().getValue();
				Assert.assertEquals(1, block.get(0, 0), 0);
				Assert.assertEquals(2, block.get(0, 199), 0);
				Assert.assertEquals(2, block.get(199, 199), 0);
			}
			Assert.assertNull(result.dequeueCB());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testTsmmSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			input(ec, "X", 400, 400, 200, 2, 2);
			MatrixObject out = matrixObject(400, 400, 200);
			ec.setVariable("R", out);

			TSMMOOCInstruction.parseInstruction("OOC°tsmm°X·MATRIX·FP64°R·MATRIX·FP64°LEFT").processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = out.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					Assert.assertEquals(400, current.get().getValue().get(0, 0), 0);
					blocks++;
				}
			Assert.assertEquals(4, blocks);
			Assert.assertNull("TSMM initialized the legacy LRU cache", OOCCacheManager.getCacheIfInitialized());
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testRemoveEmptyRowsSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			boolean[] select = mask(800, index -> index % 3 != 0);
			indexedInput(ec, "A", 800, 800, 200);
			selectInput(ec, "S", select, 200);
			MatrixObject output = matrixObject(533, 800, 200);
			ec.setVariable("R", output);
			ParameterizedBuiltinOOCInstruction
				.parseInstruction("OOC°rmempty°target=A°margin=rows°select=S°empty.return=true°R·MATRIX·FP64")
				.processInstruction(ec);
			Assert.assertEquals(533, output.getNumRows());
			Assert.assertEquals(800, output.getNumColumns());
			assertCompacted(output, select, 800, 800, 200, true);
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testRemoveEmptyColsSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			boolean[] select = mask(800, index -> index % 3 != 0);
			indexedInput(ec, "A", 800, 800, 200);
			selectInput(ec, "S", select, 200);
			MatrixObject output = matrixObject(800, 533, 200);
			ec.setVariable("R", output);
			ParameterizedBuiltinOOCInstruction
				.parseInstruction("OOC°rmempty°target=A°margin=cols°select=S°empty.return=true°R·MATRIX·FP64")
				.processInstruction(ec);
			Assert.assertEquals(800, output.getNumRows());
			Assert.assertEquals(533, output.getNumColumns());
			assertCompacted(output, select, 800, 800, 200, false);
		}
		finally {
			reset(statistics);
		}
	}

	@Test
	public void testNaryMinSpill() throws InterruptedException {
		boolean statistics = prepareSpillCache();
		try {
			ExecutionContext ec = new ExecutionContext(new LocalVariableMap());
			input(ec, "A", 800, 800, 200, 4, 4, false, 3);
			input(ec, "B", 800, 800, 200, 4, 4, true, 1);
			input(ec, "C", 800, 800, 200, 4, 4, false, 2);
			MatrixObject output = matrixObject(800, 800, 200);
			ec.setVariable("R", output);
			BuiltinNaryOOCInstruction
				.parseInstruction("OOC°nmin°A·MATRIX·FP64°B·MATRIX·FP64°C·MATRIX·FP64°R·MATRIX·FP64")
				.processInstruction(ec);
			OOCStream<IndexedMatrixValue> result = output.getStreamHandle();
			result.start();
			waitForSpill();
			int blocks = 0;
			OOCStream.QueueCallback<IndexedMatrixValue> callback;
			while((callback = result.dequeueCB()) != null)
				try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
					MatrixBlock block = (MatrixBlock) current.get().getValue();
					Assert.assertEquals(1, block.get(0, 0), 0);
					Assert.assertEquals(1, block.get(199, 199), 0);
					blocks++;
				}
			Assert.assertEquals(16, blocks);
		}
		finally {
			reset(statistics);
		}
	}

	/**
	 * Drains a compacted output and compares it against the same filter applied directly to the indexed input, so the
	 * expectation is independent of the position map used by the instruction.
	 */
	private static void assertCompacted(MatrixObject output, boolean[] select, int rows, int cols, int blocksize,
		boolean marginRows) throws InterruptedException {
		OOCStream<IndexedMatrixValue> result = output.getStreamHandle();
		result.start();
		waitForSpill();
		long[] positions = new long[select.length];
		long kept = 0;
		for(int index = 0; index < select.length; index++)
			positions[index] = select[index] ? kept++ : -1;
		double[][] expected = new double[marginRows ? (int) kept : rows][marginRows ? cols : (int) kept];
		for(int row = 0; row < rows; row++)
			for(int col = 0; col < cols; col++) {
				long target = marginRows ? positions[row] : positions[col];
				if(target >= 0)
					expected[marginRows ? (int) target : row][marginRows ? col : (int) target] = indexedValue(row, col);
			}

		int seen = 0;
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = result.dequeueCB()) != null)
			try(OOCStream.QueueCallback<IndexedMatrixValue> current = callback) {
				IndexedMatrixValue value = current.get();
				MatrixBlock block = (MatrixBlock) value.getValue();
				int rowOffset = (int) (value.getIndexes().getRowIndex() - 1) * blocksize;
				int colOffset = (int) (value.getIndexes().getColumnIndex() - 1) * blocksize;
				for(int row = 0; row < block.getNumRows(); row++)
					for(int col = 0; col < block.getNumColumns(); col++)
						Assert.assertEquals("block " + value.getIndexes() + " cell [" + row + "," + col + "]",
							expected[rowOffset + row][colOffset + col], block.get(row, col), 0);
				seen += block.getNumRows() * block.getNumColumns();
			}
		Assert.assertEquals(expected.length * expected[0].length, seen);
	}

	private static boolean[] mask(int length, IntPredicate selected) {
		boolean[] select = new boolean[length];
		for(int index = 0; index < length; index++)
			select[index] = selected.test(index);
		return select;
	}

	/** Emits a select vector as a column vector stream, one block per {@code blocksize} entries. */
	private static void selectInput(ExecutionContext ec, String name, boolean[] select, int blocksize) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixObject matrix = matrixObject(select.length, 1, blocksize);
		matrix.setStreamHandle(stream);
		ec.setVariable(name, matrix);
		for(int offset = 0; offset < select.length; offset += blocksize) {
			int length = Math.min(blocksize, select.length - offset);
			MatrixBlock block = new MatrixBlock(length, 1, false);
			for(int index = 0; index < length; index++)
				if(select[offset + index])
					block.set(index, 0, 1);
			stream.enqueue(new IndexedMatrixValue(new MatrixIndexes(offset / blocksize + 1, 1), block));
		}
		stream.closeInput();
	}

	/** Input whose cells carry their global position, so a compaction can be verified cell by cell. */
	private static void indexedInput(ExecutionContext ec, String name, int rows, int cols, int blocksize) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixObject matrix = matrixObject(rows, cols, blocksize);
		matrix.setStreamHandle(stream);
		ec.setVariable(name, matrix);
		for(int rowOffset = 0; rowOffset < rows; rowOffset += blocksize)
			for(int colOffset = 0; colOffset < cols; colOffset += blocksize) {
				int blockRows = Math.min(blocksize, rows - rowOffset);
				int blockCols = Math.min(blocksize, cols - colOffset);
				MatrixBlock block = new MatrixBlock(blockRows, blockCols, false);
				block.allocateDenseBlock();
				for(int row = 0; row < blockRows; row++)
					for(int col = 0; col < blockCols; col++)
						block.set(row, col, indexedValue(rowOffset + row, colOffset + col));
				stream.enqueue(new IndexedMatrixValue(
					new MatrixIndexes(rowOffset / blocksize + 1, colOffset / blocksize + 1), block));
			}
		stream.closeInput();
	}

	private static double indexedValue(int row, int col) {
		return row * 1000d + col + 1;
	}

	private static MatrixObject input(ExecutionContext ec, String name, int rows, int cols, int blocksize,
		int rowBlocks, int colBlocks) {
		return input(ec, name, rows, cols, blocksize, rowBlocks, colBlocks, false, 1);
	}

	private static MatrixObject input(ExecutionContext ec, String name, int rows, int cols, int blocksize,
		int rowBlocks, int colBlocks, boolean reverse, double value) {
		SubscribableTaskQueue<IndexedMatrixValue> stream = new SubscribableTaskQueue<>();
		MatrixObject matrix = matrixObject(rows, cols, blocksize);
		matrix.setStreamHandle(stream);
		ec.setVariable(name, matrix);
		int blocks = rowBlocks * colBlocks;
		for(int index = 0; index < blocks; index++) {
			int position = reverse ? blocks - index - 1 : index;
			int row = position / colBlocks + 1;
			int col = position % colBlocks + 1;
			stream.enqueue(tile(row, col, Math.min(blocksize, rows - (row - 1) * blocksize),
				Math.min(blocksize, cols - (col - 1) * blocksize), value));
		}
		stream.closeInput();
		return matrix;
	}

	private static boolean prepareSpillCache() {
		OOCCacheManager.reset();
		boolean statistics = DMLScript.OOC_STATISTICS;
		DMLScript.OOC_STATISTICS = true;
		Statistics.resetOOCEvictionStats();
		OOCCacheManager.getGlobalCache().updateLimits(2_000_000, 100_000);
		return statistics;
	}

	private static void waitForSpill() throws InterruptedException {
		for(int attempt = 0; attempt < 100 && Statistics.getOOCEvictionWriteCount() == 0; attempt++)
			Thread.sleep(10);
		Assert.assertTrue("Expected instruction state to spill", Statistics.getOOCEvictionWriteCount() > 0);
	}

	private static void drain(OOCStream<IndexedMatrixValue> stream) {
		stream.start();
		drainStarted(stream);
	}

	private static void drainStarted(OOCStream<IndexedMatrixValue> stream) {
		OOCStream.QueueCallback<IndexedMatrixValue> callback;
		while((callback = stream.dequeueCB()) != null)
			callback.close();
	}

	private static IndexedMatrixValue tile(long row, long col, int rows, int cols, double value) {
		return new IndexedMatrixValue(new MatrixIndexes(row, col), new MatrixBlock(rows, cols, value));
	}

	private static MatrixObject matrixObject(long rows, long cols, int blocksize) {
		return matrixObject(rows, cols, blocksize, "/dev/null", FileFormat.BINARY);
	}

	private static MatrixObject matrixObject(long rows, long cols, int blocksize, String fileName, FileFormat format) {
		return new MatrixObject(ValueType.FP64, fileName,
			new MetaDataFormat(new MatrixCharacteristics(rows, cols, blocksize, rows * cols), format));
	}

	private static void reset(boolean statistics) {
		OOCCacheManager.reset();
		DMLScript.OOC_STATISTICS = statistics;
	}
}
