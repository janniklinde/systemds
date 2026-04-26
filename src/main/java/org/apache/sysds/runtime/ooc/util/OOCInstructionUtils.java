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

package org.apache.sysds.runtime.ooc.util;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.primitives.GroupedReduceOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.JoinOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.MappingOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.PlannableDataGenOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.TransposeOOCPrimitive;
import org.apache.sysds.runtime.ooc.stats.OOCEventLog;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.stream.TaskContext;
import org.apache.sysds.runtime.util.CommonThreadPool;
import org.apache.sysds.utils.Statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class OOCInstructionUtils {
	public static final ExecutorService COMPUTE_EXECUTOR = CommonThreadPool.get();
	public static final AtomicInteger COMPUTE_IN_FLIGHT = new AtomicInteger(0);
	public static final AtomicInteger NEXT_STREAM_ID = new AtomicInteger(0);

	public static void dataGen(OOCStream<IndexedMatrixValue> out, Function<MatrixIndexes, MatrixBlock> fn, StreamContext sc) {
		OOCPrimitive primitive = new PlannableDataGenOOCPrimitive(out, fn, sc);
		out.assignPrimitive(primitive);
	}

	public static void equiMap(OOCStream<IndexedMatrixValue> in, OOCStream<IndexedMatrixValue> out, Function<MatrixBlock, MatrixBlock> fn, StreamContext sc) {
		OOCPrimitive primitive = new MappingOOCPrimitive(in, out, fn, sc);
		out.assignPrimitive(primitive);
	}

	public static void transposedMap(OOCStream<IndexedMatrixValue> in, OOCStream<IndexedMatrixValue> out,
		Function<MatrixBlock, MatrixBlock> fn, StreamContext sc) {
		OOCPrimitive primitive = new TransposeOOCPrimitive(in, out, fn, sc);
		out.assignPrimitive(primitive);
	}

	public static void transpose(OOCStream<IndexedMatrixValue> in, OOCStream<IndexedMatrixValue> out, StreamContext sc) {
		transposedMap(in, out, MatrixBlock::transpose, sc);
	}

	public static void groupedReduce(OOCStream<IndexedMatrixValue> in, OOCStream<IndexedMatrixValue> out,
		GroupedReduceOOCPrimitive.Grouping grouping, int accumulatorsPerGroup,
		Function<MatrixBlock, MatrixBlock> partialFn, BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn,
		Function<MatrixBlock, MatrixBlock> finalizeFn, StreamContext sc) {
		OOCPrimitive primitive = new GroupedReduceOOCPrimitive(in, out, grouping, accumulatorsPerGroup, partialFn,
			mergeFn, finalizeFn, sc);
		out.assignPrimitive(primitive);
	}

	public static void groupedReduce(OOCStream<IndexedMatrixValue> in, OOCStream<IndexedMatrixValue> out,
		GroupedReduceOOCPrimitive.Grouping grouping, int accumulatorsPerGroup,
		Function<MatrixBlock, MatrixBlock> partialFn, BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn,
		StreamContext sc) {
		groupedReduce(in, out, grouping, accumulatorsPerGroup, partialFn, mergeFn, Function.identity(), sc);
	}

	public static void rowGroupedReduce(OOCStream<IndexedMatrixValue> in, OOCStream<IndexedMatrixValue> out,
		int accumulatorsPerGroup, Function<MatrixBlock, MatrixBlock> partialFn,
		BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn, Function<MatrixBlock, MatrixBlock> finalizeFn,
		StreamContext sc) {
		groupedReduce(in, out, GroupedReduceOOCPrimitive.Grouping.ROW_BLOCKS, accumulatorsPerGroup, partialFn,
			mergeFn, finalizeFn, sc);
	}

	public static void colGroupedReduce(OOCStream<IndexedMatrixValue> in, OOCStream<IndexedMatrixValue> out,
		int accumulatorsPerGroup, Function<MatrixBlock, MatrixBlock> partialFn,
		BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn, Function<MatrixBlock, MatrixBlock> finalizeFn,
		StreamContext sc) {
		groupedReduce(in, out, GroupedReduceOOCPrimitive.Grouping.COL_BLOCKS, accumulatorsPerGroup, partialFn,
			mergeFn, finalizeFn, sc);
	}

	public static void singleGroupedReduce(OOCStream<IndexedMatrixValue> in, OOCStream<IndexedMatrixValue> out,
		int accumulatorsPerGroup, Function<MatrixBlock, MatrixBlock> partialFn,
		BiFunction<MatrixBlock, MatrixBlock, MatrixBlock> mergeFn, Function<MatrixBlock, MatrixBlock> finalizeFn,
		StreamContext sc) {
		groupedReduce(in, out, GroupedReduceOOCPrimitive.Grouping.SINGLE, accumulatorsPerGroup, partialFn, mergeFn,
			finalizeFn, sc);
	}

	public static void equiJoin(List<OOCStreamable<IndexedMatrixValue>> l, OOCStream<IndexedMatrixValue> out, Function<List<MatrixBlock>, MatrixBlock> fn, StreamContext sc) {
		OOCPrimitive primitive = new JoinOOCPrimitive(l, out, fn, sc);
		out.assignPrimitive(primitive);
	}

	public static <T> CompletableFuture<Void> submitOOCTasks(final List<OOCStream<T>> queues,
		BiConsumer<Integer, OOCStream.QueueCallback<T>> consumer, StreamContext sc) {
		return submitOOCTasks(queues, consumer, null, null, sc);
	}

	public static <T> CompletableFuture<Void> submitOOCTasks(OOCStream<T> queue,
		Consumer<OOCStream.QueueCallback<T>> consumer, StreamContext sc) {
		return OOCInstructionUtils.submitOOCTasks(List.of(queue), (i, tmp) -> consumer.accept(tmp), null, null, sc);
	}

	public static <T> CompletableFuture<Void> submitOOCTasks(OOCStream<T> queue,
		Consumer<OOCStream.QueueCallback<T>> consumer, Function<OOCStream.QueueCallback<T>, Boolean> predicate,
		BiConsumer<Integer, OOCStream.QueueCallback<T>> onNotProcessed, StreamContext sc) {
		return submitOOCTasks(List.of(queue), (i, tmp) -> consumer.accept(tmp), (i, tmp) -> predicate.apply(tmp),
			onNotProcessed, sc);
	}

	public static <T> CompletableFuture<Void> submitOOCTasks(final List<OOCStream<T>> queues,
		BiConsumer<Integer, OOCStream.QueueCallback<T>> consumer,
		BiFunction<Integer, OOCStream.QueueCallback<T>, Boolean> predicate,
		BiConsumer<Integer, OOCStream.QueueCallback<T>> onNotProcessed, StreamContext sc) {
		sc.addInStream(queues.toArray(OOCStream[]::new));
		if(!sc.outStreamsDefined())
			throw new IllegalArgumentException(
				"Explicit specification of all output streams is required before submitting tasks. If no output streams are present use addOutStream().");

		final List<AtomicInteger> activeTaskCtrs = new ArrayList<>(queues.size());
		final List<CompletableFuture<Void>> futures = new ArrayList<>(queues.size());

		for(int i = 0; i < queues.size(); i++) {
			activeTaskCtrs.add(new AtomicInteger(1));
			futures.add(new CompletableFuture<>());
		}

		final CompletableFuture<Void> globalFuture = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
		final StreamContext streamContext = sc.copy(); // Snapshot of the current stream context
		if(streamContext == null || !streamContext.inStreamsDefined() || !streamContext.outStreamsDefined())
			throw new IllegalArgumentException(
				"Explicit specification of all output streams is required before submitting tasks. If no output streams are present use addOutStream().");

		int i = 0;
		@SuppressWarnings("unused")
		final int streamId = NEXT_STREAM_ID.getAndIncrement();

		for(OOCStream<T> queue : queues) {
			final int k = i;
			final AtomicInteger localTaskCtr = activeTaskCtrs.get(k);
			final CompletableFuture<Void> localFuture = futures.get(k);
			final AtomicBoolean closeRaceWatchdog = new AtomicBoolean(false);

			queue.setSubscriber(oocTask(callback -> {
				long startTime = DMLScript.STATISTICS ? System.nanoTime() : 0;
				try(callback) {
					if(callback.isEos()) {
						if(!closeRaceWatchdog.compareAndSet(false, true))
							throw new DMLRuntimeException(
								"Race condition observed: NO_MORE_TASKS callback has been triggered more than once");

						if(localTaskCtr.decrementAndGet() == 0) {
							// Then we can run the finalization procedure already
							localFuture.complete(null);
						}
						return;
					}

					Consumer<OOCStream.QueueCallback<T>> process = cb -> {
						if(predicate != null && !predicate.apply(k, cb)) { // Can get closed due to cancellation
							if(onNotProcessed != null)
								onNotProcessed.accept(k, cb);
							return;
						}

						if(localFuture.isDone()) {
							if(onNotProcessed != null)
								onNotProcessed.accept(k, cb);
							return;
						}
						else {
							localTaskCtr.incrementAndGet();
						}

						// The item needs to be pinned in memory to be accessible in the executor thread
						final OOCStream.QueueCallback<T> pinned = cb.keepOpen();

						COMPUTE_IN_FLIGHT.incrementAndGet();
						try {
							Runnable oocTask = oocTask(() -> {
								long taskStartTime =
									DMLScript.STATISTICS || DMLScript.OOC_LOG_EVENTS ? System.nanoTime() : 0;
								try(pinned) {
									consumer.accept(k, pinned);

									if(localTaskCtr.decrementAndGet() == 0) {
										TaskContext.defer(() -> localFuture.complete(null));
									}
								}
								finally {
									COMPUTE_IN_FLIGHT.decrementAndGet();
									if(DMLScript.STATISTICS) {
										sc.getLocalStatisticsLongAdder().add(System.nanoTime() - taskStartTime);
										if(globalFuture.isDone()) {
											Statistics.maintainOOCHeavyHitter(sc.getExtendedOpcode(),
												sc.getLocalStatisticsLongAdder().sum());
											sc.getLocalStatisticsLongAdder().reset();
										}
									}
									if(DMLScript.OOC_LOG_EVENTS)
										OOCEventLog.onComputeEvent(sc.getCallerId(), taskStartTime, System.nanoTime());
								}
							}, localFuture, streamContext);
							COMPUTE_EXECUTOR.submit(oocTask);
						}
						catch(Exception e) {
							COMPUTE_IN_FLIGHT.decrementAndGet();
							throw e;
						}
					};

					if(callback instanceof OOCStream.GroupQueueCallback<?>) {
						OOCStream.GroupQueueCallback<T> group = (OOCStream.GroupQueueCallback<T>) callback;

						if(localFuture.isDone()) {
							for(int idx = 0; idx < group.size(); idx++) {
								OOCStream.QueueCallback<T> sub = group.getCallback(idx);
								try(sub) {
									if(onNotProcessed != null)
										onNotProcessed.accept(k, sub);
								}
							}
							return;
						}

						localTaskCtr.incrementAndGet();
						final OOCStream.GroupQueueCallback<T> pinnedGroup = (OOCStream.GroupQueueCallback<T>) group.keepOpen();

						COMPUTE_IN_FLIGHT.incrementAndGet();
						try {
							Runnable oocTask = oocTask(() -> {
								long taskStartTime =
									DMLScript.STATISTICS || DMLScript.OOC_LOG_EVENTS ? System.nanoTime() : 0;
								try(pinnedGroup) {
									for(int idx = 0; idx < pinnedGroup.size(); idx++) {
										OOCStream.QueueCallback<T> sub = pinnedGroup.getCallback(idx);
										try(sub) {
											process.accept(sub);
										}
									}

									if(localTaskCtr.decrementAndGet() == 0) {
										TaskContext.defer(() -> localFuture.complete(null));
									}
								}
								finally {
									COMPUTE_IN_FLIGHT.decrementAndGet();
									if(DMLScript.STATISTICS) {
										sc.getLocalStatisticsLongAdder().add(System.nanoTime() - taskStartTime);
										if(globalFuture.isDone()) {
											Statistics.maintainOOCHeavyHitter(sc.getExtendedOpcode(),
												sc.getLocalStatisticsLongAdder().sum());
											sc.getLocalStatisticsLongAdder().reset();
										}
									}
									if(DMLScript.OOC_LOG_EVENTS)
										OOCEventLog.onComputeEvent(sc.getCallerId(), taskStartTime, System.nanoTime());
								}
							}, localFuture, streamContext);
							COMPUTE_EXECUTOR.submit(oocTask);
						}
						catch(Exception e) {
							COMPUTE_IN_FLIGHT.decrementAndGet();
							throw e;
						}
					}
					else {
						process.accept(callback);
					}

					if(closeRaceWatchdog.get()) // Sanity check
						throw new DMLRuntimeException("Race condition observed");
				}
				catch(Throwable t) {
					streamContext.failAll(DMLRuntimeException.of(t));
					throw t;
				}
				finally {
					if(DMLScript.STATISTICS) {
						sc.getLocalStatisticsLongAdder().add(System.nanoTime() - startTime);
						if(globalFuture.isDone()) {
							Statistics.maintainOOCHeavyHitter(sc.getExtendedOpcode(),
								sc.getLocalStatisticsLongAdder().sum());
							sc.getLocalStatisticsLongAdder().reset();
						}
					}
				}
			}, null, streamContext));

			i++;
		}

		return globalFuture.handle((res, e) -> {
			if(globalFuture.isCancelled() || globalFuture.isCompletedExceptionally()) {
				futures.forEach(f -> {
					if(!f.isDone()) {
						if(globalFuture.isCancelled() || globalFuture.isCompletedExceptionally())
							f.cancel(true);
						else
							f.complete(null);
					}
				});
			}

			streamContext.clear();
			return null;
		});
	}

	public static CompletableFuture<Void> submitOOCTask(Runnable r, StreamContext sc) {
		ExecutorService pool = CommonThreadPool.get();
		final CompletableFuture<Void> future = new CompletableFuture<>();
		try {
			COMPUTE_IN_FLIGHT.incrementAndGet();
			pool.submit(oocTask(() -> {
				long startTime = DMLScript.STATISTICS || DMLScript.OOC_LOG_EVENTS ? System.nanoTime() : 0;
				try {
					r.run();
					future.complete(null);
					sc.clear();
					if (DMLScript.STATISTICS)
						Statistics.maintainOOCHeavyHitter(sc.getExtendedOpcode(), System.nanoTime() - startTime);
					if (DMLScript.OOC_LOG_EVENTS)
						OOCEventLog.onComputeEvent(sc.getCallerId(), startTime,  System.nanoTime());
				}
				finally {
					COMPUTE_IN_FLIGHT.decrementAndGet();
				}
			}, future, sc));
		}
		catch (Exception ex) {
			COMPUTE_IN_FLIGHT.decrementAndGet();
			throw new DMLRuntimeException(ex);
		}
		finally {
			pool.shutdown();
		}

		return future;
	}

	private static Runnable oocTask(Runnable r, CompletableFuture<Void> future, StreamContext ctx) {
		return () -> {
			boolean setContext = TaskContext.getContext() == null;
			if(setContext)
				TaskContext.setContext(new TaskContext());
			long startTime = DMLScript.STATISTICS ? System.nanoTime() : 0;
			try {
				r.run();
				if(setContext) {
					while(TaskContext.runDeferred()) {
					}
				}
			}
			catch(Exception ex) {
				DMLRuntimeException re = DMLRuntimeException.of(ex);

				ctx.failAll(re);

				if(future != null)
					future.completeExceptionally(re);

				// Rethrow to ensure proper future handling
				throw re;
			}
			finally {
				if(setContext)
					TaskContext.clearContext();
				if(DMLScript.STATISTICS)
					ctx.getLocalStatisticsLongAdder().add(System.nanoTime() - startTime);
			}
		};
	}

	private static <T> Consumer<OOCStream.QueueCallback<T>> oocTask(Consumer<OOCStream.QueueCallback<T>> c,
		CompletableFuture<Void> future, StreamContext ctx) {
		return callback -> {
			try {
				c.accept(callback);
			}
			catch(Exception ex) {
				DMLRuntimeException re = DMLRuntimeException.of(ex);

				ctx.failAll(re);

				if(future != null)
					future.completeExceptionally(re);

				// Rethrow to ensure proper future handling
				throw re;
			}
		};
	}
}
