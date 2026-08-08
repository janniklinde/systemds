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

package org.apache.sysds.runtime.ooc.primitives;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.ooc.cache.io.SpillableObject;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;
import org.apache.sysds.runtime.ooc.util.OOCUtils;

public final class UncoordinatedDataGenOOCPrimitive<T extends SpillableObject> extends OOCPrimitive {
	private final OOCStreamable<T> _output;
	private final long _bulkBytes;
	private final long _productionLimit;
	private final long _maxBulkBytes;
	private final LongSupplier _bulkBytesSupplier;
	private final OOCAccessPattern _emissionPattern;
	private final BiFunction<Long, Consumer<T>, Boolean> _producer;
	private final Runnable _cleanup;
	private final long _outputBulkBytes;
	private final LongSupplier _outputBulkBytesSupplier;
	private final Predicate<T> _startsOutputBulk;
	private final Predicate<T> _endsOutputBulk;
	private final Predicate<T> _refillsOutputBulk;
	private final Queue<ReservationBudget> _outputBulks = new ConcurrentLinkedQueue<>();
	private final ConcurrentHashMap<Thread, ReservationBudget> _activeOutputBulks = new ConcurrentHashMap<>();
	private final AtomicBoolean _finished = new AtomicBoolean();

	public UncoordinatedDataGenOOCPrimitive(OOCStreamable<T> output, long bulkBytes, long productionLimit,
		OOCAccessPattern emissionPattern, BiFunction<Long, Consumer<T>, Boolean> producer, Runnable cleanup,
		StreamContext context) {
		this(output, bulkBytes, productionLimit, emissionPattern, producer, cleanup, 0, null, null, null, context);
	}

	public UncoordinatedDataGenOOCPrimitive(OOCStreamable<T> output, long bulkBytes, long productionLimit,
		OOCAccessPattern emissionPattern, BiFunction<Long, Consumer<T>, Boolean> producer, Runnable cleanup,
		long outputBulkBytes, Predicate<T> startsOutputBulk, Predicate<T> endsOutputBulk,
		Predicate<T> refillsOutputBulk, StreamContext context) {
		super(context);
		if(bulkBytes <= 0 || productionLimit <= 0 || productionLimit > bulkBytes)
			throw new IllegalArgumentException("Invalid bulk allocation: " + productionLimit + "/" + bulkBytes);
		if(outputBulkBytes < 0 || outputBulkBytes > 0 && (bulkBytes % outputBulkBytes != 0 ||
			startsOutputBulk == null || endsOutputBulk == null || refillsOutputBulk == null))
			throw new IllegalArgumentException("Invalid output bulk allocation: " + outputBulkBytes);
		_output = output;
		_bulkBytes = bulkBytes;
		_productionLimit = productionLimit;
		_maxBulkBytes = bulkBytes;
		_bulkBytesSupplier = null;
		_emissionPattern = emissionPattern;
		_producer = producer;
		_cleanup = cleanup;
		_outputBulkBytes = outputBulkBytes;
		_outputBulkBytesSupplier = null;
		_startsOutputBulk = startsOutputBulk;
		_endsOutputBulk = endsOutputBulk;
		_refillsOutputBulk = refillsOutputBulk;
	}

	public UncoordinatedDataGenOOCPrimitive(OOCStreamable<T> output, LongSupplier bulkBytes, long maxBulkBytes,
		OOCAccessPattern emissionPattern, BiFunction<Long, Consumer<T>, Boolean> producer, Runnable cleanup,
		LongSupplier outputBulkBytes, Predicate<T> startsOutputBulk, Predicate<T> endsOutputBulk,
		Predicate<T> refillsOutputBulk, StreamContext context) {
		super(context);
		if(maxBulkBytes <= 0 || bulkBytes == null || outputBulkBytes == null || startsOutputBulk == null ||
			endsOutputBulk == null || refillsOutputBulk == null)
			throw new IllegalArgumentException("Dynamic bulk allocation requires bulk suppliers and boundaries");
		_output = output;
		_bulkBytes = 0;
		_productionLimit = 0;
		_maxBulkBytes = maxBulkBytes;
		_bulkBytesSupplier = bulkBytes;
		_emissionPattern = emissionPattern;
		_producer = producer;
		_cleanup = cleanup;
		_outputBulkBytes = 0;
		_outputBulkBytesSupplier = outputBulkBytes;
		_startsOutputBulk = startsOutputBulk;
		_endsOutputBulk = endsOutputBulk;
		_refillsOutputBulk = refillsOutputBulk;
	}

	@Override
	protected long getAllowanceLimit(GlobalMemoryBroker broker) {
		return Math.min(super.getAllowanceLimit(broker), 3 * _maxBulkBytes);
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = _emissionPattern;
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = _emissionPattern;
	}

	@Override
	protected void startExecution() {
		OOCStream<T> output = _output.getWriteStream();
		getContext().addOutStream(output);
		produceNext(output);
	}

	private void produceNext(OOCStream<T> output) {
		long bulkBytes = _bulkBytesSupplier != null ? _bulkBytesSupplier.getAsLong() : _bulkBytes;
		long productionLimit = _bulkBytesSupplier != null ? bulkBytes : _productionLimit;
		long outputBulkBytes = _outputBulkBytesSupplier != null ? _outputBulkBytesSupplier
			.getAsLong() : _outputBulkBytes;
		if(bulkBytes <= 0 || bulkBytes > _maxBulkBytes || outputBulkBytes < 0 ||
			outputBulkBytes > 0 && bulkBytes % outputBulkBytes != 0) {
			failAndFinish(
				new IllegalArgumentException("Invalid dynamic bulk allocation: " + outputBulkBytes + "/" + bulkBytes));
			return;
		}
		_allowance.reserveAsync(bulkBytes).whenComplete((ignored, admissionError) -> {
			if(admissionError != null) {
				failAndFinish(admissionError);
				return;
			}
			ReservationBudget phase = new ReservationBudget(_allowance, bulkBytes);
			if(outputBulkBytes > 0) {
				for(long bytes = 0; bytes < bulkBytes; bytes += outputBulkBytes) {
					phase.reserveBlocking(outputBulkBytes);
					_outputBulks.add(new ReservationBudget(phase, outputBulkBytes));
				}
			}
			OOCInstructionUtils.submitOOCTask(() -> {
				boolean done = _producer.apply(productionLimit, value -> emit(output, value, phase, outputBulkBytes));
				if(done)
					finish(output);
			}, new StreamContext(getContext().getCallerId(), getContext().getExtendedOpcode()))
				.whenComplete((ignoredTask, producerError) -> {
					phase.close();
					if(producerError != null)
						failAndFinish(producerError);
					else if(!_finished.get())
						produceNext(output);
				});
		});
	}

	private void emit(OOCStream<T> output, T value, ReservationBudget phase, long outputBulkBytes) {
		if(outputBulkBytes == 0) {
			enqueue(output, value, phase, value.size());
			return;
		}

		Thread worker = Thread.currentThread();
		if(_startsOutputBulk.test(value)) {
			ReservationBudget bulk = _outputBulks.poll();
			if(bulk == null)
				throw new IllegalStateException("No admitted output bulk available for worker");
			if(_activeOutputBulks.put(worker, bulk) != null) {
				bulk.close();
				throw new IllegalStateException("Worker started an output bulk before completing the previous bulk");
			}
		}
		ReservationBudget bulk = _activeOutputBulks.get(worker);
		if(bulk == null)
			throw new IllegalStateException("Output emitted outside a worker bulk");
		enqueue(output, value, bulk, value.size());
		if(_endsOutputBulk.test(value)) {
			_activeOutputBulks.remove(worker).close();
			if(_refillsOutputBulk.test(value)) {
				_allowance.reserveBlocking(outputBulkBytes);
				_outputBulks.add(new ReservationBudget(_allowance, outputBulkBytes));
			}
		}
	}

	private static <T extends SpillableObject> void enqueue(OOCStream<T> output, T value, ReservationBudget budget,
		long bytes) {
		budget.reserveBlocking(bytes);
		ReservationBudget tile = new ReservationBudget(budget, bytes);
		try {
			OOCUtils.enqueueExact(output, value, tile);
		}
		catch(Throwable error) {
			tile.close();
			throw error;
		}
	}

	private void finish(OOCStream<T> output) {
		if(!_finished.compareAndSet(false, true))
			return;
		try {
			output.closeInput();
		}
		catch(Throwable error) {
			fail(error);
		}
		finally {
			closeOutputBulks();
			_cleanup.run();
			onComplete();
		}
	}

	private void failAndFinish(Throwable error) {
		if(!_finished.compareAndSet(false, true))
			return;
		try {
			fail(error);
		}
		finally {
			closeOutputBulks();
			_cleanup.run();
			onComplete();
		}
	}

	private void closeOutputBulks() {
		_activeOutputBulks.values().forEach(ReservationBudget::close);
		_activeOutputBulks.clear();
		ReservationBudget bulk;
		while((bulk = _outputBulks.poll()) != null)
			bulk.close();
	}
}
