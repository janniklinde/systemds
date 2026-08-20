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

import java.util.Collection;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.ooc.memory.InMemoryQueueCallback;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.stream.AllocatedOOCStream;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCInstructionUtils;

public final class FlatMapOOCPrimitive<I, O> extends OOCPrimitive {
	private final OOCStreamable<O> _output;
	private final Function<I, Collection<O>> _operation;
	private final ToLongFunction<O> _outputSize;
	private final long _taskBytes;

	public FlatMapOOCPrimitive(OOCStreamable<I> input, OOCStreamable<O> output, Function<I, Collection<O>> operation,
		ToLongFunction<O> outputSize, long taskBytes, StreamContext context) {
		super(context, input);
		_output = output;
		_operation = operation;
		_outputSize = outputSize;
		_taskBytes = taskBytes;
	}

	@Override
	protected void inferPatternsInternal() {
		_pattern = OOCAccessPattern.ANY;
		inferParentPatterns();
	}

	@Override
	protected void requestPatternInternal(OOCAccessPattern accessPattern) {
		_pattern = OOCAccessPattern.ANY;
	}

	@Override
	protected void startExecution() {
		OOCStream<I> input = getInputReadStream(0);
		OOCStream<O> output = _output.getWriteStream();
		AllocatedOOCStream<I> admitted = new AllocatedOOCStream<>(input, _allowance, ignored -> _taskBytes, true);
		getContext().addOutStream(output);
		OOCInstructionUtils.submitOOCTasks(admitted, callback -> {
			ReservationBudget budget = AllocatedOOCStream.detachBudget(callback).enableReuse();
			try {
				for(O value : _operation.apply(callback.get())) {
					long bytes = _outputSize.applyAsLong(value);
					budget.reserveBlocking(bytes);
					OOCStream.QueueCallback<O> result = new InMemoryQueueCallback<>(value, null, budget, bytes);
					try {
						output.enqueue(result);
						result = null;
					}
					finally {
						if(result != null)
							result.close();
					}
				}
			}
			finally {
				budget.close();
			}
		}, getContext()).whenComplete((ignored, error) -> {
			try {
				if(error != null)
					fail(error);
				output.closeInput();
			}
			finally {
				onComplete();
			}
		});
	}
}
