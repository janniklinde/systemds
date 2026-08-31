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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongBinaryOperator;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.OOCWatchdog;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.ReservationBudget;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCPlanner;
import org.apache.sysds.runtime.ooc.planning.OOCStoreLayout;
import org.apache.sysds.runtime.ooc.planning.OOCTileOperation;
import org.apache.sysds.runtime.ooc.store.MaterializedStore;
import org.apache.sysds.runtime.ooc.stream.StreamContext;
import org.apache.sysds.runtime.ooc.util.OOCUtils;
import org.apache.sysds.utils.stats.InfrastructureAnalyzer;

public abstract class OOCPrimitive {
	private final StreamContext _context;
	private final Set<OOCPrimitive> _children;
	private final Set<OOCPrimitive> _parents;
	private final List<InputSlot> _inputs;
	private final AtomicBoolean _started;
	private final AtomicBoolean _executionStarted;
	private boolean _subtreeStarted;
	private final AtomicBoolean _failed;
	private volatile Throwable _failure;
	protected OOCAccessPattern _pattern;
	protected MemoryAllowance _allowance;
	private OOCTileOperation _tileOperation;
	private final long _planEpoch;

	protected OOCPrimitive(StreamContext context, List<OOCPrimitive> children) {
		this(context);
		children.stream().filter(Objects::nonNull).forEach(child -> {
			_children.add(child);
			child._parents.add(this);
		});
	}

	protected OOCPrimitive(StreamContext context, OOCStreamable<?>... inputs) {
		this(context);
		for(OOCStreamable<?> input : inputs)
			_inputs.add(new InputSlot(input));
		rebuildInputChildren();
	}

	protected OOCPrimitive(StreamContext context) {
		_context = context;
		_children = ConcurrentHashMap.newKeySet();
		_parents = ConcurrentHashMap.newKeySet();
		_inputs = new ArrayList<>();
		_started = new AtomicBoolean();
		_executionStarted = new AtomicBoolean();
		_failed = new AtomicBoolean();
		_pattern = OOCAccessPattern.UNSET;
		_planEpoch = OOCPlanner.currentEpoch();
		if(OOCWatchdog.WATCH_PRIMITIVES)
			OOCWatchdog.registerPrimitive(this);
	}

	public final StreamContext getContext() {
		return _context;
	}

	public final Set<OOCPrimitive> getChildren() {
		return _children;
	}

	public final Set<OOCPrimitive> getParents() {
		return _parents;
	}

	protected final void inferParentPatterns() {
		for(OOCPrimitive parent : _parents)
			if(parent._pattern.isUnset())
				parent.inferPatterns();
	}

	public final OOCAccessPattern getAccessPattern() {
		return _pattern;
	}

	public final OOCTileOperation getTileOperation() {
		return _tileOperation;
	}

	public final void setTileOperation(OOCTileOperation operation) {
		if(_tileOperation != null)
			throw new IllegalStateException("Tile operation already assigned");
		if(Objects.requireNonNull(operation).getNumInputs() != _inputs.size())
			throw new IllegalArgumentException("Tile operation input count does not match primitive inputs");
		_tileOperation = operation;
	}

	public final boolean hasStartedExecution() {
		return _executionStarted.get();
	}

	public final boolean isSubtreeStarted() {
		return _subtreeStarted;
	}

	public final void markSubtreeStarted() {
		_subtreeStarted = true;
	}

	public List<OOCMaterializedInputRequest> requiredMaterializedInputs() {
		return List.of();
	}

	public List<Integer> dimensionCriticalInputs() {
		return List.of();
	}

	public final OOCStreamable<?> getInput(int index) {
		return _inputs.get(index)._source;
	}

	public final int getNumInputs() {
		return _inputs.size();
	}

	public final long getPlanEpoch() {
		return _planEpoch;
	}

	public final OOCPrimitive getInputDependency(int index) {
		return _inputs.get(index)._dependency;
	}

	public final void installMaterializedInput(int index, MaterializeOOCPrimitive boundary) {
		if(hasStartedExecution())
			throw new IllegalStateException("Cannot replace an input after primitive execution started.");
		InputSlot input = _inputs.get(index);
		input._dependency = boundary;
		rebuildInputChildren();
	}

	public final void refreshInputDependencies() {
		if(hasStartedExecution())
			return;
		for(InputSlot input : _inputs)
			input._dependency = input._source.getPrimitive();
		rebuildInputChildren();
	}

	public final void discardInputHandles() {
		for(int i = 0; i < _inputs.size(); i++)
			discardInputHandle(i);
	}

	private synchronized void consumeInputHandle(int index) {
		InputSlot input = _inputs.get(index);
		if(!input._handleReserved)
			throw new IllegalStateException("Input " + index + " no longer owns a lazy handle.");
		input._handleReserved = false;
	}

	public final void discardInputHandle(int index) {
		OOCStreamable<?> source;
		synchronized(this) {
			InputSlot input = _inputs.get(index);
			if(!input._handleReserved)
				return;
			input._handleReserved = false;
			source = input._source;
		}
		source.discardHandle();
	}

	@SuppressWarnings("unchecked")
	protected final <T> OOCStream<T> getInputReadStream(int index) {
		consumeInputHandle(index);
		return (OOCStream<T>) _inputs.get(index)._source.getReservedReadStream();
	}

	protected final OOCFuture<MaterializedStore<IndexedMatrixValue>> getMaterializedInput(int index) {
		MaterializeOOCPrimitive boundary = (MaterializeOOCPrimitive) _inputs.get(index)._dependency;
		boundary.startOnDemand();
		OOCFuture<MaterializedStore<IndexedMatrixValue>> materialized = boundary.store();
		if(materialized == null)
			throw new IllegalStateException("Input " + index + " was not materialized by the planner.");
		return materialized;
	}

	public final void start() {
		if(claimCompilation())
			OOCPlanner.compile(this);
	}

	public final boolean claimCompilation() {
		return _started.compareAndSet(false, true);
	}

	public final void tryStartExecution() {
		if(_executionStarted.compareAndSet(false, true)) {
			GlobalMemoryBroker broker = GlobalMemoryBroker.get();
			_allowance = new SyncMemoryAllowance(broker, getAllowanceLimit(broker));
			startExecution();
		}
	}

	protected long getAllowanceLimit(GlobalMemoryBroker broker) {
		long fairShare = broker.getAllowedMemory() / 3;
		long taskBytes = getMaxTaskReservationBytes();
		if(taskBytes <= 0)
			return fairShare;
		// A primitive never needs more than its workers can hold at once, but it must always be able to admit a single
		// task: a limit below one reservation starves the primitive for as long as the broker is contended.
		long concurrent = 2L * InfrastructureAnalyzer.getLocalParallelism() * taskBytes;
		return Math.max(Math.min(fairShare, concurrent), taskBytes);
	}

	/**
	 * Returns the largest reservation for a task with the given ordered inputs. Callers may provide dummy tiles
	 * carrying only indexes, shape, and sparsity. With no inputs, implementations return a conservative geometry-based
	 * upper bound, or 0 when the geometry is not yet known and the plain fair share applies.
	 *
	 * @param inputs ordered real or dummy input tiles
	 * @return required task reservation in bytes
	 */
	public long getMaxTaskReservationBytes(IndexedMatrixValue... inputs) {
		return 0;
	}

	public final boolean fail(Throwable error) {
		if(!_failed.compareAndSet(false, true))
			return false;
		_failure = error;
		if(_context != null)
			_context.failAll(DMLRuntimeException.of(error));
		return true;
	}

	public final Throwable getFailure() {
		return _failure;
	}

	protected final boolean hasFailed() {
		return _failed.get();
	}

	/**
	 * Emits a mapped output while preserving an unchanged input callback. Returning the input MatrixBlock means
	 * read-only pass-through; in-place updates require explicit primitive support. Reindexed aliases are copied because
	 * the retained callback exposes the original indexes.
	 */
	protected final void prepareOutput(OOCStream<IndexedMatrixValue> output,
		OOCStream.QueueCallback<IndexedMatrixValue> input, IndexedMatrixValue result, ReservationBudget budget) {
		try {
			IndexedMatrixValue source = input.get();
			if(result.getValue() == source.getValue()) {
				if(result.getIndexes().equals(source.getIndexes())) {
					OOCStream.QueueCallback<IndexedMatrixValue> retained = input.keepOpen();
					try {
						output.enqueue(retained);
						retained = null;
					}
					finally {
						if(retained != null)
							retained.close();
					}
					return;
				}
				result = new IndexedMatrixValue(new MatrixIndexes(result.getIndexes()),
					new MatrixBlock((MatrixBlock) result.getValue()));
			}
			OOCUtils.enqueueExact(output, result, budget);
			budget = null;
		}
		finally {
			if(budget != null)
				budget.close();
		}
	}

	public final void onComplete() {
		for(int i = 0; i < _inputs.size(); i++)
			discardInputHandle(i);
		_allowance.shutdown();
		if(OOCWatchdog.WATCH_PRIMITIVES)
			OOCWatchdog.unregisterPrimitive(this);
	}

	public final String debugState() {
		StringBuilder sb = new StringBuilder();
		sb.append(getClass().getSimpleName()).append('@').append(System.identityHashCode(this)).append(" started=")
			.append(_executionStarted.get()).append(" compiled=").append(_started.get()).append(" failed=")
			.append(_failed.get()).append(" pattern=").append(_pattern);
		if(_allowance != null)
			sb.append(" allowance[").append(_allowance.debugState()).append(']');
		if(_failure != null)
			sb.append(" failure=").append(_failure);
		for(int i = 0; i < _inputs.size(); i++) {
			InputSlot input = _inputs.get(i);
			sb.append("\n      in[").append(i).append("] ").append(input._source.debugState())
				.append(" handleReserved=").append(input._handleReserved).append(" producer=")
				.append(describeProducer(input._dependency));
		}
		return sb.toString();
	}

	private static String describeProducer(OOCPrimitive producer) {
		if(producer == null)
			return "none";
		return producer.getClass().getSimpleName() + "@" + System.identityHashCode(producer) + "(started="
			+ producer._executionStarted.get() + ")";
	}

	public final void inferPatterns() {
		if(!hasStartedExecution())
			inferPatternsInternal();
	}

	public final void requestPattern(OOCAccessPattern accessPattern) {
		if(!hasStartedExecution() && _pattern != accessPattern)
			requestPatternInternal(accessPattern);
	}

	private void rebuildInputChildren() {
		List<OOCPrimitive> next = new ArrayList<>();
		for(InputSlot input : _inputs)
			if(input._dependency != null)
				next.add(input._dependency);
		for(OOCPrimitive child : _children)
			if(!next.contains(child))
				child._parents.remove(this);
		for(OOCPrimitive child : next)
			child._parents.add(this);
		_children.clear();
		_children.addAll(next);
	}

	protected abstract void startExecution();

	protected abstract void inferPatternsInternal();

	protected abstract void requestPatternInternal(OOCAccessPattern accessPattern);

	private static final class InputSlot {
		private final OOCStreamable<?> _source;
		private OOCPrimitive _dependency;
		private boolean _handleReserved;

		private InputSlot(OOCStreamable<?> source) {
			_source = source;
			_dependency = source.getPrimitive();
			_handleReserved = true;
			source.reserveLazyHandle();
		}
	}

	public record OOCMaterializedInputRequest(int inputIndex, OOCStoreLayout layout, int expectedReaders,
		Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> liveConsumer, Consumer<Boolean> liveRegistration,
		LongBinaryOperator evictionPolicy) {
		public OOCMaterializedInputRequest(int inputIndex, OOCStoreLayout layout, int expectedReaders) {
			this(inputIndex, layout, expectedReaders, null, null, null);
		}

		public OOCMaterializedInputRequest(int inputIndex, OOCStoreLayout layout, int expectedReaders,
			LongBinaryOperator evictionPolicy) {
			this(inputIndex, layout, expectedReaders, null, null, evictionPolicy);
		}

		public OOCMaterializedInputRequest(int inputIndex, OOCStoreLayout layout, int expectedReaders,
			Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> liveConsumer) {
			this(inputIndex, layout, expectedReaders, liveConsumer, null, null);
		}

		public OOCMaterializedInputRequest(int inputIndex, OOCStoreLayout layout, int expectedReaders,
			Consumer<OOCStream.QueueCallback<IndexedMatrixValue>> liveConsumer, Consumer<Boolean> liveRegistration) {
			this(inputIndex, layout, expectedReaders, liveConsumer, liveRegistration, null);
		}
	}
}
