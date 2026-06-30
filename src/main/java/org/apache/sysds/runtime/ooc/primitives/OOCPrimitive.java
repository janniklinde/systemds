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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.planning.OOCAccessPattern;
import org.apache.sysds.runtime.ooc.planning.OOCMaterializedInputRequest;
import org.apache.sysds.runtime.ooc.planning.OOCPlanner;
import org.apache.sysds.runtime.ooc.planning.OOCRegionBinding;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;

public abstract class OOCPrimitive {
	private static final AtomicInteger COORDINATOR_THREAD_ID = new AtomicInteger();
	private static final AtomicReference<ExecutorService> COORDINATOR_EXECUTOR =
		new AtomicReference<>(newCoordinatorExecutor());

	private final List<OOCPrimitive> _children;
	private final List<OOCPrimitive> _parents;
	private final List<OOCStreamable<?>> _inputStreams;
	private final List<OOCStreamable<?>> _outputStreams;
	private final AtomicBoolean _started;
	private final AtomicBoolean _executionStarted;
	protected OOCAccessPattern _pattern;
	protected OOCRegionBinding _regionBinding;
	protected MemoryAllowance _allowance;
	protected ToLongFunction<MatrixIndexes> _allocFn;
	protected boolean _crossBoundaries;
	protected boolean _startsRegion;

	public OOCPrimitive(List<OOCPrimitive> children) {
		this(children, List.of(), List.of());
	}

	public OOCPrimitive(List<OOCPrimitive> children, List<? extends OOCStreamable<?>> inputs,
		List<? extends OOCStreamable<?>> outputs) {
		_children = new ArrayList<>();
		if(children != null) {
			for(OOCPrimitive child : children) {
				if(child == null)
					continue;
				_children.add(child);
				child.addParent(this);
			}
		}
		_parents = new ArrayList<>();
		_inputStreams = new ArrayList<>();
		if(inputs != null) {
			for(OOCStreamable<?> input : inputs)
				_inputStreams.add(reserveLazyHandle(input));
		}
		_outputStreams = outputs == null ? new ArrayList<>() : new ArrayList<>(outputs);
		_started = new AtomicBoolean(false);
		_executionStarted = new AtomicBoolean(false);
		_pattern = OOCAccessPattern.UNSET;
	}

	public void bindRegion(OOCRegionBinding binding, boolean crossBoundaries, boolean startsRegion) {
		_regionBinding = binding;
		_allowance = binding.allowance();
		_allocFn = binding.allocFn();
		_crossBoundaries = crossBoundaries;
		_startsRegion = startsRegion;
	}

	public List<OOCPrimitive> getChildren() {
		return _children;
	}

	public void addParent(OOCPrimitive p) {
		_parents.add(p);
	}

	public List<OOCPrimitive> getParents() {
		return _parents;
	}

	public void inferPatterns(List<OOCPrimitive> toInfer) {
		toInfer.stream().filter(OOCPrimitive::patternUnset).forEach(OOCPrimitive::inferPatterns);
	}

	public boolean patternUnset() {
		return _pattern.isUnset();
	}

	public boolean isPlannable() {
		return false;
	}

	public boolean isEmissionControlled() {
		return false;
	}

	public boolean isTileLocal() {
		return false;
	}

	public boolean isOneToOne() {
		return false;
	}

	public boolean isIndexPreserving() {
		return false;
	}

	public boolean isMaterializationBoundary() {
		return false;
	}

	public boolean requiresCache() {
		return false;
	}

	public void bindCache(CachedAllowance cache) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Capability seam of the new architecture: a migrated primitive requests an
	 * {@link OperatorStateTable} (rendezvous, accumulators, retention slots over the global cache)
	 * instead of a {@link CachedAllowance}. Takes precedence over {@link #requiresCache()} so a
	 * primitive can keep both paths selectable during migration. The planner supplies a table over
	 * the global cache with the region allowance and a fresh stream id, so eviction sees the table
	 * as one population. The primitive owns the table lifecycle and closes it in {@code onComplete}.
	 */
	public boolean requiresStateTable() {
		return false;
	}

	public void bindStateTable(OperatorStateTable<IndexedMatrixValue> table) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Declares that one input must be materialized before this primitive can consume it, or null when
	 * no materialized input is required. The request carries the input and the primitive's preferred
	 * physical layout; the planner owns store creation, source attachment, and sharing.
	 */
	public OOCMaterializedInputRequest requiresMaterializedInput() {
		return null;
	}

	public long getDenseTileMemoryFactor() {
		return 1;
	}

	public long getMinimumOperatingMemoryFactor() {
		return 1;
	}

	public boolean isLeaf() {
		return _children.isEmpty();
	}

	public OOCAccessPattern getAccessPattern() {
		return _pattern;
	}

	public final void start() {
		if(_started.compareAndSet(false, true))
			OOCPlanner.compile(this);
	}

	public final boolean hasStartedExecution() {
		return _executionStarted.get();
	}

	public final void tryStartExecution() {
		if(_executionStarted.compareAndSet(false, true))
			startExecution();
	}

	public void startExecution() {
		throw new NotImplementedException();
	}

	public void onComplete() {
		_regionBinding.dereference();
	}

	protected static void runCoordinator(String name, Runnable task) {
		COORDINATOR_EXECUTOR.get().execute(() -> {
			Thread thread = Thread.currentThread();
			String oldName = thread.getName();
			if(name != null)
				thread.setName(name);
			try {
				task.run();
			}
			finally {
				thread.setName(oldName);
			}
		});
	}

	public static void resetCoordinatorExecutor() {
		COORDINATOR_EXECUTOR.getAndSet(newCoordinatorExecutor()).shutdownNow();
	}

	private static ExecutorService newCoordinatorExecutor() {
		return Executors.newCachedThreadPool(r -> {
			Thread thread = new Thread(r, "ooc-primitive-coordinator-" + COORDINATOR_THREAD_ID.incrementAndGet());
			thread.setDaemon(true);
			return thread;
		});
	}

	protected static OOCPrimitive safePrimitive(OOCStreamable<?> streamable) {
		if(streamable == null)
			return null;
		try {
			return streamable.getPrimitive();
		}
		catch(RuntimeException ex) {
			return null;
		}
	}

	protected static <T extends OOCStreamable<?>> T reserveLazyHandle(T streamable) {
		if(streamable != null)
			streamable.reserveLazyHandle();
		return streamable;
	}

	protected OOCAccessPattern getPattern(OOCStreamable<?> streamable) {
		OOCPrimitive primitive = safePrimitive(streamable);
		return primitive == null ? OOCAccessPattern.UNKNOWN : primitive.getAccessPattern();
	}

	@SuppressWarnings("unchecked")
	public <T extends OOCStreamable<?>> T getInputStream(int index) {
		return (T) _inputStreams.get(index);
	}

	@SuppressWarnings("unchecked")
	public <T extends OOCStreamable<?>> T getOutputStream(int index) {
		return (T) _outputStreams.get(index);
	}

	public void replaceInputStream(int index, OOCStreamable<?> replacement) {
		_inputStreams.set(index, replacement);
	}

	public List<OOCStreamable<?>> getInputStreams() {
		return List.copyOf(_inputStreams);
	}

	public List<OOCStreamable<?>> getOutputStreams() {
		return List.copyOf(_outputStreams);
	}

	public abstract void inferPatterns();
	public abstract void requestPattern(OOCAccessPattern accessPattern);
}
