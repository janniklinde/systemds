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
import org.apache.sysds.runtime.ooc.planning.OOCPlanner;
import org.apache.sysds.runtime.ooc.planning.OOCRegionBinding;
import org.apache.sysds.runtime.ooc.planning.OOCStoreBinding;
import org.apache.sysds.runtime.ooc.planning.OOCStoreRequest;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;

public abstract class OOCPrimitive {
	private static final AtomicInteger COORDINATOR_THREAD_ID = new AtomicInteger();
	protected static final ExecutorService COORDINATOR_EXECUTOR = Executors.newCachedThreadPool(r -> {
		Thread thread = new Thread(r, "ooc-primitive-coordinator-" + COORDINATOR_THREAD_ID.incrementAndGet());
		thread.setDaemon(true);
		return thread;
	});

	private final List<OOCPrimitive> _children;
	private final List<OOCPrimitive> _parents;
	private final AtomicBoolean _started;
	private final AtomicBoolean _executionStarted;
	protected OOCAccessPattern _pattern;
	protected OOCRegionBinding _regionBinding;
	protected MemoryAllowance _allowance;
	protected ToLongFunction<MatrixIndexes> _allocFn;
	protected boolean _crossBoundaries;
	protected boolean _startsRegion;

	public OOCPrimitive(List<OOCPrimitive> children) {
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
	 * Declares a materialized store over one of this primitive's boundary inputs, or null when the
	 * primitive does not consume a store (or keeps its legacy path). The request carries only what
	 * the planner cannot know — the boundary's index linearization and the registration counts —
	 * while the planner supplies cache, stream id, and sink allowance, and answers with
	 * {@link #bindStore(OOCStoreBinding)}. The primitive attaches the binding's sink to its input
	 * stream when execution starts.
	 */
	public OOCStoreRequest requiresStore() {
		return null;
	}

	/**
	 * Capability seam for primitives consuming a materialized boundary: the planner hands the shared
	 * {@link OOCStoreBinding} of an input store (created once per boundary; reader registration goes
	 * through the binding so {@code sealReaders()} fires exactly when the declared consumer set has
	 * registered). The primitive must await {@code binding.completion()} before opening readers and
	 * call {@code binding.release()} when done. Long-term, primitives declare strategy variants here
	 * (join RENDEZVOUS vs INDEXED_LOOKUP, replay ordered vs opportunistic) and the planner picks.
	 */
	public void bindStore(OOCStoreBinding store) {
		throw new UnsupportedOperationException();
	}

	public long getDenseTileMemoryFactor() {
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
		COORDINATOR_EXECUTOR.execute(() -> {
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

	public abstract List<OOCStreamable<?>> getInputStreams();
	public abstract List<OOCStreamable<?>> getOutputStreams();
	public abstract void inferPatterns();
	public abstract void requestPattern(OOCAccessPattern accessPattern);
}
