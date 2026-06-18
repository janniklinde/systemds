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

package org.apache.sysds.runtime.ooc.planning;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;

/**
 * One {@link OOCStoreBinding} per materialized input, keyed by the input streamable identity. An
 * input is materialized exactly once: the first consumer's compile creates the binding, every other
 * consumer receives the SAME binding and opens its reader on the existing store.
 *
 * Because all readers must register before the store seals (forgetting needs the full reader
 * population), the registry counts the input's consumer set UP FRONT at binding creation: it walks
 * the producer parents and aggregates the {@code requiresMaterializedInput()} requests of every
 * already-constructed consumer of the same input. If consumers prefer different physical layouts,
 * the first request's layout wins; layout is a cache-locality hint, not a correctness contract.
 */
public final class OOCStoreBindingRegistry {
	private static final Map<OOCStreamable<?>, Entry> BINDINGS = new IdentityHashMap<>();

	private OOCStoreBindingRegistry() {
	}

	public static OOCStoreBinding acquire(OOCMaterializedInputRequest request, OOCPrimitive requester,
		MemoryAllowance sinkAllowance) {
		OOCStreamable<IndexedMatrixValue> source = request.input();
		if(source == null) {
			//anonymous boundary: never shared
			return new OOCStoreBinding(null, OOCCacheManager.getGlobalCache(), CachingStream._streamSeq.getNextID(),
				request.preferredLayout(), sinkAllowance, request.expectedReaders(), request.consumers());
		}
		synchronized(BINDINGS) {
			Entry entry = BINDINGS.get(source);
			if(entry != null && entry.binding.isReleased()) {
				//all declared consumers are done and the store is closed; a re-consumable source
				//(e.g. a regenerated stream handle) legitimately starts a fresh materialization
				BINDINGS.remove(source);
				entry = null;
			}
			if(entry != null) {
				if(entry.counted.contains(requester))
					return entry.binding;
				if(!entry.binding.tryRegister(request.expectedReaders(), request.consumers()))
					throw new DMLRuntimeException("A consumer joined a materialized input after its reader set "
						+ "sealed; all consumers of an input must be constructed before the declared "
						+ "readers register (source=" + source + ").");
				entry.counted.add(requester);
				return entry.binding;
			}

			Entry created = createEntry(request, requester, sinkAllowance, source);
			BINDINGS.put(source, created);
			return created.binding;
		}
	}

	public static void reset() {
		synchronized(BINDINGS) {
			BINDINGS.clear();
		}
	}

	private static Entry createEntry(OOCMaterializedInputRequest request, OOCPrimitive requester,
		MemoryAllowance sinkAllowance, OOCStreamable<IndexedMatrixValue> source) {
		//aggregate the requests of every already-constructed consumer of this input, so the
		//declared reader/consumer counts cover the full set before the first reader registers
		Set<OOCPrimitive> counted = Collections.newSetFromMap(new IdentityHashMap<>());
		int readers = 0;
		int consumers = 0;
		OOCPrimitive producer = producerOf(source);
		if(producer != null) {
			for(OOCPrimitive parent : producer.getParents()) {
				//a consumer that already executed belongs to an earlier (released) materialization
				//of this input; its reader registration will never arrive on this binding
				if(parent != requester && parent.hasStartedExecution())
					continue;
				OOCMaterializedInputRequest parentRequest = parent == requester ? request :
					parent.requiresMaterializedInput();
				if(parentRequest == null || parentRequest.input() != source || !counted.add(parent))
					continue;
				readers += parentRequest.expectedReaders();
				consumers += parentRequest.consumers();
			}
		}
		if(!counted.contains(requester)) {
			counted.add(requester);
			readers += request.expectedReaders();
			consumers += request.consumers();
		}
		OOCStoreBinding binding = new OOCStoreBinding(source, OOCCacheManager.getGlobalCache(),
			CachingStream._streamSeq.getNextID(), request.preferredLayout(), sinkAllowance, readers, consumers);
		return new Entry(binding, counted);
	}

	private static OOCPrimitive producerOf(OOCStreamable<IndexedMatrixValue> source) {
		try {
			return source.getPrimitive();
		}
		catch(RuntimeException ex) {
			return null;
		}
	}

	private static final class Entry {
		private final OOCStoreBinding binding;
		private final Set<OOCPrimitive> counted;

		private Entry(OOCStoreBinding binding, Set<OOCPrimitive> counted) {
			this.binding = binding;
			this.counted = counted;
		}
	}
}
