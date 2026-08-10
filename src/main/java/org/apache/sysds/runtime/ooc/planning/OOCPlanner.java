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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCFuture;
import org.apache.sysds.runtime.ooc.primitives.MaterializeOOCPrimitive;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;

public final class OOCPlanner {
	public static void compile(OOCPrimitive root) {
		injectMaterializations(root, Collections.newSetFromMap(new IdentityHashMap<>()), new IdentityHashMap<>());
		List<OOCPrimitive> primitives = new ArrayList<>();
		collect(root, Collections.newSetFromMap(new IdentityHashMap<>()), primitives);
		if(primitives.isEmpty())
			return;

		for(int i = primitives.size() - 1; i >= 0; i--)
			if(primitives.get(i).getAccessPattern().isUnset())
				primitives.get(i).inferPatterns();
		if(root.getAccessPattern() == OOCAccessPattern.ANY || root.getAccessPattern().isUnset())
			root.requestPattern(OOCAccessPattern.ROW_MAJOR);

		for(OOCPrimitive primitive : primitives)
			if(!deferUntilDimensionsResolved(primitive))
				primitive.tryStartExecution();
	}

	/**
	 * Holds back a primitive that declared dimension-critical inputs which are not resolved yet. The rest of the plan
	 * starts regardless: the deferred primitive's own inputs are among them, so the producers that resolve the
	 * dimensions are running, and its consumers simply block on its output stream.
	 *
	 * @param primitive candidate
	 * @return true if the start was deferred to a completion callback
	 */
	private static boolean deferUntilDimensionsResolved(OOCPrimitive primitive) {
		List<Integer> critical = primitive.dimensionCriticalInputs();
		if(critical.isEmpty() || primitive.hasStartedExecution())
			return false;
		List<OOCFuture<DataCharacteristics>> pending = new ArrayList<>(critical.size());
		for(int index : critical) {
			OOCFuture<DataCharacteristics> dimensions = primitive.getInput(index).dimensions();
			//a streamable that cannot signal late resolution leaves the primitive to its own guard, which keeps this
			//a no-op for every plan that does not opt in
			if(dimensions == null)
				continue;
			if(dimensions.isDone())
				continue;
			checkResolvable(primitive, index);
			pending.add(dimensions);
		}
		if(pending.isEmpty())
			return false;
		OOCFuture.allOf(pending, ignored -> {
		}).whenComplete((ignored, error) -> {
			if(error != null)
				primitive.fail(error);
			else
				primitive.tryStartExecution();
		});
		return true;
	}

	/**
	 * Rejects a deferral that could never complete, namely one whose dimensions are produced by a sub-DAG that
	 * transitively depends on the primitive being deferred.
	 */
	private static void checkResolvable(OOCPrimitive primitive, int index) {
		OOCPrimitive producer = primitive.getInputDependency(index);
		if(producer == null)
			return;
		if(producer == primitive || dependsOn(producer, primitive, Collections.newSetFromMap(new IdentityHashMap<>())))
			throw new DMLRuntimeException("Dimension dependency on input " + index + " of "
				+ primitive.getClass().getSimpleName() + " would deadlock: its producer depends on that primitive.");
	}

	private static boolean dependsOn(OOCPrimitive from, OOCPrimitive target, Set<OOCPrimitive> visited) {
		if(from == target)
			return true;
		if(!visited.add(from))
			return false;
		for(OOCPrimitive child : from.getChildren())
			if(dependsOn(child, target, visited))
				return true;
		return false;
	}

	@SuppressWarnings("unchecked")
	private static void injectMaterializations(OOCPrimitive primitive, Set<OOCPrimitive> visited,
		IdentityHashMap<OOCStreamable<IndexedMatrixValue>, MaterializeOOCPrimitive> boundaries) {
		if(primitive.hasStartedExecution() || !visited.add(primitive))
			return;
		for(OOCPrimitive.OOCMaterializedInputRequest request : primitive.requiredMaterializedInputs()) {
			OOCStreamable<IndexedMatrixValue> input = (OOCStreamable<IndexedMatrixValue>) primitive
				.getInput(request.inputIndex());
			OOCPrimitive dependency = primitive.getInputDependency(request.inputIndex());
			if(input.hasMaterializedStore() && dependency instanceof MaterializeOOCPrimitive existingBoundary) {
				boolean live = existingBoundary.registerRequest(request.expectedReaders(), request.liveConsumer());
				if(request.liveRegistration() != null)
					request.liveRegistration().accept(live);
				continue;
			}
			MaterializeOOCPrimitive boundary = boundaries.get(input);
			if(boundary == null) {
				boundary = new MaterializeOOCPrimitive(input, request.layout(), primitive.getContext());
				boundaries.put(input, boundary);
			}
			primitive.discardInputHandle(request.inputIndex());
			boolean live = boundary.registerRequest(request.expectedReaders(), request.liveConsumer());
			if(request.liveRegistration() != null)
				request.liveRegistration().accept(live);
			primitive.installMaterializedInput(request.inputIndex(), boundary);
		}
		for(OOCPrimitive child : primitive.getChildren())
			injectMaterializations(child, visited, boundaries);
	}

	private static void collect(OOCPrimitive primitive, Set<OOCPrimitive> visited, List<OOCPrimitive> result) {
		if(primitive.hasStartedExecution() || !visited.add(primitive))
			return;
		result.add(primitive);
		for(OOCPrimitive child : primitive.getChildren())
			collect(child, visited, result);
	}
}
