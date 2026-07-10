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

import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class OOCPlanner {
	public static void compile(OOCPrimitive root) {
		Set<OOCPrimitive> leaves = new HashSet<>();
		Set<OOCPrimitive> visitedLeaves = Collections.newSetFromMap(new IdentityHashMap<>());
		findLeaves(root, visitedLeaves, leaves);
		inferAccessPatterns(leaves);

		if(root.getAccessPattern() == OOCAccessPattern.ANY || root.getAccessPattern() == OOCAccessPattern.UNSET)
			root.requestPattern(OOCAccessPattern.ROW_MAJOR);

		List<OOCPrimitive> regionRoots = new ArrayList<>();
		Set<OOCPrimitive> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		collectRegionRoots(root, visited, regionRoots);

		List<List<OOCPrimitive>> regions = new ArrayList<>();
		Set<OOCPrimitive> assigned = Collections.newSetFromMap(new IdentityHashMap<>());
		for(OOCPrimitive regionRoot : regionRoots) {
			List<OOCPrimitive> region = buildRegionFromRoot(regionRoot, assigned);
			if(!region.isEmpty())
				regions.add(region);
		}

		List<OOCStoreBinding> materializedInputs = new ArrayList<>();
		for(List<OOCPrimitive> region : regions)
			compileRegion(region, materializedInputs);

		for(OOCStoreBinding materializedInput : materializedInputs)
			materializedInput.attachMaterializedInput();

		for(int i = regions.size() - 1; i >= 0; i--)
			startRegion(regions.get(i));
	}

	private static void findLeaves(OOCPrimitive primitive, Set<OOCPrimitive> visited, Set<OOCPrimitive> leaves) {
		if(!visited.add(primitive))
			return;

		if(primitive.hasStartedExecution()) {
			leaves.add(primitive);
			return;
		}

		if(primitive.isLeaf()) {
			leaves.add(primitive);
			return;
		}

		for(OOCPrimitive child : primitive.getChildren())
			findLeaves(child, visited, leaves);
	}

	private static void inferAccessPatterns(Set<OOCPrimitive> leaves) {
		for(OOCPrimitive leaf : leaves) {
			if(leaf.hasStartedExecution())
				leaf.getParents().forEach(OOCPrimitive::inferPatterns);
			else
				leaf.inferPatterns();
		}
	}

	private static void compileRegion(List<OOCPrimitive> region, List<OOCStoreBinding> materializedInputs) {
		List<OOCPrimitive> activeRegion = new ArrayList<>();
		for(OOCPrimitive primitive : region) {
			if(!primitive.hasStartedExecution())
				activeRegion.add(primitive);
		}
		if(activeRegion.isEmpty())
			return;

		long minimumOperatingBytes = buildMinimumOperatingBytes(region);
		MemoryAllowance allowance = new SyncMemoryAllowance(GlobalMemoryBroker.get(),
			GlobalMemoryBroker.defaultAllowanceLimit(), minimumOperatingBytes);
		OOCRegionBinding binding = new OOCRegionBinding(allowance, new AtomicInteger(activeRegion.size()));

		for(int i = 0; i < activeRegion.size(); i++) {
			OOCPrimitive primitive = activeRegion.get(i);
			primitive.bindRegion(binding);
			//A primitive may additionally declare an input that must be materialized. The registry
			//hands out ONE binding per input; the planner attaches the source once after all regions
			//are bound, and consumers only open readers on the binding they receive.
			OOCMaterializedInputRequest inputRequest = primitive.requiresMaterializedInput();
			if(inputRequest != null) {
				OOCStreamable<IndexedMatrixValue> source =
					primitive.getInputStream(inputRequest.inputIndex());
				OOCStoreBinding store = OOCStoreBindingRegistry.acquire(inputRequest, primitive, source, allowance);
				primitive.replaceInputStream(inputRequest.inputIndex(),
					new MaterializedInputStreamable(source, store));
				materializedInputs.add(store);
			}
			if(primitive.requiresCache()) {
				CachedAllowance cache = new CachedAllowance(GlobalMemoryBroker.get());
				cache.registerDebugOwner(primitive.getClass().getSimpleName() + "@"
					+ System.identityHashCode(primitive) + "[legacy-cache]");
				primitive.bindCache(cache);
			}
		}
	}

	private static void startRegion(List<OOCPrimitive> region) {
		for(OOCPrimitive primitive : region)
			primitive.tryStartExecution();
	}

	private static void collectRegionRoots(OOCPrimitive primitive, Set<OOCPrimitive> visited, List<OOCPrimitive> regionRoots) {
		if(!visited.add(primitive))
			return;

		if(primitive.hasStartedExecution())
			return;

		for(OOCPrimitive child : primitive.getChildren())
			collectRegionRoots(child, visited, regionRoots);

		if(isRegionRoot(primitive))
			regionRoots.add(primitive);
	}

	private static boolean isRegionRoot(OOCPrimitive primitive) {
		if(primitive.getParents().size() != 1)
			return true;

		OOCPrimitive parent = primitive.getParents().get(0);
		return !canFuseDownstream(parent, primitive);
	}

	private static boolean canFuseDownstream(OOCPrimitive downstream, OOCPrimitive upstream) {
		if(downstream.hasStartedExecution() || upstream.hasStartedExecution())
			return false;

		return downstream.isTileLocal()
			&& downstream.isOneToOne()
			&& downstream.isIndexPreserving()
			&& !downstream.isMaterializationBoundary()
			&& downstream.getChildren().size() == 1
			&& upstream.getParents().size() == 1
			&& !upstream.isMaterializationBoundary()
			&& upstream.isTileLocal();
	}

	private static List<OOCPrimitive> buildRegionFromRoot(OOCPrimitive regionRoot, Set<OOCPrimitive> assigned) {
		List<OOCPrimitive> region = new ArrayList<>();
		OOCPrimitive current = regionRoot;

		if(!assigned.add(current))
			return region;
		region.add(current);

		while(current.getChildren().size() == 1) {
			OOCPrimitive child = current.getChildren().get(0);
			if(child.hasStartedExecution() || !canFuseDownstream(current, child) || !assigned.add(child))
				break;
			region.add(child);
			current = child;
		}

		return region;
	}

	private static long buildMinimumOperatingBytes(List<OOCPrimitive> region) {
		long primitiveFactor = 1;
		DataCharacteristics dc = null;

		for(OOCPrimitive primitive : region) {
			primitiveFactor = Math.max(primitiveFactor, primitive.getMinimumOperatingMemoryFactor());

			if(dc == null && !primitive.getOutputStreams().isEmpty())
				dc = primitive.getOutputStreams().get(0).getDataCharacteristics();
		}

		return primitiveFactor * estimateDenseTileBytes(dc, new MatrixIndexes(1, 1));
	}

	private static long estimateDenseTileBytes(DataCharacteristics dc, MatrixIndexes ix) {
		if(dc == null || dc.getBlocksize() <= 0 || !dc.dimsKnown()) {
			int blen = dc != null && dc.getBlocksize() > 0 ? dc.getBlocksize() : 1000;
			return MatrixBlock.estimateSizeDenseInMemory(blen, blen);
		}

		long blen = dc.getBlocksize();
		long rowStart = (ix.getRowIndex() - 1) * blen;
		long colStart = (ix.getColumnIndex() - 1) * blen;
		long rows = Math.max(0, Math.min(blen, dc.getRows() - rowStart));
		long cols = Math.max(0, Math.min(blen, dc.getCols() - colStart));
		return MatrixBlock.estimateSizeDenseInMemory(rows, cols);
	}
}
