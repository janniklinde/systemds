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

import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.ooc.cache.OOCCacheManager;
import org.apache.sysds.runtime.ooc.memory.CachedAllowance;
import org.apache.sysds.runtime.ooc.memory.GlobalMemoryBroker;
import org.apache.sysds.runtime.ooc.memory.MemoryAllowance;
import org.apache.sysds.runtime.ooc.memory.SyncMemoryAllowance;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.store.OperatorStateTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;

public class OOCPlanner {
	public static void compile(OOCPrimitive root) {
		Set<OOCPrimitive> leaves = new HashSet<>();
		findLeaves(root, leaves);
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

		for(List<OOCPrimitive> region : regions)
			compileRegion(region);

		for(int i = regions.size() - 1; i >= 0; i--)
			startRegion(regions.get(i));
	}

	private static void findLeaves(OOCPrimitive primitive, Set<OOCPrimitive> leaves) {
		if(primitive.hasStartedExecution()) {
			leaves.add(primitive);
			return;
		}

		if(primitive.isLeaf()) {
			leaves.add(primitive);
			return;
		}

		for(OOCPrimitive child : primitive.getChildren())
			findLeaves(child, leaves);
	}

	private static void inferAccessPatterns(Set<OOCPrimitive> leaves) {
		for(OOCPrimitive leaf : leaves) {
			if(leaf.hasStartedExecution())
				leaf.getParents().forEach(OOCPrimitive::inferPatterns);
			else
				leaf.inferPatterns();
		}
	}

	private static void compileRegion(List<OOCPrimitive> region) {
		List<OOCPrimitive> activeRegion = new ArrayList<>();
		for(OOCPrimitive primitive : region) {
			if(!primitive.hasStartedExecution())
				activeRegion.add(primitive);
		}
		if(activeRegion.isEmpty())
			return;

		MemoryAllowance allowance = new SyncMemoryAllowance(GlobalMemoryBroker.get(), 200_000_000);
		ToLongFunction<MatrixIndexes> allocFn = buildAllocFn(region);
		OOCRegionBinding binding = new OOCRegionBinding(allowance, allocFn, new AtomicInteger(activeRegion.size()));

		for(int i = 0; i < activeRegion.size(); i++) {
			OOCPrimitive primitive = activeRegion.get(i);
			boolean crossBoundaries = i == 0;
			boolean startsRegion = i == activeRegion.size() - 1;
			primitive.bindRegion(binding, crossBoundaries, startsRegion);
			//migrated primitives get an OperatorStateTable over the global cache (one fresh stream id
			//per table so eviction sees one population); unmigrated primitives keep CachedAllowance.
			//A boundary consumer additionally declares its input store. The registry hands out ONE
			//binding per boundary: the first consumer materializes (fresh stream id, this region
			//allowance as sink allowance), later consumers share the existing store.
			OOCStoreRequest storeRequest = primitive.requiresStore();
			if(storeRequest != null)
				primitive.bindStore(OOCStoreBindingRegistry.acquire(storeRequest, primitive, allowance));
			if(primitive.requiresStateTable()) {
				primitive.bindStateTable(new OperatorStateTable<>(OOCCacheManager.getGlobalCache(),
					CachingStream._streamSeq.getNextID(), allowance));
			}
			else if(primitive.requiresCache()) {
				primitive.bindCache(new CachedAllowance(GlobalMemoryBroker.get()));
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

	private static ToLongFunction<MatrixIndexes> buildAllocFn(List<OOCPrimitive> region) {
		long primitiveFactor = 1;
		DataCharacteristics dc = null;

		for(OOCPrimitive primitive : region) {
			primitiveFactor = Math.max(primitiveFactor, primitive.getDenseTileMemoryFactor());

			if(dc == null && !primitive.getOutputStreams().isEmpty())
				dc = primitive.getOutputStreams().get(0).getDataCharacteristics();
		}

		final DataCharacteristics outDc = dc;
		final long factor = primitiveFactor;
		return ix -> factor * estimateDenseTileBytes(outDc, ix);
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
