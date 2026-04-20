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

import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
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
		if(primitive.isLeaf()) {
			leaves.add(primitive);
			return;
		}

		for(OOCPrimitive child : primitive.getChildren())
			findLeaves(child, leaves);
	}

	private static void inferAccessPatterns(Set<OOCPrimitive> leaves) {
		for(OOCPrimitive leaf : leaves)
			leaf.inferPatterns();
	}

	private static void compileRegion(List<OOCPrimitive> region) {
		MemoryAllowance allowance = new SyncMemoryAllowance(GlobalMemoryBroker.get());
		ToLongFunction<MatrixIndexes> allocFn = buildAllocFn(region);

		for(int i = 0; i < region.size(); i++) {
			boolean crossBoundaries = i == 0;
			region.get(i).bindRegion(new OOCRegionBinding(allowance, allocFn, crossBoundaries));
		}
	}

	private static void startRegion(List<OOCPrimitive> region) {
		for(OOCPrimitive primitive : region)
			primitive.startExecution();
	}

	private static void collectRegionRoots(OOCPrimitive primitive, Set<OOCPrimitive> visited, List<OOCPrimitive> regionRoots) {
		if(!visited.add(primitive))
			return;

		for(OOCPrimitive child : primitive.getChildren())
			collectRegionRoots(child, visited, regionRoots);

		if(isRegionRoot(primitive))
			regionRoots.add(primitive);
	}

	private static boolean isRegionRoot(OOCPrimitive primitive) {
		if(!isFusiblePrimitive(primitive))
			return false;
		if(primitive.getParents().size() != 1)
			return true;

		OOCPrimitive parent = primitive.getParents().get(0);
		return !canFuseDownstream(parent, primitive);
	}

	private static boolean isFusiblePrimitive(OOCPrimitive primitive) {
		return primitive.isTileLocal() && !primitive.isMaterializationBoundary();
	}

	private static boolean canFuseDownstream(OOCPrimitive downstream, OOCPrimitive upstream) {
		return downstream.isTileLocal()
			&& downstream.isOneToOne()
			&& downstream.isIndexPreserving()
			&& !downstream.isMaterializationBoundary()
			&& downstream.getChildren().size() == 1
			&& upstream.getParents().size() == 1
			&& isFusiblePrimitive(upstream);
	}

	private static List<OOCPrimitive> buildRegionFromRoot(OOCPrimitive regionRoot, Set<OOCPrimitive> assigned) {
		List<OOCPrimitive> region = new ArrayList<>();
		OOCPrimitive current = regionRoot;

		while(current != null && isFusiblePrimitive(current) && assigned.add(current)) {
			region.add(current);
			if(current.getChildren().size() != 1)
				break;

			OOCPrimitive child = current.getChildren().get(0);
			if(!canFuseDownstream(current, child))
				break;
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
