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

package org.apache.sysds.hops.rewrite;

import org.apache.sysds.api.DMLScript;
import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.OpOpData;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.IndexingOp;
import org.apache.sysds.hops.ReorgOp;
import org.apache.sysds.parser.IfStatementBlock;
import org.apache.sysds.parser.StatementBlock;
import org.apache.sysds.parser.WhileStatementBlock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This Rewrite rule injects a Tee Operator for specific Out-Of-Core (OOC) patterns
 * where a value or an intermediate result is shared twice. Since for OOC we data streams
 * can only be consumed once.
 *
 * <p>
 *   Pattern identified {@code t(X) %*% X}, where the data {@code X} will be shared by
 *   {@code t(X)} and {@code %*%} multiplication.
 * </p>
 *
 * The rewrite uses a stable two-pass approach:
 * 1. <b>Find candidates (Read-Only):</b> Traverse the entire HOP DAG to identify candidates
 * the fit the target pattern.
 * 2. <b>Apply Rewrites (Modification):</b> Iterate over the collected candidate and put
 * {@code TeeOp}, and safely rewire the graph.
 */
public class RewriteInjectOOCTee extends StatementBlockRewriteRule {

	public static boolean APPLY_ONLY_XtX_PATTERN = false;

	private static final Map<String, Integer> _transientVars = new HashMap<>();
	private static final Map<String, List<Hop>> _transientHops = new HashMap<>();
	private static final Set<String> teeTransientVars = new HashSet<>();
	
	private static final Set<Long> rewrittenHops = new HashSet<>();
	private static final Map<Long, Hop> handledHop = new HashMap<>();

	// Maintain a list of candidates to rewrite in the second pass
	private final List<Hop> rewriteCandidates = new ArrayList<>();
	private boolean forceTee = false;

	/**
	 * First pass: Find candidates for rewrite without modifying the graph.
	 * This method traverses the graph and identifies nodes that need to be
	 * rewritten based on the transpose-matrix multiply pattern.
	 *
	 * @param hop current hop being examined
	 */
	private void findRewriteCandidates(Hop hop) {
		if (hop.isVisited()) {
			return;
		}

		// Mark as visited to avoid processing the same hop multiple times
		hop.setVisited(true);

		// Recursively traverse the graph (depth-first)
		for (Hop input : hop.getInput()) {
			findRewriteCandidates(input);
		}

		boolean isRewriteCandidate = DMLScript.USE_OOC
			&& hop.getDataType().isMatrix()
			&& !isSharingHop(hop)
			&& hop.getParent().size() > 1
			&& (!APPLY_ONLY_XtX_PATTERN || isSelfTranposePattern(hop));

		if (HopRewriteUtils.isData(hop, OpOpData.TRANSIENTREAD) && hop.getDataType().isMatrix()) {
			_transientVars.compute(hop.getName(), (key, ctr) -> {
				int incr = (isRewriteCandidate || forceTee) ? 2 : 1;

				int ret = ctr == null ? 0 : ctr;
				ret += incr;

				if (ret > 1)
					teeTransientVars.add(hop.getName());

				return ret;
			});

			_transientHops.compute(hop.getName(), (key, hops) -> {
				if (hops == null)
					return new ArrayList<>(List.of(hop));
				hops.add(hop);
				return hops;
			});

			return; // We do not tee transient reads but rather inject before TWrite or PRead as caching stream
		}

		// Check if this hop is a candidate for OOC Tee injection
		if (DMLScript.USE_OOC 
			&& hop.getDataType().isMatrix()
			&& !isSharingHop(hop)
			&& hop.getParent().size() > 1
			&& (!APPLY_ONLY_XtX_PATTERN || isSelfTranposePattern(hop))) //FIXME remove
		{
			rewriteCandidates.add(hop);
		}
	}

	/**
	 * Second pass: Apply the TeeOp transformation to a candidate hop.
	 * This safely rewires the graph by creating a TeeOp node and placeholders.
	 *
	 * @param sharedInput the hop to be rewritten
	 */
	private void applyTopDownTeeRewrite(Hop sharedInput) {
		// Only process if not already handled
		if (handledHop.containsKey(sharedInput.getHopID())) {
			return;
		}

		int consumerCount = sharedInput.getParent().size();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Inject tee for hop " + sharedInput.getHopID() + " ("
				+ sharedInput.getName() + "), consumers=" + consumerCount);
		}

		// Take a defensive copy of consumers before modifying the graph
		ArrayList<Hop> consumers = new ArrayList<>(sharedInput.getParent());

		// Create the new TeeOp with the original hop as input
		DataOp teeOp = new DataOp("tee_out_" + sharedInput.getName(),
			sharedInput.getDataType(), sharedInput.getValueType(), Types.OpOpData.TEE, null,
			sharedInput.getDim1(), sharedInput.getDim2(), sharedInput.getNnz(), sharedInput.getBlocksize());
		HopRewriteUtils.addChildReference(teeOp, sharedInput);

		// Rewire the graph: replace original connections with TeeOp outputs
		for (Hop consumer : consumers) {
			HopRewriteUtils.replaceChildReference(consumer, sharedInput, teeOp);
		}

		// Record that we've handled this hop
		handledHop.put(sharedInput.getHopID(), teeOp);
		rewrittenHops.add(sharedInput.getHopID());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created tee hop " + teeOp.getHopID() + " -> "
				+ teeOp.getName());
		}
	}

	public static boolean injectTeesForRecompiledDag(ArrayList<Hop> roots) {
		if(!DMLScript.USE_OOC || roots == null || roots.isEmpty())
			return false;

		Set<Hop> requiredTees = Collections.newSetFromMap(new IdentityHashMap<>());
		boolean changed = stripTees(roots, requiredTees);
		while(true) {
			List<Hop> shared = collectSharedHops(roots, true);
			Hop candidate = null;
			List<Hop> correlated = Collections.emptyList();
			for(Hop hop : shared) {
				correlated = correlatedConsumers(hop);
				if(!correlated.isEmpty()) {
					candidate = hop;
					break;
				}
			}
			if(candidate == null)
				break;
			shareRowsHop(candidate, correlated);
			changed = true;
		}
		for(Hop hop : requiredTees)
			if(!hop.getParent().isEmpty() && !isSharingHop(hop))
				teeSharedHop(hop);
		List<Hop> remainingShared = collectSharedHops(roots, false);
		for(Hop hop : remainingShared)
			teeSharedHop(hop);
		return changed || !remainingShared.isEmpty();
	}

	private static boolean stripTees(ArrayList<Hop> roots, Set<Hop> requiredTees) {
		List<Hop> tees = new ArrayList<>();
		Hop.resetVisitStatus(roots);
		for(Hop root : roots)
			collectTees(root, tees);
		Hop.resetVisitStatus(roots);
		for(Hop tee : tees) {
			Hop input = tee.getInput().get(0);
			requiredTees.add(input);
			for(Hop parent : new ArrayList<>(tee.getParent()))
				HopRewriteUtils.replaceChildReference(parent, tee, input);
			HopRewriteUtils.removeAllChildReferences(tee);
		}
		return !tees.isEmpty();
	}

	private static void collectTees(Hop hop, List<Hop> tees) {
		if(hop.isVisited())
			return;
		hop.setVisited(true);
		for(Hop input : hop.getInput())
			collectTees(input, tees);
		if(HopRewriteUtils.isData(hop, OpOpData.TEE))
			tees.add(hop);
	}

	private static List<Hop> collectSharedHops(ArrayList<Hop> roots, boolean excludeRowShared) {
		List<Hop> shared = new ArrayList<>();
		Hop.resetVisitStatus(roots);
		for(Hop root : roots)
			collectSharedHops(root, shared, excludeRowShared);
		Hop.resetVisitStatus(roots);
		return shared;
	}

	private static void collectSharedHops(Hop hop, List<Hop> shared, boolean excludeRowShared) {
		if(hop.isVisited())
			return;
		hop.setVisited(true);
		for(Hop input : hop.getInput())
			collectSharedHops(input, shared, excludeRowShared);
		if(hop.getDataType().isMatrix() && hop.getParent().size() > 1 && !isSharingHop(hop) &&
			(!excludeRowShared || !dependsOnSharedRows(hop, Collections.newSetFromMap(new IdentityHashMap<>()))))
			shared.add(hop);
	}

	private static boolean dependsOnSharedRows(Hop hop, Set<Hop> visited) {
		if(!visited.add(hop))
			return false;
		for(Hop input : hop.getInput())
			if(HopRewriteUtils.isData(input, OpOpData.SHAREDROWS) || dependsOnSharedRows(input, visited))
				return true;
		return false;
	}

	private static void shareRowsHop(Hop sharedInput, List<Hop> correlated) {
		DataOp sharedRows = new DataOp("shared_rows_" + sharedInput.getName(), sharedInput.getDataType(),
			sharedInput.getValueType(), OpOpData.SHAREDROWS, null, sharedInput.getDim1(), sharedInput.getDim2(),
			sharedInput.getNnz(), sharedInput.getBlocksize());
		HopRewriteUtils.addChildReference(sharedRows, sharedInput);
		for(Hop consumer : correlated)
			HopRewriteUtils.replaceChildReference(consumer, sharedInput, sharedRows);
	}

	private static List<Hop> correlatedConsumers(Hop shared) {
		Set<Hop> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		List<Hop> pending = new ArrayList<>(shared.getParent());
		while(!pending.isEmpty()) {
			Hop candidate = pending.remove(pending.size() - 1);
			if(!visited.add(candidate))
				continue;
			if(HopRewriteUtils.isMatrixMultiply(candidate) && candidate.getInput().size() == 2 &&
				dependsOn(candidate.getInput().get(0), shared, Collections.newSetFromMap(new IdentityHashMap<>())) &&
				dependsOn(candidate.getInput().get(1), shared, Collections.newSetFromMap(new IdentityHashMap<>()))) {
				List<Hop> consumers = new ArrayList<>(2);
				for(Hop input : candidate.getInput()) {
					Hop closest = null;
					int closestDistance = Integer.MAX_VALUE;
					for(Hop consumer : shared.getParent()) {
						if(consumer instanceof IndexingOp || HopRewriteUtils.isData(consumer, OpOpData.SHAREDROWS))
							continue;
						int distance = input == shared && consumer == candidate ? 0 : distanceTo(input, consumer,
							new IdentityHashMap<>());
						if(distance < closestDistance) {
							closest = consumer;
							closestDistance = distance;
						}
					}
					if(closest != null && !consumers.contains(closest))
						consumers.add(closest);
				}
				if(consumers.size() == 2)
					return consumers;
			}
			pending.addAll(candidate.getParent());
		}
		return Collections.emptyList();
	}

	private static int distanceTo(Hop current, Hop target, IdentityHashMap<Hop, Integer> memo) {
		if(current == target)
			return 0;
		Integer known = memo.get(current);
		if(known != null)
			return known;
		int distance = Integer.MAX_VALUE;
		memo.put(current, distance);
		for(Hop input : current.getInput()) {
			int childDistance = distanceTo(input, target, memo);
			if(childDistance != Integer.MAX_VALUE)
				distance = Math.min(distance, childDistance + 1);
		}
		memo.put(current, distance);
		return distance;
	}

	private static boolean dependsOn(Hop current, Hop target, Set<Hop> visited) {
		if(current == target)
			return true;
		if(!visited.add(current))
			return false;
		for(Hop input : current.getInput())
			if(dependsOn(input, target, visited))
				return true;
		return false;
	}

	private static boolean isSharingHop(Hop hop) {
		return HopRewriteUtils.isData(hop, OpOpData.TEE) || HopRewriteUtils.isData(hop, OpOpData.SHAREDROWS);
	}

	private static void teeSharedHop(Hop sharedInput) {
		ArrayList<Hop> consumers = new ArrayList<>(sharedInput.getParent());

		DataOp teeOp = new DataOp("tee_out_" + sharedInput.getName(), sharedInput.getDataType(),
			sharedInput.getValueType(), Types.OpOpData.TEE, null, sharedInput.getDim1(),
			sharedInput.getDim2(), sharedInput.getNnz(), sharedInput.getBlocksize());
		HopRewriteUtils.addChildReference(teeOp, sharedInput);

		for(Hop consumer : consumers)
			HopRewriteUtils.replaceChildReference(consumer, sharedInput, teeOp);

		if(LOG.isDebugEnabled())
			LOG.debug("Injected recompile-time tee " + teeOp.getHopID() + " for shared hop "
				+ sharedInput.getHopID() + " (" + sharedInput.getName() + "), consumers=" + consumers.size());
	}

	@SuppressWarnings("unused")
	private boolean isSelfTranposePattern (Hop hop) {
		boolean hasTransposeConsumer = false; // t(X)
		boolean hasMatrixMultiplyConsumer = false; // %*%

		for (Hop parent: hop.getParent()) {
			if (parent instanceof ReorgOp) {
				if (HopRewriteUtils.isTransposeOperation(parent)) {
					hasTransposeConsumer = true;
				}
			}
			else if (HopRewriteUtils.isMatrixMultiply(parent)) {
				hasMatrixMultiplyConsumer = true;
			}
		}
		return hasTransposeConsumer &&  hasMatrixMultiplyConsumer;
	}

	@Override
	public boolean createsSplitDag() {
		return false;
	}

	@Override
	public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {
		if (!DMLScript.USE_OOC)
			return List.of(sb);

		rewriteSB(sb, state);

		for (String tVar : teeTransientVars) {
			List<Hop> tHops = _transientHops.get(tVar);

			if (tHops == null)
				continue;

			for (Hop affectedHops : tHops) {
				applyTopDownTeeRewrite(affectedHops);
			}

			tHops.clear();
		}

		removeRedundantTeeChains(sb);

		return List.of(sb);
	}

	@Override
	public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
		if (!DMLScript.USE_OOC)
			return sbs;

		for (StatementBlock sb : sbs)
			rewriteSB(sb, state);

		for (String tVar : teeTransientVars) {
			List<Hop> tHops = _transientHops.get(tVar);

			if (tHops == null)
				continue;

			for (Hop affectedHops : tHops) {
				applyTopDownTeeRewrite(affectedHops);
			}
		}

		for (StatementBlock sb : sbs)
			removeRedundantTeeChains(sb);

		return sbs;
	}

	private void rewriteSB(StatementBlock sb, ProgramRewriteStatus state) {
		rewriteCandidates.clear();

		if (sb.getHops() != null) {
			for(Hop hop : sb.getHops()) {
				hop.resetVisitStatus();
				findRewriteCandidates(hop);
			}
		}
		Hop predicate = sb instanceof IfStatementBlock ? ((IfStatementBlock) sb)
			.getPredicateHops() : sb instanceof WhileStatementBlock ? ((WhileStatementBlock) sb)
				.getPredicateHops() : null;
		if(predicate != null) {
			predicate.resetVisitStatus();
			findRewriteCandidates(predicate);
		}

		for (Hop candidate : rewriteCandidates) {
			applyTopDownTeeRewrite(candidate);
		}
	}

	private void removeRedundantTeeChains(StatementBlock sb) {
		if (sb == null || sb.getHops() == null)
			return;

		Hop.resetVisitStatus(sb.getHops());
		for (Hop hop : sb.getHops())
			removeRedundantTeeChains(hop);
		Hop.resetVisitStatus(sb.getHops());
	}

	private void removeRedundantTeeChains(Hop hop) {
		if (hop.isVisited())
			return;

		ArrayList<Hop> inputs = new ArrayList<>(hop.getInput());
		for (Hop in : inputs)
			removeRedundantTeeChains(in);

		if (HopRewriteUtils.isData(hop, OpOpData.TEE) && hop.getInput().size() == 1) {
			Hop teeInput = hop.getInput().get(0);
			if (HopRewriteUtils.isData(teeInput, OpOpData.TEE)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Remove redundant tee hop " + hop.getHopID()
						+ " (" + hop.getName() + ") -> " + teeInput.getHopID()
						+ " (" + teeInput.getName() + ")");
				}
				HopRewriteUtils.rewireAllParentChildReferences(hop, teeInput);
				HopRewriteUtils.removeAllChildReferences(hop);
			}
		}

		hop.setVisited();
	}
}
