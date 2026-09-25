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

package org.apache.sysds.lops.rewrite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.lops.Data;
import org.apache.sysds.lops.Lop;
import org.apache.sysds.lops.Tee;
import org.apache.sysds.parser.DMLProgram;
import org.apache.sysds.parser.ForStatement;
import org.apache.sysds.parser.ForStatementBlock;
import org.apache.sysds.parser.FunctionDictionary;
import org.apache.sysds.parser.FunctionStatement;
import org.apache.sysds.parser.FunctionStatementBlock;
import org.apache.sysds.parser.IfStatement;
import org.apache.sysds.parser.IfStatementBlock;
import org.apache.sysds.parser.StatementBlock;
import org.apache.sysds.parser.WhileStatement;
import org.apache.sysds.parser.WhileStatementBlock;

/** Inserts OOC tees after physical LOP lowering, when shared inputs of TSMM and other fused operators are known. */
public final class RewriteInjectOOCTeeLop {
	private RewriteInjectOOCTeeLop() {
	}

	public static void rewriteProgram(DMLProgram program) {
		for(FunctionDictionary<FunctionStatementBlock> functions : program.getNamespaces().values()) {
			for(FunctionStatementBlock function : functions.getFunctions().values())
				rewriteScope(List.of(function));
			if(functions.getFunctions(false) != null)
				for(FunctionStatementBlock function : functions.getFunctions(false).values())
					rewriteScope(List.of(function));
		}
		rewriteScope(program.getStatementBlocks());
	}

	public static void rewriteRecompiled(StatementBlock block, List<Lop> roots) {
		Map<String, Integer> transientReads = new HashMap<>();
		Set<String> repeatedReads = new HashSet<>();
		if(block != null && block.getDMLProg() != null) {
			countTransientReads(collectProgram(block.getDMLProg()), transientReads);
			collectLoopReads(block.getDMLProg(), repeatedReads);
		}
		else {
			countTransientReads(collect(roots), transientReads);
			// A separately recompiled predicate may be evaluated repeatedly.
			if(block == null)
				collectTransientReadLabels(collect(roots), repeatedReads);
		}
		rewrite(collect(roots), transientReads, repeatedReads);
	}

	private static void rewriteScope(List<StatementBlock> blocks) {
		List<Lop> roots = new ArrayList<>();
		for(StatementBlock block : blocks)
			collectRoots(block, roots);
		roots.removeIf(root -> root == null);
		List<Lop> lops = collect(roots);
		Map<String, Integer> transientReads = new HashMap<>();
		countTransientReads(lops, transientReads);
		Set<String> repeatedReads = new HashSet<>();
		for(StatementBlock block : blocks)
			collectLoopReads(block, repeatedReads);
		rewrite(lops, transientReads, repeatedReads);
		// Predicates are not visited by the regular RewriteFixIDs statement-block pass.
		RewriteFixIDs.assignNewIDs(roots);
	}

	private static void collectLoopReads(DMLProgram program, Set<String> labels) {
		for(FunctionDictionary<FunctionStatementBlock> functions : program.getNamespaces().values()) {
			for(FunctionStatementBlock function : functions.getFunctions().values())
				collectLoopReads(function, labels);
			if(functions.getFunctions(false) != null)
				for(FunctionStatementBlock function : functions.getFunctions(false).values())
					collectLoopReads(function, labels);
		}
		for(StatementBlock block : program.getStatementBlocks())
			collectLoopReads(block, labels);
	}

	private static void collectLoopReads(StatementBlock block, Set<String> labels) {
		// One syntactic read in a loop body can still consume the same stream on every iteration.
		if(block instanceof ForStatementBlock || block instanceof WhileStatementBlock) {
			List<Lop> roots = new ArrayList<>();
			collectRoots(block, roots);
			collectTransientReadLabels(collect(roots), labels);
		}
		else if(block instanceof IfStatementBlock) {
			IfStatement statement = (IfStatement) block.getStatement(0);
			for(StatementBlock child : statement.getIfBody())
				collectLoopReads(child, labels);
			for(StatementBlock child : statement.getElseBody())
				collectLoopReads(child, labels);
		}
		else if(block instanceof FunctionStatementBlock)
			for(StatementBlock child : ((FunctionStatement) block.getStatement(0)).getBody())
				collectLoopReads(child, labels);
	}

	private static void collectTransientReadLabels(List<Lop> lops, Set<String> labels) {
		for(Lop lop : lops)
			if(lop instanceof Data && ((Data) lop).isTransientRead() && lop.getDataType() == DataType.MATRIX)
				labels.add(lop.getOutputParameters().getLabel());
	}

	private static List<Lop> collectProgram(DMLProgram program) {
		List<Lop> roots = new ArrayList<>();
		for(FunctionDictionary<FunctionStatementBlock> functions : program.getNamespaces().values()) {
			for(FunctionStatementBlock function : functions.getFunctions().values())
				collectRoots(function, roots);
			if(functions.getFunctions(false) != null)
				for(FunctionStatementBlock function : functions.getFunctions(false).values())
					collectRoots(function, roots);
		}
		for(StatementBlock block : program.getStatementBlocks())
			collectRoots(block, roots);
		return collect(roots);
	}

	private static void collectRoots(StatementBlock block, List<Lop> roots) {
		if(block.getLops() != null)
			roots.addAll(block.getLops());
		if(block instanceof WhileStatementBlock) {
			WhileStatementBlock whileBlock = (WhileStatementBlock) block;
			roots.add(whileBlock.getPredicateLops());
			for(StatementBlock child : ((WhileStatement) block.getStatement(0)).getBody())
				collectRoots(child, roots);
		}
		else if(block instanceof IfStatementBlock) {
			IfStatementBlock ifBlock = (IfStatementBlock) block;
			roots.add(ifBlock.getPredicateLops());
			IfStatement statement = (IfStatement) block.getStatement(0);
			for(StatementBlock child : statement.getIfBody())
				collectRoots(child, roots);
			for(StatementBlock child : statement.getElseBody())
				collectRoots(child, roots);
		}
		else if(block instanceof ForStatementBlock) {
			ForStatementBlock forBlock = (ForStatementBlock) block;
			roots.add(forBlock.getFromLops());
			roots.add(forBlock.getToLops());
			roots.add(forBlock.getIncrementLops());
			for(StatementBlock child : ((ForStatement) block.getStatement(0)).getBody())
				collectRoots(child, roots);
		}
		else if(block instanceof FunctionStatementBlock)
			for(StatementBlock child : ((FunctionStatement) block.getStatement(0)).getBody())
				collectRoots(child, roots);
	}

	private static List<Lop> collect(List<Lop> roots) {
		List<Lop> lops = new ArrayList<>();
		Set<Lop> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		for(Lop root : roots)
			collect(root, visited, lops);
		return lops;
	}

	private static void collect(Lop lop, Set<Lop> visited, List<Lop> lops) {
		if(lop == null || !visited.add(lop))
			return;
		for(Lop input : lop.getInputs())
			collect(input, visited, lops);
		lops.add(lop);
	}

	private static void countTransientReads(List<Lop> lops, Map<String, Integer> counts) {
		for(Lop lop : lops)
			if(lop instanceof Data && ((Data) lop).isTransientRead() && lop.getDataType() == DataType.MATRIX)
				counts.merge(lop.getOutputParameters().getLabel(), 1, Integer::sum);
	}

	private static void rewrite(List<Lop> lops, Map<String, Integer> transientReads, Set<String> repeatedReads) {
		for(Lop lop : lops) {
			if(lop.getDataType() != DataType.MATRIX || lop instanceof Tee || lop.getOutputs().isEmpty())
				continue;
			boolean sharedTransient = lop instanceof Data && ((Data) lop).isTransientRead()
				&& (transientReads.getOrDefault(lop.getOutputParameters().getLabel(), 0) > 1
					|| repeatedReads.contains(lop.getOutputParameters().getLabel()));
			if(lop.getOutputs().size() < 2 && !sharedTransient)
				continue;
			List<Lop> consumers = new ArrayList<>(lop.getOutputs());
			Tee tee = new Tee(lop, lop.getDataType(), lop.getValueType());
			tee.getOutputParameters().setDimensions(lop.getOutputParameters().getNumRows(),
				lop.getOutputParameters().getNumCols(), lop.getOutputParameters().getBlocksize(),
				lop.getOutputParameters().getNnz(), lop.getOutputParameters().getUpdateType(),
				lop.getOutputParameters().getCompressedSize());
			for(Lop consumer : consumers) {
				tee.addOutput(consumer);
				consumer.replaceInput(lop, tee);
				lop.removeOutput(consumer);
			}
		}
	}
}
