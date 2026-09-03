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

package org.apache.sysds.runtime.instructions.ooc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.hops.recompile.Recompiler;
import org.apache.sysds.runtime.controlprogram.BasicProgramBlock;
import org.apache.sysds.runtime.controlprogram.LocalVariableMap;
import org.apache.sysds.runtime.controlprogram.Program;
import org.apache.sysds.runtime.controlprogram.caching.CacheableData;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.ooc.CachingStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.ooc.OOCStreamable;
import org.apache.sysds.runtime.instructions.ooc.TeeOOCInstruction;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;

public class LazyOOCInstruction extends Instruction {
	private static final AtomicLong ID = new AtomicLong();
	private final ArrayList<Hop> _templates;
	private final Set<String> _liveOut;

	public LazyOOCInstruction(List<Hop> roots) {
		this(roots, Collections.emptySet());
	}

	public LazyOOCInstruction(List<Hop> roots, Collection<String> liveOut) {
		super(null);
		this._templates = Recompiler.deepCopyHopsDag(roots);
		this._liveOut = new HashSet<>(liveOut);
		this.instOpcode = "lazyooc";
		this.instString = "lazyooc";
	}

	public LazyOOCInstruction copy() {
		LazyOOCInstruction copy = new LazyOOCInstruction(this._templates, this._liveOut);
		copy.setLocation(this);
		return copy;
	}

	public static boolean supports(List<Hop> roots) {
		return !roots.isEmpty() && roots.stream().allMatch(LazyOOCInstruction::isTransientOutput) &&
			roots.stream().anyMatch(root -> LazyOOCInstruction.containsOOC(root, new IdentityHashMap<Hop, Boolean>()));
	}

	private static boolean isTransientOutput(Hop root) {
		return root instanceof DataOp && ((DataOp) root).getOp() == Types.OpOpData.TRANSIENTWRITE &&
			(root.getDataType() == Types.DataType.MATRIX || root.getDataType() == Types.DataType.SCALAR);
	}

	private static boolean containsOOC(Hop hop, IdentityHashMap<Hop, Boolean> memo) {
		if(memo.put(hop, Boolean.TRUE) != null) {
			return false;
		}
		if(hop.getExecType() == Types.ExecType.OOC) {
			return true;
		}
		for(Hop input : hop.getInput()) {
			if(!LazyOOCInstruction.containsOOC(input, memo))
				continue;
			return true;
		}
		return false;
	}

	@Override
	public Instruction.IType getType() {
		return Instruction.IType.OUT_OF_CORE;
	}

	@Override
	public String getGraphString() {
		return this.getOpcode();
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		ArrayList<Hop> roots = Recompiler.deepCopyHopsDag(this._templates);
		Set<String> rootOutputs = new HashSet<>();
		roots.forEach(root -> rootOutputs.add(root.getName()));
		Set<String> immediateInputs = new HashSet<>();
		for(Hop root : roots)
			if(root.getDataType() == Types.DataType.SCALAR)
				collectTransientReads(root, immediateInputs, new IdentityHashMap<>());
		LinkedHashMap<String, Data> bindings = new LinkedHashMap<String, Data>();
		IdentityHashMap<Hop, Hop> memo = new IdentityHashMap<Hop, Hop>();
		IdentityHashMap<Plan, Map<String, Hop>> expanded = new IdentityHashMap<Plan, Map<String, Hop>>();
		IdentityHashMap<Plan, Map<String, Hop>> expandedRoots = new IdentityHashMap<Plan, Map<String, Hop>>();
		IdentityHashMap<Plan, List<String>> expandedOutputs = new IdentityHashMap<Plan, List<String>>();
		Map<String, Hop> retainedOutputs = new LinkedHashMap<>();
		for(int i = 0; i < roots.size(); ++i) {
			roots.set(i, bind(roots.get(i), ec, bindings, memo, expanded, expandedRoots, expandedOutputs,
				retainedOutputs, rootOutputs, immediateInputs));
		}
		roots.addAll(retainedOutputs.values());
		IdentityHashMap<OOCStreamable<IndexedMatrixValue>, Hop> streamReads = new IdentityHashMap<>();
		IdentityHashMap<Hop, Hop> coalesced = new IdentityHashMap<>();
		for(int i = 0; i < roots.size(); i++)
			roots.set(i, coalesceStreamReads(roots.get(i), bindings, streamReads, coalesced));
		LinkedHashMap<String, Hop> outputs = new LinkedHashMap<String, Hop>();
		LinkedHashMap<String, String> scalarOutputs = new LinkedHashMap<>();
		for(Hop hop : roots) {
			String string = hop.getName();
			String internal = "__lazy_ooc_" + ID.incrementAndGet();
			hop.setName(internal);
			if(hop.getDataType() == Types.DataType.MATRIX) {
				outputs.put(string, hop);
				continue;
			}
			scalarOutputs.put(string, internal);
		}
		Plan plan = new Plan(roots, bindings, ec.getProgram());
		for(Map.Entry<Plan, List<String>> entry : expandedOutputs.entrySet()) {
			entry.getKey().releaseExpandedReservations(entry.getValue());
		}
		for(Map.Entry entry : outputs.entrySet()) {
			Hop root = (Hop) entry.getValue();
			MatrixCharacteristics dc = new MatrixCharacteristics(root.getDim1(), root.getDim2(), root.getBlocksize(),
				root.getNnz());
			plan._characteristics.put(root.getName(), dc);
			MatrixObject matrix = new MatrixObject(root.getValueType(), OptimizerUtils.getUniqueTempFileName(),
				new MetaDataFormat(dc, Types.FileFormat.BINARY));
			Data previous = ec.removeVariable((String) entry.getKey());
			ec.setVariable((String) entry.getKey(), matrix);
			TeeOOCInstruction.releaseRef(ec, previous);
			ec.cleanupDataObject(previous);
			LazyStream stream = new LazyStream(plan, root.getName(), dc);
			matrix.setStreamHandle(stream);
		}
		LinkedHashSet<String> linkedHashSet = new LinkedHashSet<String>(scalarOutputs.values());
		linkedHashSet.addAll(plan._eagerOutputs);
		plan.compile(linkedHashSet);
		for(Map.Entry output : scalarOutputs.entrySet()) {
			ec.setVariable((String) output.getKey(), plan.takeScalar((String) output.getValue()));
		}
	}

	private Hop bind(Hop hop, ExecutionContext ec, Map<String, Data> bindings, IdentityHashMap<Hop, Hop> memo,
		IdentityHashMap<Plan, Map<String, Hop>> expanded, IdentityHashMap<Plan, Map<String, Hop>> expandedRoots,
		IdentityHashMap<Plan, List<String>> expandedOutputs, Map<String, Hop> retainedOutputs, Set<String> rootOutputs,
		Set<String> immediateInputs) {
		Hop known = memo.get(hop);
		if(known != null) {
			return known;
		}
		if(hop instanceof DataOp && ((DataOp) hop).getOp() == Types.OpOpData.TRANSIENTREAD) {
			Data value = ec.getVariable(hop.getName());
			if(value instanceof MatrixObject) {
				MatrixObject matrix = (MatrixObject) value;
				OOCStreamable<IndexedMatrixValue> stream = matrix.getStreamable();
				if(stream instanceof LazyStream &&
					!((LazyStream) stream)._plan.isCompiled(((LazyStream) stream)._output)) {
					LazyStream lazy = (LazyStream) stream;
					Map<String, Hop> priorOutputs = expanded.get(lazy._plan);
					if(priorOutputs == null) {
						priorOutputs = new LinkedHashMap<String, Hop>();
						Map<String, Hop> priorRoots = new LinkedHashMap<>();
						for(Hop root : Recompiler.deepCopyHopsDag(lazy._plan._roots)) {
							Hop expression = root.getInput(0);
							expression.getParent().remove(root);
							priorOutputs.put(root.getName(), expression);
							priorRoots.put(root.getName(), root);
						}
						expanded.put(lazy._plan, priorOutputs);
						expandedRoots.put(lazy._plan, priorRoots);
						bindings.putAll(lazy._plan._bindings);
					}
					expandedOutputs.computeIfAbsent(lazy._plan, key -> new ArrayList()).add(lazy._output);
					Hop replacement = priorOutputs.get(lazy._output);
					if(immediateInputs.contains(hop.getName()) && _liveOut.contains(hop.getName()) &&
						!rootOutputs.contains(hop.getName()) && !retainedOutputs.containsKey(hop.getName())) {
						Hop retained = expandedRoots.get(lazy._plan).get(lazy._output);
						retained.setName(hop.getName());
						((DataOp) retained).setFileName(hop.getName());
						replacement.getParent().add(retained);
						retainedOutputs.put(hop.getName(), retained);
					}
					memo.put(hop, replacement);
					return replacement;
				}
				if(matrix.hasStreamHandle()) {
					MatrixObject captured = new MatrixObject(matrix);
					captured.setStreamHandle(new ReservedStreamable(stream));
					value = captured;
				}
			}
			String alias = "__lazy_ooc_" + ID.incrementAndGet();
			hop.setName(alias);
			((DataOp) hop).setFileName(alias);
			bindings.put(alias, value);
		}
		memo.put(hop, hop);
		for(int i = 0; i < hop.getInput().size(); ++i) {
			Hop replacement;
			Hop old = hop.getInput().get(i);
			if(old == (replacement = bind(old, ec, bindings, memo, expanded, expandedRoots, expandedOutputs,
				retainedOutputs, rootOutputs, immediateInputs)))
				continue;
			old.getParent().remove(hop);
			hop.getInput().set(i, replacement);
			if(replacement.getParent().contains(hop))
				continue;
			replacement.getParent().add(hop);
		}
		return hop;
	}

	private static Hop coalesceStreamReads(Hop hop, Map<String, Data> bindings,
		IdentityHashMap<OOCStreamable<IndexedMatrixValue>, Hop> streamReads, IdentityHashMap<Hop, Hop> memo) {
		Hop known = memo.get(hop);
		if(known != null)
			return known;
		for(int i = 0; i < hop.getInput().size(); i++) {
			Hop old = hop.getInput().get(i);
			Hop replacement = coalesceStreamReads(old, bindings, streamReads, memo);
			if(old == replacement)
				continue;
			old.getParent().remove(hop);
			hop.getInput().set(i, replacement);
			if(!replacement.getParent().contains(hop))
				replacement.getParent().add(hop);
		}
		Hop replacement = hop;
		if(hop instanceof DataOp && ((DataOp) hop).getOp() == Types.OpOpData.TRANSIENTREAD &&
			bindings.get(hop.getName()) instanceof MatrixObject) {
			MatrixObject matrix = (MatrixObject) bindings.get(hop.getName());
			if(matrix.hasStreamHandle()) {
				OOCStreamable<IndexedMatrixValue> stream = matrix.getStreamable();
				while(stream instanceof ReservedStreamable)
					stream = ((ReservedStreamable) stream)._source;
				Hop shared = streamReads.putIfAbsent(stream, hop);
				if(shared != null)
					replacement = shared;
			}
		}
		memo.put(hop, replacement);
		return replacement;
	}

	private static void collectTransientReads(Hop hop, Set<String> inputs, IdentityHashMap<Hop, Boolean> visited) {
		if(visited.put(hop, Boolean.TRUE) != null)
			return;
		if(hop instanceof DataOp && ((DataOp) hop).getOp() == Types.OpOpData.TRANSIENTREAD)
			inputs.add(hop.getName());
		for(Hop input : hop.getInput())
			collectTransientReads(input, inputs, visited);
	}

	private static final class Plan {
		private final ArrayList<Hop> _roots;
		private final Map<String, Data> _bindings;
		private final Program _program;
		private final Map<String, List<Hop>> _groups = new LinkedHashMap<String, List<Hop>>();
		private final IdentityHashMap<List<Hop>, List<OOCStreamable<?>>> _reservations = new IdentityHashMap();
		private final Set<List<Hop>> _releasedGroups = Collections.newSetFromMap(new IdentityHashMap());
		private final Set<String> _compiled = new HashSet<String>();
		private final Set<String> _cancelled = new HashSet<String>();
		private final Set<String> _eagerOutputs = new HashSet<String>();
		private final Set<String> _releasedOutputs = new HashSet<String>();
		private final Map<String, DataCharacteristics> _characteristics = new LinkedHashMap<String, DataCharacteristics>();
		private final Map<String, OOCStreamable<IndexedMatrixValue>> _outputs = new LinkedHashMap<String, OOCStreamable<IndexedMatrixValue>>();
		private final Map<String, Data> _scalarOutputs = new LinkedHashMap<String, Data>();

		private Plan(ArrayList<Hop> roots, Map<String, Data> bindings, Program program) {
			this._roots = roots;
			this._bindings = bindings;
			this._program = program;
			int[] parents = new int[roots.size()];
			for(int i = 0; i < parents.length; ++i) {
				parents[i] = i;
			}
			IdentityHashMap<Hop, Integer> owners = new IdentityHashMap<Hop, Integer>();
			for(int i = 0; i < roots.size(); ++i) {
				Plan.collectRootOwners(roots.get(i), i, parents, owners, new IdentityHashMap<Hop, Boolean>());
			}
			LinkedHashMap<Integer, List<Hop>> groups = new LinkedHashMap<>();
			for(int i = 0; i < roots.size(); ++i) {
				groups.computeIfAbsent(Plan.find(parents, i), key -> new ArrayList()).add(roots.get(i));
			}
			for(List<Hop> group : groups.values()) {
				ArrayList<OOCStreamable<?>> reservations = new ArrayList<>();
				boolean eager = false;
				for(Hop root2 : group) {
					this._groups.put(root2.getName(), group);
					eager |= Plan.collectReservations(root2, bindings, reservations,
						new IdentityHashMap<Hop, Boolean>());
				}
				if(eager) {
					group.forEach(root -> this._eagerOutputs.add(root.getName()));
				}
				reservations.forEach(OOCStreamable::reserveLazyHandle);
				this._reservations.put(group, reservations);
			}
		}

		private synchronized OOCStream<IndexedMatrixValue> read(String output) {
			this.compile(List.of(output));
			return this._outputs.get(output).getReservedReadStream();
		}

		private synchronized OOCStream<IndexedMatrixValue> readReserved(String output) {
			this.compile(List.of(output));
			return this._outputs.get(output).getReservedReadStream();
		}

		private synchronized Data takeScalar(String output) {
			return this._scalarOutputs.remove(output);
		}

		private synchronized void compile(Collection<String> requested) {
			Set<List<Hop>> selectedGroups = Collections.newSetFromMap(new IdentityHashMap<>());
			for(String string : requested) {
				List<Hop> group = this._groups.get(string);
				if(group == null || this._compiled.contains(string) || this._cancelled.contains(string))
					continue;
				selectedGroups.add(group);
			}
			if(selectedGroups.isEmpty()) {
				return;
			}
			ArrayList<Hop> selected = new ArrayList<Hop>();
			for(List<Hop> group : selectedGroups) {
				for(Hop root : group) {
					if(this._compiled.contains(root.getName()))
						continue;
					selected.add(root);
				}
			}
			LocalVariableMap localVariableMap = new LocalVariableMap();
			this._bindings.forEach(localVariableMap::put);
			ExecutionContext ec = new ExecutionContext(localVariableMap);
			ec.setProgram(this._program);
			BasicProgramBlock block = new BasicProgramBlock(this._program);
			block.setInstructions(Recompiler.recompileHopsDag(null, selected, localVariableMap, null, false, true, 0L));
			try {
				block.execute(ec);
				for(Hop root : selected) {
					String name = root.getName();
					if(root.getDataType() == Types.DataType.MATRIX && !this._cancelled.contains(name)) {
						MatrixObject matrix = ec.getMatrixObject(name);
						this._characteristics.get(name).set(matrix.getDataCharacteristics());
						OOCStreamable<IndexedMatrixValue> stream = matrix.getOrCreateStreamHandle(source -> {
							MaterializedStoreStreamable created = new MaterializedStoreStreamable(
								(OOCStream<IndexedMatrixValue>) source, matrix);
							TeeOOCInstruction.incrRef(created, 1);
							return created;
						});
						TeeOOCInstruction.incrRef(stream, 1);
						stream.reserveLazyHandle();
						this._outputs.put(name, stream);
					}
					else if(!this._cancelled.contains(name)) {
						this._scalarOutputs.put(name, ec.getVariable(name));
					}
					this._compiled.add(name);
				}
			}
			finally {
				selectedGroups.forEach(this::releaseReservations);
			}
			for(Hop root : selected) {
				Data removed = ec.removeVariable(root.getName());
				TeeOOCInstruction.releaseRef(ec, removed);
			}
		}

		private void releaseReservations(List<Hop> group) {
			if(!this._releasedGroups.add(group)) {
				return;
			}
			this._reservations.get(group).forEach(OOCStreamable::discardHandle);
		}

		private synchronized boolean isCompiled(String output) {
			return this._compiled.contains(output);
		}

		private synchronized OOCStreamable<IndexedMatrixValue> getOutput(String output) {
			return this._outputs.get(output);
		}

		private synchronized void releaseOutput(String output) {
			OOCStreamable<IndexedMatrixValue> stream = this._outputs.get(output);
			if(stream != null && this._releasedOutputs.add(output)) {
				TeeOOCInstruction.incrRef(stream, -1);
			}
		}

		private synchronized void cancel(String output) {
			if(this._compiled.contains(output) || !this._cancelled.add(output)) {
				return;
			}
			List<Hop> group = this._groups.get(output);
			if(group != null && group.stream().allMatch(
				root -> this._compiled.contains(root.getName()) || this._cancelled.contains(root.getName()))) {
				this.releaseReservations(group);
			}
		}

		private synchronized void releaseExpandedReservations(List<String> outputs) {
			HashSet<String> expanded = new HashSet<String>(outputs);
			for(List<Hop> list : new LinkedHashSet<>(this._groups.values())) {
				if(!list.stream()
					.filter(
						root -> !this._compiled.contains(root.getName()) && !this._cancelled.contains(root.getName()))
					.allMatch(root -> expanded.contains(root.getName())))
					continue;
				this.releaseReservations(list);
			}
		}

		private static void collectRootOwners(Hop hop, int root, int[] parents, IdentityHashMap<Hop, Integer> owners,
			IdentityHashMap<Hop, Boolean> visited) {
			Integer owner;
			if(visited.put(hop, Boolean.TRUE) != null) {
				return;
			}
			Integer n = owner = hop instanceof LiteralOp ? null : owners.putIfAbsent(hop, root);
			if(owner != null) {
				Plan.union(parents, root, owner);
			}
			for(Hop input : hop.getInput()) {
				Plan.collectRootOwners(input, root, parents, owners, visited);
			}
		}

		private static int find(int[] parents, int root) {
			while(parents[root] != root) {
				parents[root] = parents[parents[root]];
				root = parents[root];
			}
			return root;
		}

		private static void union(int[] parents, int left, int right) {
			int rightRoot;
			int leftRoot = Plan.find(parents, left);
			if(leftRoot != (rightRoot = Plan.find(parents, right))) {
				parents[rightRoot] = leftRoot;
			}
		}

		private static boolean collectReservations(Hop hop, Map<String, Data> bindings,
			List<OOCStreamable<?>> reservations, IdentityHashMap<Hop, Boolean> visited) {
			Data value;
			if(visited.put(hop, Boolean.TRUE) != null) {
				return false;
			}
			boolean unreserved = false;
			if(hop instanceof DataOp && ((DataOp) hop).getOp() == Types.OpOpData.TRANSIENTREAD &&
				(value = bindings.get(hop.getName())) instanceof MatrixObject) {
				OOCStreamable<IndexedMatrixValue> stream = ((MatrixObject) value).getStreamable();
				if(stream instanceof ReservedStreamable) {
					reservations.add(stream);
				}
				else {
					unreserved = true;
				}
			}
			for(Hop input : hop.getInput()) {
				unreserved |= Plan.collectReservations(input, bindings, reservations, visited);
			}
			return unreserved;
		}
	}

	private static final class LazyStream implements OOCStreamable<IndexedMatrixValue> {
		private final Plan _plan;
		private final String _output;
		private final DataCharacteristics _characteristics;
		private CacheableData<?> _data;

		private LazyStream(Plan plan, String output, DataCharacteristics characteristics) {
			this._plan = plan;
			this._output = output;
			this._characteristics = characteristics;
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReadStream() {
			return this._plan.read(this._output);
		}

		@Override
		public OOCStream<IndexedMatrixValue> getWriteStream() {
			return this.getReadStream();
		}

		@Override
		public boolean hasStreamCache() {
			OOCStreamable<IndexedMatrixValue> output = this._plan.getOutput(this._output);
			return output != null && output.hasStreamCache();
		}

		@Override
		public CachingStream getStreamCache() {
			OOCStreamable<IndexedMatrixValue> output = this._plan.getOutput(this._output);
			return output == null ? null : output.getStreamCache();
		}

		@Override
		public boolean hasMaterializedStore() {
			OOCStreamable<IndexedMatrixValue> output = this._plan.getOutput(this._output);
			return output != null && output.hasMaterializedStore();
		}

		@Override
		public void scheduleMaterializedStoreDeletion() {
			this._plan.releaseOutput(this._output);
		}

		@Override
		public boolean isProcessed() {
			return this._plan.isCompiled(this._output);
		}

		@Override
		public DataCharacteristics getDataCharacteristics() {
			return this._characteristics;
		}

		@Override
		public CacheableData<?> getData() {
			return this._data;
		}

		@Override
		public void setData(CacheableData<?> data) {
			this._data = data;
		}

		@Override
		public OOCPrimitive getPrimitive() {
			OOCStreamable<IndexedMatrixValue> output = this._plan.getOutput(this._output);
			return output == null ? null : output.getPrimitive();
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReservedReadStream() {
			return this._plan.readReserved(this._output);
		}

		@Override
		public void reserveLazyHandle() {
			OOCStreamable<IndexedMatrixValue> output = this._plan.getOutput(this._output);
			if(output != null) {
				output.reserveLazyHandle();
			}
		}

		@Override
		public void discardHandle() {
			OOCStreamable<IndexedMatrixValue> output = this._plan.getOutput(this._output);
			if(output != null) {
				output.discardHandle();
			}
			else {
				this._plan.cancel(this._output);
			}
		}
	}

	private static final class ReservedStreamable implements OOCStreamable<IndexedMatrixValue> {
		private final OOCStreamable<IndexedMatrixValue> _source;
		private CacheableData<?> _data;

		private ReservedStreamable(OOCStreamable<IndexedMatrixValue> source) {
			this._source = source;
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReadStream() {
			return this._source.getReservedReadStream();
		}

		@Override
		public OOCStream<IndexedMatrixValue> getWriteStream() {
			return this.getReadStream();
		}

		@Override
		public boolean hasStreamCache() {
			return this._source.hasStreamCache();
		}

		@Override
		public CachingStream getStreamCache() {
			return this._source.getStreamCache();
		}

		@Override
		public boolean hasMaterializedStore() {
			return this._source.hasMaterializedStore();
		}

		@Override
		public void scheduleMaterializedStoreDeletion() {
			this._source.scheduleMaterializedStoreDeletion();
		}

		@Override
		public boolean isProcessed() {
			return this._source.isProcessed();
		}

		@Override
		public DataCharacteristics getDataCharacteristics() {
			return this._source.getDataCharacteristics();
		}

		@Override
		public CacheableData<?> getData() {
			return this._data;
		}

		@Override
		public void setData(CacheableData<?> data) {
			this._data = data;
		}

		@Override
		public OOCPrimitive getPrimitive() {
			return this._source.getPrimitive();
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReservedReadStream() {
			return this._source.getReservedReadStream();
		}

		@Override
		public void reserveLazyHandle() {
			this._source.reserveLazyHandle();
		}

		@Override
		public void discardHandle() {
			this._source.discardHandle();
		}
	}
}
