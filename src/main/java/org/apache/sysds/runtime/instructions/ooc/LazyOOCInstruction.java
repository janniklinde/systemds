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
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sysds.common.Types.DataType;
import org.apache.sysds.common.Types.ExecType;
import org.apache.sysds.common.Types.FileFormat;
import org.apache.sysds.common.Types.OpOpData;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
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
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.primitives.OOCPrimitive;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;

public class LazyOOCInstruction extends Instruction {
	private static final AtomicLong ID = new AtomicLong();
	private final ArrayList<Hop> _templates;

	public LazyOOCInstruction(List<Hop> roots) {
		super(null);
		_templates = Recompiler.deepCopyHopsDag(roots);
		instOpcode = "lazyooc";
		instString = "lazyooc";
	}

	public LazyOOCInstruction copy() {
		LazyOOCInstruction copy = new LazyOOCInstruction(_templates);
		copy.setLocation(this);
		return copy;
	}

	public static boolean supports(List<Hop> roots) {
		return !roots.isEmpty() && roots.stream().allMatch(LazyOOCInstruction::isTransientOutput)
			&& roots.stream().anyMatch(root -> containsOOC(root, new IdentityHashMap<>()));
	}

	private static boolean isTransientOutput(Hop root) {
		return root instanceof DataOp && ((DataOp) root).getOp() == OpOpData.TRANSIENTWRITE
			&& (root.getDataType() == DataType.MATRIX || root.getDataType() == DataType.SCALAR);
	}

	private static boolean containsOOC(Hop hop, IdentityHashMap<Hop, Boolean> memo) {
		if(memo.put(hop, Boolean.TRUE) != null)
			return false;
		if(hop.getExecType() == ExecType.OOC)
			return true;
		for(Hop input : hop.getInput())
			if(containsOOC(input, memo))
				return true;
		return false;
	}

	@Override
	public IType getType() {
		return IType.OUT_OF_CORE;
	}

	@Override
	public String getGraphString() {
		return getOpcode();
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		ArrayList<Hop> roots = Recompiler.deepCopyHopsDag(_templates);
		Map<String, Data> bindings = new LinkedHashMap<>();
		IdentityHashMap<Hop, Hop> memo = new IdentityHashMap<>();
		IdentityHashMap<Plan, Map<String, Hop>> expanded = new IdentityHashMap<>();
		IdentityHashMap<OOCStreamable<?>, Boolean> reservations = new IdentityHashMap<>();
		for(int i = 0; i < roots.size(); i++)
			roots.set(i, bind(roots.get(i), ec, bindings, memo, expanded, reservations));

		Map<String, Hop> outputs = new LinkedHashMap<>();
		Map<String, String> scalarOutputs = new LinkedHashMap<>();
		for(Hop root : roots) {
			String output = root.getName();
			String internal = "__lazy_ooc_" + ID.incrementAndGet();
			root.setName(internal);
			if(root.getDataType() == DataType.MATRIX)
				outputs.put(output, root);
			else
				scalarOutputs.put(output, internal);
		}

		Plan plan = new Plan(roots, bindings, ec.getProgram(), new ArrayList<>(reservations.keySet()));
		for(Map.Entry<String, Hop> output : outputs.entrySet()) {
			Hop root = output.getValue();
			DataCharacteristics dc = new MatrixCharacteristics(root.getDim1(), root.getDim2(), root.getBlocksize(),
				root.getNnz());
			MatrixObject matrix = new MatrixObject(root.getValueType(), OptimizerUtils.getUniqueTempFileName(),
				new MetaDataFormat(dc, FileFormat.BINARY));
			ec.setVariable(output.getKey(), matrix);
			LazyStream stream = new LazyStream(plan, root.getName(), dc);
			matrix.setStreamHandle(stream);
		}
		for(Map.Entry<String, String> output : scalarOutputs.entrySet())
			ec.setVariable(output.getKey(), plan.readScalar(output.getValue()));
	}

	private static Hop bind(Hop hop, ExecutionContext ec,
		Map<String, Data> bindings, IdentityHashMap<Hop, Hop> memo,
		IdentityHashMap<Plan, Map<String, Hop>> expanded,
		IdentityHashMap<OOCStreamable<?>, Boolean> reservations) {
		Hop known = memo.get(hop);
		if(known != null)
			return known;
		if(hop instanceof DataOp && ((DataOp) hop).getOp() == OpOpData.TRANSIENTREAD) {
			Data value = ec.getVariable(hop.getName());
			if(value instanceof MatrixObject) {
				MatrixObject matrix = (MatrixObject) value;
				OOCStreamable<IndexedMatrixValue> stream = matrix.getStreamable();
				if(stream instanceof LazyStream && !((LazyStream) stream)._plan.isCompiled()) {
					LazyStream lazy = (LazyStream) stream;
					Map<String, Hop> priorOutputs = expanded.get(lazy._plan);
					if(priorOutputs == null) {
						priorOutputs = new LinkedHashMap<>();
						for(Hop root : Recompiler.deepCopyHopsDag(lazy._plan._roots)) {
							Hop expression = root.getInput(0);
							expression.getParent().remove(root);
							priorOutputs.put(root.getName(), expression);
						}
						expanded.put(lazy._plan, priorOutputs);
						bindings.putAll(lazy._plan._bindings);
						lazy._plan.transferReservationsTo(reservations);
					}
					Hop replacement = priorOutputs.get(lazy._output);
					memo.put(hop, replacement);
					return replacement;
				}
				if(matrix.hasStreamHandle()) {
					if(reservations.put(stream, Boolean.TRUE) == null)
						stream.reserveLazyHandle();
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
		for(int i = 0; i < hop.getInput().size(); i++) {
			Hop old = hop.getInput().get(i);
			Hop replacement = bind(old, ec, bindings, memo, expanded, reservations);
			if(old != replacement) {
				old.getParent().remove(hop);
				hop.getInput().set(i, replacement);
				if(!replacement.getParent().contains(hop))
					replacement.getParent().add(hop);
			}
		}
		return hop;
	}

	private static final class Plan {
		private final ArrayList<Hop> _roots;
		private final Map<String, Data> _bindings;
		private final Program _program;
		private final List<OOCStreamable<?>> _reservations;
		private Map<String, OOCStreamable<IndexedMatrixValue>> _outputs;
		private Map<String, Data> _scalarOutputs;
		private boolean _released;

		private Plan(ArrayList<Hop> roots, Map<String, Data> bindings, Program program,
			List<OOCStreamable<?>> reservations) {
			_roots = roots;
			_bindings = bindings;
			_program = program;
			_reservations = reservations;
		}

		private synchronized OOCStream<IndexedMatrixValue> read(String output) {
			compile();
			return _outputs.get(output).getReservedReadStream();
		}

		private synchronized OOCStream<IndexedMatrixValue> readReserved(String output) {
			compile();
			return _outputs.get(output).getReservedReadStream();
		}

		private synchronized Data readScalar(String output) {
			compile();
			return _scalarOutputs.remove(output);
		}

		private void compile() {
			if(_outputs == null) {
				LocalVariableMap variables = new LocalVariableMap();
				_bindings.forEach(variables::put);
				ExecutionContext ec = new ExecutionContext(variables);
				ec.setProgram(_program);
				BasicProgramBlock block = new BasicProgramBlock(_program);
				block.setInstructions(Recompiler.recompileHopsDag(null, _roots, variables, null, false, true, 0));
				try {
					block.execute(ec);
					_outputs = new LinkedHashMap<>();
					_scalarOutputs = new LinkedHashMap<>();
					for(Hop root : _roots) {
						String name = root.getName();
						if(root.getDataType() == DataType.MATRIX) {
							MatrixObject matrix = ec.getMatrixObject(name);
							OOCStreamable<IndexedMatrixValue> stream = matrix.getOrCreateStreamHandle(source -> {
								OOCStreamable<IndexedMatrixValue> created =
									new MaterializedStoreStreamable(source, matrix);
								TeeOOCInstruction.incrRef(created, 1);
								return created;
							});
							stream.reserveLazyHandle();
							_outputs.put(name, stream);
						}
						else
							_scalarOutputs.put(name, ec.getVariable(name));
					}
				}
				finally {
					releaseReservations();
				}
				for(Hop root : _roots) {
					Data removed = ec.removeVariable(root.getName());
					TeeOOCInstruction.releaseRef(ec, removed);
				}
			}
		}

		private synchronized void releaseReservations() {
			if(_released)
				return;
			_released = true;
			_reservations.forEach(OOCStreamable::discardHandle);
		}

		private synchronized boolean isCompiled() {
			return _outputs != null;
		}

		private synchronized OOCStreamable<IndexedMatrixValue> getOutput(String output) {
			return _outputs == null ? null : _outputs.get(output);
		}

		private synchronized void releaseOutput(String output) {
			OOCStreamable<IndexedMatrixValue> stream = _outputs == null ? null : _outputs.get(output);
			if(stream != null)
				stream.scheduleMaterializedStoreDeletion();
		}

		private synchronized void transferReservationsTo(IdentityHashMap<OOCStreamable<?>, Boolean> target) {
			if(_released)
				return;
			_released = true;
			for(OOCStreamable<?> reservation : _reservations)
				if(target.put(reservation, Boolean.TRUE) != null)
					reservation.discardHandle();
		}
	}

	private static final class LazyStream implements OOCStreamable<IndexedMatrixValue> {
		private final Plan _plan;
		private final String _output;
		private final DataCharacteristics _characteristics;
		private CacheableData<?> _data;

		private LazyStream(Plan plan, String output, DataCharacteristics characteristics) {
			_plan = plan;
			_output = output;
			_characteristics = characteristics;
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReadStream() {
			return _plan.read(_output);
		}

		@Override
		public OOCStream<IndexedMatrixValue> getWriteStream() {
			return getReadStream();
		}

		@Override
		public boolean hasStreamCache() {
			OOCStreamable<IndexedMatrixValue> output = _plan.getOutput(_output);
			return output != null && output.hasStreamCache();
		}

		@Override
		public CachingStream getStreamCache() {
			OOCStreamable<IndexedMatrixValue> output = _plan.getOutput(_output);
			return output == null ? null : output.getStreamCache();
		}

		@Override
		public boolean hasMaterializedStore() {
			OOCStreamable<IndexedMatrixValue> output = _plan.getOutput(_output);
			return output != null && output.hasMaterializedStore();
		}

		@Override
		public void scheduleMaterializedStoreDeletion() {
			_plan.releaseOutput(_output);
		}

		@Override
		public boolean isProcessed() {
			return _plan._outputs != null;
		}

		@Override
		public DataCharacteristics getDataCharacteristics() {
			return _characteristics;
		}

		@Override
		public CacheableData<?> getData() {
			return _data;
		}

		@Override
		public void setData(CacheableData<?> data) {
			_data = data;
		}

		@Override
		public OOCPrimitive getPrimitive() {
			OOCStreamable<IndexedMatrixValue> output = _plan.getOutput(_output);
			return output == null ? null : output.getPrimitive();
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReservedReadStream() {
			return _plan.readReserved(_output);
		}

		@Override
		public void reserveLazyHandle() {
			OOCStreamable<IndexedMatrixValue> output = _plan.getOutput(_output);
			if(output != null)
				output.reserveLazyHandle();
		}

		@Override
		public void discardHandle() {
			OOCStreamable<IndexedMatrixValue> output = _plan.getOutput(_output);
			if(output != null)
				output.discardHandle();
		}
	}

	private static final class ReservedStreamable implements OOCStreamable<IndexedMatrixValue> {
		private final OOCStreamable<IndexedMatrixValue> _source;
		private CacheableData<?> _data;

		private ReservedStreamable(OOCStreamable<IndexedMatrixValue> source) {
			_source = source;
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReadStream() {
			return _source.getReservedReadStream();
		}

		@Override
		public OOCStream<IndexedMatrixValue> getWriteStream() {
			return getReadStream();
		}

		@Override
		public boolean hasStreamCache() {
			return _source.hasStreamCache();
		}

		@Override
		public CachingStream getStreamCache() {
			return _source.getStreamCache();
		}

		@Override
		public boolean hasMaterializedStore() {
			return _source.hasMaterializedStore();
		}

		@Override
		public void scheduleMaterializedStoreDeletion() {
			_source.scheduleMaterializedStoreDeletion();
		}

		@Override
		public boolean isProcessed() {
			return _source.isProcessed();
		}

		@Override
		public DataCharacteristics getDataCharacteristics() {
			return _source.getDataCharacteristics();
		}

		@Override
		public CacheableData<?> getData() {
			return _data;
		}

		@Override
		public void setData(CacheableData<?> data) {
			_data = data;
		}

		@Override
		public OOCPrimitive getPrimitive() {
			return _source.getPrimitive();
		}

		@Override
		public OOCStream<IndexedMatrixValue> getReservedReadStream() {
			return _source.getReservedReadStream();
		}

		@Override
		public void reserveLazyHandle() {
			_source.reserveLazyHandle();
		}

		@Override
		public void discardHandle() {
			_source.discardHandle();
		}
	}
}
