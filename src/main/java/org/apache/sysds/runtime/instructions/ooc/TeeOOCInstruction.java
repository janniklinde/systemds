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

import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.InstructionUtils;
import org.apache.sysds.runtime.instructions.cp.CPOperand;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.instructions.cp.ListObject;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.ooc.store.MaterializedStoreStreamable;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TeeOOCInstruction extends ComputationOOCInstruction {

	private static final ConcurrentHashMap<OOCStreamable<IndexedMatrixValue>, Integer> refCtr = new ConcurrentHashMap<>();

	private static final ConcurrentHashMap<OOCStreamable<IndexedMatrixValue>, Set<MatrixObject>> refOwners =
		new ConcurrentHashMap<>();

	public static void reset() {
		refOwners.clear();
		if (!refCtr.isEmpty()) {
			Map<OOCStreamable<IndexedMatrixValue>, Integer> dangling = refCtr.entrySet().stream()
				.filter(e -> e.getValue() > 0).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			if(!dangling.isEmpty())
				System.err.println("There are some dangling streams still in the cache: " + dangling);
			for(OOCStreamable<IndexedMatrixValue> stream : refCtr.keySet()) {
				try {
					scheduleDeletion(stream);
				}
				catch(Exception ex) {
					System.err
						.println("Failed to schedule deletion for dangling stream " + stream + ": " + ex.getMessage());
				}
			}
			refCtr.clear();
		}
	}

	/**
	 * Increments the reference counter of a stream by the set amount.
	 */
	public static void incrRef(OOCStreamable<IndexedMatrixValue> stream, int incr) {
		changeRef(stream, incr, null, null);
	}

	public static void registerOwner(OOCStreamable<IndexedMatrixValue> stream, MatrixObject owner) {
		OOCStreamable<IndexedMatrixValue> handle = resolveHandle(stream);
		if(handle == null || owner == null)
			return;
		refOwners.computeIfAbsent(handle,
			k -> Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()))).add(owner);
	}

	public static void releaseRef(ExecutionContext ec, Data data) {
		if(data instanceof ListObject) {
			for(Data element : ((ListObject) data).getData())
				releaseRef(ec, element);
			return;
		}
		if(!(data instanceof MatrixObject))
			return;
		MatrixObject mo = (MatrixObject) data;
		changeRef(mo.getStreamable(), -1, mo, ec);
	}

	private static void changeRef(OOCStreamable<IndexedMatrixValue> stream, int incr, MatrixObject owner,
		ExecutionContext ec) {
		OOCStreamable<IndexedMatrixValue> handle = resolveHandle(stream);
		if(handle == null)
			return;
		if(owner != null)
			registerOwner(handle, owner);

		Integer ref = refCtr.compute(handle, (k, v) -> {
			int count = (v == null ? 0 : v) + incr;
			if(count > 0)
				return count;
			return isReleasable(handle, owner, ec) ? null : 0;
		});

		if(ref == null)
			scheduleDeletion(handle);
	}

	private static boolean isReleasable(OOCStreamable<IndexedMatrixValue> handle, MatrixObject owner,
		ExecutionContext ec) {
		if(owner == null || ec == null)
			return true;
		Set<MatrixObject> registered = refOwners.get(handle);
		if(registered == null)
			return isDead(owner, ec);
		synchronized(registered) {
			for(MatrixObject candidate : registered)
				if(!isDead(candidate, ec))
					return false;
		}
		refOwners.remove(handle);
		return true;
	}

	private static boolean isDead(MatrixObject mo, ExecutionContext ec) {
		return mo.isCleanupEnabled() && !ec.getVariables().hasReferences(mo);
	}

	private static OOCStreamable<IndexedMatrixValue> resolveHandle(OOCStreamable<IndexedMatrixValue> stream) {
		if(stream == null || (!stream.hasStreamCache() && !stream.hasMaterializedStore()))
			return null;
		return stream.hasStreamCache() ? stream.getStreamCache() : stream;
	}

	private static void scheduleDeletion(OOCStreamable<IndexedMatrixValue> stream) {
		if(stream.hasMaterializedStore())
			stream.scheduleMaterializedStoreDeletion();
		else
			stream.getStreamCache().scheduleDeletion();
	}

	protected TeeOOCInstruction(OOCType type, CPOperand in1, CPOperand out, String opcode, String istr) {
		super(type, null, in1, out, opcode, istr);
	}

	public static TeeOOCInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 2);
		String opcode = parts[0];
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		return new TeeOOCInstruction(OOCType.Tee, in1, out, opcode, str);
	}

	public void processInstruction(ExecutionContext ec) {
		//get input stream
		MatrixObject min = ec.getMatrixObject(input1);
		OOCStreamable<IndexedMatrixValue> streamable = min.getStreamable();
		OOCStreamable<IndexedMatrixValue> handle;

		if(streamable.hasStreamCache() || streamable.hasMaterializedStore()) {
			handle = streamable.hasStreamCache() ? streamable.getStreamCache() : streamable;
			incrRef(handle, 1);
		}
		else {
			// The input and output matrix objects both retain the new reusable handle.
			handle = new MaterializedStoreStreamable(min.getStreamHandle(), min);
			min.setStreamHandle(handle);
			incrRef(handle, 2);
		}

		//get output and create new resettable stream
		MatrixObject mo = ec.getMatrixObject(output);
		mo.setStreamHandle(handle);
		mo.setMetaData(min.getMetaData());

		registerOwner(handle, min);
		registerOwner(handle, mo);
	}
}
