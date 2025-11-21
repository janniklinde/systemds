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
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TeeOOCInstruction extends ComputationOOCInstruction {

	private static final ConcurrentHashMap<String, CachingStream> createdCachingStreams = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<CachingStream, Set<String>> cachingStreamReferences = new ConcurrentHashMap<>();

	public static void reset() {
		createdCachingStreams.clear();
	}

	public static void flagForDeletion(OOCStreamable<IndexedMatrixValue> stream, String consumerName) {
		if (!(stream instanceof CachingStream)) {
			// Check if we can resolve via consumerName
			stream = createdCachingStreams.get(consumerName);

			if (stream == null)
				return;
		}

		CachingStream cStream = (CachingStream)stream;

		Object result = cachingStreamReferences.compute(cStream, (k, s) -> {
			if (s == null)
				return null;

			s.remove(consumerName);

			if (s.isEmpty())
				return null;

			return s;
		});

		if (result == null) {
			System.out.println("Scheduling deletion of " + cStream + " based on " + consumerName);
			cStream.scheduleDeletion();
		}
	}

	public static void registerCachingStreamConsumer(CachingStream stream, String... consumerName) {
		cachingStreamReferences.compute(stream, (k, v) -> {
			if (v == null)
				v = new HashSet<>(3);

			v.addAll(List.of(consumerName));
			return v;
		});
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

	public void processInstruction( ExecutionContext ec ) {
		//get input stream
		MatrixObject min = ec.getMatrixObject(input1);
		OOCStream<IndexedMatrixValue> qIn = min.getStreamHandle();

		CachingStream handle = qIn.hasStreamCache() ? qIn.getStreamCache() : createdCachingStreams.compute(input1.getName(), (k, v) -> v == null ? new CachingStream(qIn) : v);

		registerCachingStreamConsumer(handle, input1.getName(), output.getName());

		//get output and create new resettable stream
		MatrixObject mo = ec.getMatrixObject(output);
		mo.setStreamHandle(handle);
		mo.setMetaData(min.getMetaData());
	}
}
