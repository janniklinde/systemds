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

package org.apache.sysds.runtime.ooc.cache.io;

import java.io.DataInput;
import java.io.IOException;

import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

public class OOCMatrixBlock extends MatrixBlock {
	private static final long serialVersionUID = 1L;
	private static volatile SparseBlock.Type _ultraSparseType = SparseBlock.Type.CSR;

	public static void setUseCOO(boolean useCOO) {
		_ultraSparseType = useCOO ? SparseBlock.Type.COO : SparseBlock.Type.CSR;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in, _ultraSparseType);
	}
}
