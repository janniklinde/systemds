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

package org.apache.sysds.test.component.ooc;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.runtime.meta.MetaDataFormat;
import org.apache.sysds.runtime.ooc.util.OOCDimensions;
import org.junit.Assert;
import org.junit.Test;

public class OOCDimensionsTest {

	@Test
	public void testKnownRequiresBothDimensionsAndBlocksize() {
		Assert.assertTrue(OOCDimensions.known(matrix(100, 10, 1000)));
		Assert.assertFalse(OOCDimensions.known(matrix(-1, 10, 1000)));
		Assert.assertFalse(OOCDimensions.known(matrix(100, -1, 1000)));
		Assert.assertFalse(OOCDimensions.known(matrix(100, 10, 0)));
		Assert.assertFalse(OOCDimensions.known((MatrixObject) null));
	}

	@Test
	public void testKnownOverSeveralInputs() {
		Assert.assertTrue(OOCDimensions.known(matrix(100, 10, 1000), matrix(100, 1, 1000)));
		Assert.assertFalse(OOCDimensions.known(matrix(100, 10, 1000), matrix(-1, 1, 1000)));
		Assert.assertTrue(OOCDimensions.known());
	}

	@Test
	public void testRequireAcceptsResolvedAndOptionalInputs() {
		OOCDimensions.require("nmin", matrix(100, 10, 1000), null);
	}

	@Test
	public void testRequireNamesEveryUnresolvedInput() {
		try {
			OOCDimensions.require("nmin", matrix(100, 10, 1000), matrix(-1, 10, 1000), matrix(5, -1, 1000));
			Assert.fail("unresolved dimensions must be rejected");
		}
		catch(DMLRuntimeException expected) {
			String message = expected.getMessage();
			Assert.assertTrue(message, message.contains("nmin"));
			Assert.assertTrue(message, message.contains("input 2"));
			Assert.assertTrue(message, message.contains("input 3"));
			Assert.assertFalse(message, message.contains("input 1"));
		}
	}

	private static MatrixObject matrix(long rows, long cols, int blocksize) {
		DataCharacteristics dc = new MatrixCharacteristics(rows, cols, blocksize, -1);
		MatrixObject mo = new MatrixObject(Types.ValueType.FP64, "dimensions-test");
		mo.setMetaData(new MetaDataFormat(dc, Types.FileFormat.BINARY));
		return mo;
	}
}
