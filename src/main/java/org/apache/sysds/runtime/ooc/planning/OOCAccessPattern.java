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

public enum OOCAccessPattern {
	ROW_MAJOR, COL_MAJOR, ANY, UNKNOWN, UNSET;

	public OOCAccessPattern fused(OOCAccessPattern that) {
		return switch(this) {
			case ANY -> that;
			case UNKNOWN, UNSET -> this;
			case ROW_MAJOR -> that == ROW_MAJOR || that == ANY ? this : UNKNOWN;
			case COL_MAJOR -> that == COL_MAJOR || that == ANY ? this : UNKNOWN;
		};
	}

	public OOCAccessPattern transposed() {
		return switch(this) {
			case ROW_MAJOR -> COL_MAJOR;
			case COL_MAJOR -> ROW_MAJOR;
			default -> this;
		};
	}

	public OOCAccessPattern preferred(OOCAccessPattern preferred) {
		return (this == ANY || this == UNSET) ? preferred : this;
	}

	public boolean isPlannable() {
		return this == ROW_MAJOR || this == COL_MAJOR || this == ANY;
	}

	public boolean isUnset() {
		return this == UNSET;
	}
}
