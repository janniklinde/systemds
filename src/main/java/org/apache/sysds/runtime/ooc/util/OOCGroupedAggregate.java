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

package org.apache.sysds.runtime.ooc.util;

import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.functionobjects.CM;
import org.apache.sysds.runtime.instructions.cp.CmCovObject;
import org.apache.sysds.runtime.instructions.cp.KahanObject;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;
import org.apache.sysds.runtime.matrix.operators.CMOperator;
import org.apache.sysds.runtime.matrix.operators.Operator;

/**
 * Accumulator state of an out-of-core grouped aggregate. One instance covers all groups over a contiguous window of
 * target columns, so a partial built from a single tile only holds that tile's columns; merging two partials widens the
 * window to their union. Sums use a Kahan accumulator, every other aggregation a central-moment object, both merged
 * through the same function objects the CP implementation uses.
 */
public final class OOCGroupedAggregate {
	private final Operator _op;
	private final CM _cmFn;
	private final int _groups;
	private final KahanObject[][] _sums;
	private final CmCovObject[][] _moments;
	private int _colLow;
	private int _colHigh;

	public OOCGroupedAggregate(Operator op, int groups, int colLow, int colHigh) {
		if(groups <= 0)
			throw new DMLRuntimeException("Grouped aggregate requires a positive group count: " + groups);
		if(colLow < 0 || colHigh < colLow)
			throw new DMLRuntimeException("Invalid grouped aggregate column window: [" + colLow + ", " + colHigh + ")");
		_op = op;
		_groups = groups;
		_colLow = colLow;
		_colHigh = colHigh;
		int width = colHigh - colLow;
		if(op instanceof AggregateOperator) {
			_cmFn = null;
			_sums = new KahanObject[groups][width];
			_moments = null;
			for(int g = 0; g < groups; g++)
				for(int c = 0; c < width; c++)
					_sums[g][c] = new KahanObject(((AggregateOperator) op).initialValue, 0);
		}
		else if(op instanceof CMOperator) {
			_cmFn = CM.getCMFnObject(((CMOperator) op).getAggOpType());
			_sums = null;
			_moments = new CmCovObject[groups][width];
			for(int g = 0; g < groups; g++)
				for(int c = 0; c < width; c++)
					_moments[g][c] = new CmCovObject();
		}
		else
			throw new DMLRuntimeException("Unsupported operator for grouped aggregate: " + op);
	}

	/**
	 * Folds one target tile into this accumulator. The group of a row is read from the matching entry of the paired
	 * group column, and groups beyond the declared count are dropped exactly as the CP implementation drops them.
	 *
	 * @param groupIds  column vector of group ids, one entry per row of {@code target}
	 * @param target    the tile to fold in
	 * @param colOffset global column index of the tile's first column
	 */
	public void add(MatrixBlock groupIds, MatrixBlock target, int colOffset) {
		int rows = target.getNumRows();
		int cols = target.getNumColumns();
		if(groupIds.getNumRows() != rows)
			throw new DMLRuntimeException(
				"Grouped aggregate row mismatch: " + groupIds.getNumRows() + " group ids for " + rows + " target rows");
		if(colOffset < _colLow || colOffset + cols > _colHigh)
			throw new DMLRuntimeException("Tile columns [" + colOffset + ", " + (colOffset + cols)
				+ ") outside the accumulator window [" + _colLow + ", " + _colHigh + ")");
		boolean sums = _sums != null;
		// sums are sparse-safe, central moments are not, matching the CP grouped aggregate
		if(sums && target.isEmptyBlock(false))
			return;
		for(int row = 0; row < rows; row++) {
			int group = (int) groupIds.get(row, 0);
			if(group < 1 || group > _groups)
				continue;
			for(int col = 0; col < cols; col++) {
				double value = target.get(row, col);
				int slot = colOffset - _colLow + col;
				if(sums) {
					if(value != 0)
						((AggregateOperator) _op).increOp.fn.execute(_sums[group - 1][slot], value);
				}
				else
					_cmFn.execute(_moments[group - 1][slot], value, 1);
			}
		}
	}

	public OOCGroupedAggregate merge(OOCGroupedAggregate other) {
		if(other == null)
			return this;
		if(_groups != other._groups)
			throw new DMLRuntimeException(
				"Cannot merge grouped aggregates over " + _groups + " and " + other._groups + " groups");
		OOCGroupedAggregate target = this;
		if(other._colLow < _colLow || other._colHigh > _colHigh)
			target = widened(Math.min(_colLow, other._colLow), Math.max(_colHigh, other._colHigh));
		for(int group = 0; group < _groups; group++)
			for(int col = other._colLow; col < other._colHigh; col++) {
				int slot = col - target._colLow;
				int source = col - other._colLow;
				if(target._sums != null)
					((AggregateOperator) _op).increOp.fn.execute(target._sums[group][slot],
						other._sums[group][source]._sum, other._sums[group][source]._correction);
				else
					target._cmFn.execute(target._moments[group][slot], other._moments[group][source]);
			}
		return target;
	}

	public MatrixBlock toMatrixBlock(int rowLow, int rowHigh, int colLow, int colHigh) {
		if(colLow < _colLow || colHigh > _colHigh)
			throw new DMLRuntimeException("Requested columns [" + colLow + ", " + colHigh
				+ ") outside the accumulator window [" + _colLow + ", " + _colHigh + ")");
		MatrixBlock result = new MatrixBlock(rowHigh - rowLow, colHigh - colLow, false);
		result.allocateDenseBlock();
		for(int group = rowLow; group < rowHigh; group++)
			for(int col = colLow; col < colHigh; col++) {
				int slot = col - _colLow;
				double value = _sums != null ? _sums[group][slot]._sum : _moments[group][slot].getRequiredResult(_op);
				result.set(group - rowLow, col - colLow, value);
			}
		result.recomputeNonZeros();
		result.examSparsity();
		return result;
	}

	public long estimateBytes() {
		// a Kahan pair is two doubles, a central-moment object nine plus its object header
		long perCell = _sums != null ? 32 : 128;
		return (long) _groups * (_colHigh - _colLow) * perCell;
	}

	private OOCGroupedAggregate widened(int colLow, int colHigh) {
		OOCGroupedAggregate wider = new OOCGroupedAggregate(_op, _groups, colLow, colHigh);
		for(int group = 0; group < _groups; group++)
			for(int col = _colLow; col < _colHigh; col++) {
				int slot = col - colLow;
				int source = col - _colLow;
				if(_sums != null)
					wider._sums[group][slot].set(_sums[group][source]);
				else
					wider._moments[group][slot].set(_moments[group][source]);
			}
		return wider;
	}
}
