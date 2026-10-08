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
package org.apache.asterix.column.values.writer.filters;

import org.apache.asterix.dataflow.data.nontagged.comparators.ComparatorUtil;

public class DoubleColumnFilterWriter extends AbstractColumnFilterWriter {
    private double min;
    private double max;

    public DoubleColumnFilterWriter() {
        reset();
    }

    @Override
    public void addDouble(double value) {
        // Math.min and Math.max return NaN once any value is NaN, and the range filter would then skip this mega
        // leaf node for every predicate. In SQL++ order NaN is above +INF, so it is the max and leaves the min alone.
        if (ComparatorUtil.compareDoubles(value, min) < 0) {
            min = value;
        }
        if (ComparatorUtil.compareDoubles(value, max) > 0) {
            max = value;
        }
    }

    @Override
    public long getMinNormalizedValue() {
        return Double.doubleToLongBits(min);
    }

    @Override
    public long getMaxNormalizedValue() {
        return Double.doubleToLongBits(max);
    }

    @Override
    public void reset() {
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
    }
}
