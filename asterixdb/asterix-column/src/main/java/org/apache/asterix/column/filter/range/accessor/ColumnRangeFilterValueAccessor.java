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
package org.apache.asterix.column.filter.range.accessor;

import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessor;
import org.apache.asterix.om.types.ATypeTag;

public class ColumnRangeFilterValueAccessor implements IColumnRangeFilterValueAccessor {
    private static final long NAN_BITS = Double.doubleToLongBits(Double.NaN);
    private static final long NEGATIVE_INFINITY_BITS = Double.doubleToLongBits(Double.NEGATIVE_INFINITY);
    private final int columnIndex;
    private final ATypeTag typeTag;
    private final boolean min;
    private long normalizedValue;

    public ColumnRangeFilterValueAccessor(int columnIndex, ATypeTag typeTag, boolean min) {
        this.columnIndex = columnIndex;
        this.typeTag = typeTag;
        this.min = min;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public boolean isMin() {
        return min;
    }

    public void setNormalizedValue(long normalizedValue) {
        if (min && typeTag == ATypeTag.DOUBLE && normalizedValue == NAN_BITS) {
            // Before ASTERIXDB-3944 a NaN made the writer store NaN for both bounds of a mega leaf node, hiding its
            // real min. The writer no longer stores a NaN min, so one comes only from such older data: read it.
            normalizedValue = NEGATIVE_INFINITY_BITS;
        }
        this.normalizedValue = normalizedValue;
    }

    @Override
    public long getNormalizedValue() {
        return normalizedValue;
    }

    @Override
    public ATypeTag getTypeTag() {
        return typeTag;
    }

    @Override
    public String toString() {
        return Long.toString(normalizedValue);
    }
}
