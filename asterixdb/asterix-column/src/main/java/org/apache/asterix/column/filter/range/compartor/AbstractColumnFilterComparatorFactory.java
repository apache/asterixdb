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
package org.apache.asterix.column.filter.range.compartor;

import org.apache.asterix.column.filter.FalseColumnFilterEvaluator;
import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.filter.TrueColumnFilterEvaluator;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessor;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessorFactory;
import org.apache.asterix.column.filter.range.accessor.NoOpColumnRangeFilterValueAccessor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.api.exceptions.HyracksDataException;

abstract class AbstractColumnFilterComparatorFactory implements IColumnRangeFilterEvaluatorFactory {
    private static final long serialVersionUID = 4229059703449173694L;
    private final IColumnRangeFilterValueAccessorFactory left;
    private final IColumnRangeFilterValueAccessorFactory right;

    AbstractColumnFilterComparatorFactory(IColumnRangeFilterValueAccessorFactory left,
            IColumnRangeFilterValueAccessorFactory right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public final IColumnFilterEvaluator create(FilterAccessorProvider filterAccessorProvider)
            throws HyracksDataException {
        IColumnRangeFilterValueAccessor leftAccessor = left.create(filterAccessorProvider);
        IColumnRangeFilterValueAccessor rightAccessor = right.create(filterAccessorProvider);

        ATypeTag leftTypeTag = leftAccessor.getTypeTag();
        ATypeTag rightTypeTag = rightAccessor.getTypeTag();
        if (isNoOp(leftAccessor, rightAccessor)
                || leftTypeTag != rightTypeTag && ATypeHierarchy.isCompatible(leftTypeTag, rightTypeTag)) {
            // Cannot compare comparable values with different types. Bail out.
            return TrueColumnFilterEvaluator.INSTANCE;
        } else if (cannotCompare(leftTypeTag, rightTypeTag)) {
            return FalseColumnFilterEvaluator.INSTANCE;
        }
        return createComparator(leftAccessor, rightAccessor);
    }

    private boolean isNoOp(IColumnRangeFilterValueAccessor leftAccessor,
            IColumnRangeFilterValueAccessor rightAccessor) {
        return leftAccessor == NoOpColumnRangeFilterValueAccessor.INSTANCE
                || rightAccessor == NoOpColumnRangeFilterValueAccessor.INSTANCE;
    }

    private boolean cannotCompare(ATypeTag leftTypeTag, ATypeTag rightTypeTag) {
        return rightTypeTag == ATypeTag.MISSING || leftTypeTag != rightTypeTag;
    }

    protected abstract IColumnFilterEvaluator createComparator(IColumnRangeFilterValueAccessor left,
            IColumnRangeFilterValueAccessor right);

    protected abstract String getOpt();

    @Override
    public String toString() {
        return left.toString() + " " + getOpt() + " " + right.toString();
    }

    static abstract class AbstractComparator implements IColumnFilterEvaluator {
        protected final IColumnRangeFilterValueAccessor left;
        protected final IColumnRangeFilterValueAccessor right;

        AbstractComparator(IColumnRangeFilterValueAccessor left, IColumnRangeFilterValueAccessor right) {
            this.left = left;
            this.right = right;
        }
    }
}
