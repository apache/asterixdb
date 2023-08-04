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

import org.apache.asterix.column.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessor;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessorFactory;

public class LTColumnFilterEvaluatorFactory extends AbstractColumnFilterComparatorFactory {
    private static final long serialVersionUID = -4066709771630858677L;

    public LTColumnFilterEvaluatorFactory(IColumnRangeFilterValueAccessorFactory left,
            IColumnRangeFilterValueAccessorFactory right) {
        super(left, right);
    }

    @Override
    protected IColumnFilterEvaluator createComparator(IColumnRangeFilterValueAccessor left,
            IColumnRangeFilterValueAccessor right) {
        return new AbstractComparator(left, right) {
            @Override
            public boolean evaluate() {
                return left.getNormalizedValue() < right.getNormalizedValue();
            }
        };
    }

    @Override
    protected String getOpt() {
        return "<";
    }
}
