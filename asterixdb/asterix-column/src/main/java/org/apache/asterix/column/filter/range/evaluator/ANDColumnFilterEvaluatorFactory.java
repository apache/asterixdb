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
package org.apache.asterix.column.filter.range.evaluator;

import org.apache.asterix.column.filter.FalseColumnFilterEvaluator;
import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.filter.TrueColumnFilterEvaluator;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ANDColumnFilterEvaluatorFactory extends AbstractColumnFilterEvaluatorFactory {
    private static final long serialVersionUID = -7902069740719309586L;

    public ANDColumnFilterEvaluatorFactory(IColumnRangeFilterEvaluatorFactory left,
            IColumnRangeFilterEvaluatorFactory right) {
        super(left, right);
    }

    @Override
    public IColumnFilterEvaluator create(FilterAccessorProvider filterAccessorProvider) throws HyracksDataException {
        IColumnFilterEvaluator leftEval = left.create(filterAccessorProvider);
        IColumnFilterEvaluator rightEval = right.create(filterAccessorProvider);
        if (leftEval == FalseColumnFilterEvaluator.INSTANCE || rightEval == FalseColumnFilterEvaluator.INSTANCE) {
            // Either is false, then return false
            return FalseColumnFilterEvaluator.INSTANCE;
        } else if (leftEval == TrueColumnFilterEvaluator.INSTANCE && rightEval == TrueColumnFilterEvaluator.INSTANCE) {
            //Skip both operands and return TrueColumnFilterEvaluator
            return TrueColumnFilterEvaluator.INSTANCE;
        } else if (leftEval == TrueColumnFilterEvaluator.INSTANCE) {
            //Left is true, return the right evaluator
            return rightEval;
        } else if (rightEval == TrueColumnFilterEvaluator.INSTANCE) {
            //Same as above but the right is true
            return leftEval;
        } else {
            //Both are actual filters
            return create(leftEval, rightEval);
        }
    }

    private IColumnFilterEvaluator create(IColumnFilterEvaluator left, IColumnFilterEvaluator right) {
        return new AbstractFilterEvaluator(left, right) {
            @Override
            public boolean evaluate() throws HyracksDataException {
                return left.evaluate() && right.evaluate();
            }
        };
    }

    @Override
    protected String getOp() {
        return "&&";
    }
}
