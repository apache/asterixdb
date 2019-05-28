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

package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.win.AbstractWindowRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.win.WindowAggregatorDescriptorFactory;
import org.apache.hyracks.algebricks.runtime.operators.win.WindowStreamRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public final class WindowStreamPOperator extends AbstractWindowPOperator {

    // fixed memory, 2 frames: 1 (output) + 1 (input copy conservative)
    public static final int MEM_SIZE_IN_FRAMES_FOR_WINDOW_STREAM = 2;

    public WindowStreamPOperator(List<LogicalVariable> partitionColumns, List<OrderColumn> orderColumns) {
        super(partitionColumns, orderColumns);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.WINDOW_STREAM;
    }

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        localMemoryRequirements = LocalMemoryRequirements.fixedMemoryBudget(MEM_SIZE_IN_FRAMES_FOR_WINDOW_STREAM);
    }

    @Override
    protected AbstractWindowRuntimeFactory createRuntimeFactory(WindowOperator winOp, int[] partitionColumnsList,
            IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, IScalarEvaluatorFactory[] frameValueExprEvals,
            IBinaryComparatorFactory[] frameValueComparatorFactories, IScalarEvaluatorFactory[] frameStartExprEvals,
            IScalarEvaluatorFactory[] frameStartValidationExprEvals, IScalarEvaluatorFactory[] frameEndExprEvals,
            IScalarEvaluatorFactory[] frameEndValidationExprEvals, IScalarEvaluatorFactory[] frameExcludeExprEvals,
            IBinaryComparatorFactory[] frameExcludeComparatorFactories,
            IScalarEvaluatorFactory frameExcludeUnaryExprEval, IScalarEvaluatorFactory frameOffsetExprEval,
            int[] projectionColumnsExcludingSubplans, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, int nestedAggOutSchemaSize,
            WindowAggregatorDescriptorFactory nestedAggFactory, JobGenContext context) {
        return new WindowStreamRuntimeFactory(partitionColumnsList, partitionComparatorFactories,
                orderComparatorFactories, projectionColumnsExcludingSubplans, runningAggOutColumns,
                runningAggFactories);
    }
}
