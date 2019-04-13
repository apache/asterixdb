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

package org.apache.hyracks.algebricks.runtime.operators.win;

import java.util.Arrays;

import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

/**
 * Optimized runtime for window operators that performs partition materialization and can evaluate running aggregates
 * as well as regular aggregates (in nested plans) over accumulating window frames
 * (unbounded preceding to current row or N following).
 */
public class WindowNestedPlansRunningRuntimeFactory extends AbstractWindowNestedPlansRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory[] frameValueEvalFactories;

    private final IBinaryComparatorFactory[] frameValueComparatorFactories;

    private final IScalarEvaluatorFactory[] frameEndEvalFactories;

    private final IScalarEvaluatorFactory[] frameEndValidationEvalFactories;

    private final int frameMaxObjects;

    private final IBinaryBooleanInspectorFactory booleanAccessorFactory;

    public WindowNestedPlansRunningRuntimeFactory(int[] partitionColumns,
            IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, IScalarEvaluatorFactory[] frameValueEvalFactories,
            IBinaryComparatorFactory[] frameValueComparatorFactories, IScalarEvaluatorFactory[] frameEndEvalFactories,
            IScalarEvaluatorFactory[] frameEndValidationEvalFactories, int frameMaxObjects,
            IBinaryBooleanInspectorFactory booleanAccessorFactory, int[] projectionColumnsExcludingSubplans,
            int[] runningAggOutColumns, IRunningAggregateEvaluatorFactory[] runningAggFactories,
            int nestedAggOutSchemaSize, WindowAggregatorDescriptorFactory nestedAggFactory, int memSizeInFrames) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories,
                projectionColumnsExcludingSubplans, runningAggOutColumns, runningAggFactories, nestedAggOutSchemaSize,
                nestedAggFactory, memSizeInFrames);
        this.frameValueEvalFactories = frameValueEvalFactories;
        this.frameValueComparatorFactories = frameValueComparatorFactories;
        this.frameEndEvalFactories = frameEndEvalFactories;
        this.frameEndValidationEvalFactories = frameEndValidationEvalFactories;
        this.frameMaxObjects = frameMaxObjects;
        this.booleanAccessorFactory = booleanAccessorFactory;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx) {
        return new WindowNestedPlansRunningPushRuntime(partitionColumns, partitionComparatorFactories,
                orderComparatorFactories, frameValueEvalFactories, frameValueComparatorFactories, frameEndEvalFactories,
                frameEndValidationEvalFactories, frameMaxObjects, booleanAccessorFactory, projectionList,
                runningAggOutColumns, runningAggFactories, nestedAggOutSchemaSize, nestedAggFactory, ctx,
                memSizeInFrames, sourceLoc);
    }

    @Override
    public String toString() {
        return "window [nested-running] (" + Arrays.toString(partitionColumns) + ") "
                + Arrays.toString(runningAggOutColumns) + " := " + Arrays.toString(runningAggFactories);
    }
}
