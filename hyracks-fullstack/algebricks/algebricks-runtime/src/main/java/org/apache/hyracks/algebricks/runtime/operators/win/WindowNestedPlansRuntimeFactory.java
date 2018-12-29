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

import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

/**
 * Runtime factory for window operators that performs partition materialization and can evaluate running aggregates
 * as well as regular aggregates (in nested plans) over window frames.
 */
public class WindowNestedPlansRuntimeFactory extends AbstractWindowRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory[] frameValueEvalFactories;

    private final IScalarEvaluatorFactory[] frameStartEvalFactories;

    private final IScalarEvaluatorFactory[] frameEndEvalFactories;

    private final IBinaryComparatorFactory[] frameValueComparatorFactories;

    private final IScalarEvaluatorFactory[] frameExcludeEvalFactories;

    private final int frameExcludeNegationStartIdx;

    private final IBinaryComparatorFactory[] frameExcludeComparatorFactories;

    private final IScalarEvaluatorFactory frameOffsetEvalFactory;

    private final IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory;

    private final int frameMaxObjects;

    private final int nestedAggOutSchemaSize;

    private final WindowAggregatorDescriptorFactory nestedAggFactory;

    public WindowNestedPlansRuntimeFactory(int[] partitionColumns,
            IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, IScalarEvaluatorFactory[] frameValueEvalFactories,
            IBinaryComparatorFactory[] frameValueComparatorFactories, IScalarEvaluatorFactory[] frameStartEvalFactories,
            IScalarEvaluatorFactory[] frameEndEvalFactories, IScalarEvaluatorFactory[] frameExcludeEvalFactories,
            int frameExcludeNegationStartIdx, IBinaryComparatorFactory[] frameExcludeComparatorFactories,
            IScalarEvaluatorFactory frameOffsetEvalFactory,
            IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory, int frameMaxObjects,
            int[] projectionColumnsExcludingSubplans, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, int nestedAggOutSchemaSize,
            WindowAggregatorDescriptorFactory nestedAggFactory) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories,
                projectionColumnsExcludingSubplans, runningAggOutColumns, runningAggFactories);
        this.frameValueEvalFactories = frameValueEvalFactories;
        this.frameStartEvalFactories = frameStartEvalFactories;
        this.frameEndEvalFactories = frameEndEvalFactories;
        this.frameValueComparatorFactories = frameValueComparatorFactories;
        this.frameExcludeEvalFactories = frameExcludeEvalFactories;
        this.frameExcludeComparatorFactories = frameExcludeComparatorFactories;
        this.frameExcludeNegationStartIdx = frameExcludeNegationStartIdx;
        this.frameOffsetEvalFactory = frameOffsetEvalFactory;
        this.binaryIntegerInspectorFactory = binaryIntegerInspectorFactory;
        this.frameMaxObjects = frameMaxObjects;
        this.nestedAggFactory = nestedAggFactory;
        this.nestedAggOutSchemaSize = nestedAggOutSchemaSize;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx) {
        return new WindowNestedPlansPushRuntime(partitionColumns, partitionComparatorFactories,
                orderComparatorFactories, frameValueEvalFactories, frameValueComparatorFactories,
                frameStartEvalFactories, frameEndEvalFactories, frameExcludeEvalFactories, frameExcludeNegationStartIdx,
                frameExcludeComparatorFactories, frameOffsetEvalFactory, binaryIntegerInspectorFactory, frameMaxObjects,
                projectionList, runningAggOutColumns, runningAggFactories, nestedAggOutSchemaSize, nestedAggFactory,
                ctx);
    }

    @Override
    public String toString() {
        return "window [nested] (" + Arrays.toString(partitionColumns) + ") " + Arrays.toString(runningAggOutColumns)
                + " := " + Arrays.toString(runningAggFactories);
    }
}
