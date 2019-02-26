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

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

/**
 * Base class for window runtime factories that compute nested aggregates
 */
abstract class AbstractWindowNestedPlansRuntimeFactory extends WindowMaterializingRuntimeFactory {

    private static final long serialVersionUID = 1L;

    final int nestedAggOutSchemaSize;

    final WindowAggregatorDescriptorFactory nestedAggFactory;

    AbstractWindowNestedPlansRuntimeFactory(int[] partitionColumns,
            IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int[] projectionColumnsExcludingSubplans,
            int[] runningAggOutColumns, IRunningAggregateEvaluatorFactory[] runningAggFactories,
            int nestedAggOutSchemaSize, WindowAggregatorDescriptorFactory nestedAggFactory, int memSizeInFrames) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories,
                projectionColumnsExcludingSubplans, runningAggOutColumns, runningAggFactories, memSizeInFrames);
        this.nestedAggFactory = nestedAggFactory;
        this.nestedAggOutSchemaSize = nestedAggOutSchemaSize;
    }
}
