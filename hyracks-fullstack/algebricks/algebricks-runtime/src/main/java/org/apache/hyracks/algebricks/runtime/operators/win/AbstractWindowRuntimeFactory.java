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
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public abstract class AbstractWindowRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    final int[] partitionColumns;

    final IBinaryComparatorFactory[] partitionComparatorFactories;

    final IBinaryComparatorFactory[] orderComparatorFactories;

    final int[] runningAggOutColumns;

    final IRunningAggregateEvaluatorFactory[] runningAggFactories;

    AbstractWindowRuntimeFactory(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories) {
        super(projectionColumns);
        this.runningAggOutColumns = runningAggOutColumns;
        this.runningAggFactories = runningAggFactories;
        this.partitionColumns = partitionColumns;
        this.partitionComparatorFactories = partitionComparatorFactories;
        this.orderComparatorFactories = orderComparatorFactories;
    }
}
