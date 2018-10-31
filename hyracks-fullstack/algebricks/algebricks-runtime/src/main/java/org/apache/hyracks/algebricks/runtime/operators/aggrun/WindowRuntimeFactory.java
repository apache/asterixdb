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

package org.apache.hyracks.algebricks.runtime.operators.aggrun;

import java.util.Arrays;

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class WindowRuntimeFactory extends RunningAggregateRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int[] partitionColumnList;

    private final IBinaryComparatorFactory[] partitionComparatorFactories;

    private final boolean partitionMaterialization;

    private final IBinaryComparatorFactory[] orderComparatorFactories;

    public WindowRuntimeFactory(int[] outColumns, IRunningAggregateEvaluatorFactory[] aggFactories,
            int[] projectionList, int[] partitionColumnList, IBinaryComparatorFactory[] partitionComparatorFactories,
            boolean partitionMaterialization, IBinaryComparatorFactory[] orderComparatorFactories) {
        super(outColumns, aggFactories, projectionList);
        this.partitionColumnList = partitionColumnList;
        this.partitionComparatorFactories = partitionComparatorFactories;
        this.partitionMaterialization = partitionMaterialization;
        this.orderComparatorFactories = orderComparatorFactories;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx) {
        return partitionMaterialization
                ? new MaterializingWindowPushRuntime(outColumns, aggFactories, projectionList, partitionColumnList,
                        partitionComparatorFactories, orderComparatorFactories, ctx)
                : new SimpleWindowPushRuntime(outColumns, aggFactories, projectionList, partitionColumnList,
                        partitionComparatorFactories, orderComparatorFactories, ctx);
    }

    @Override
    public String toString() {
        return "window (" + Arrays.toString(partitionColumnList) + ") " + Arrays.toString(outColumns) + " := "
                + Arrays.toString(aggFactories);
    }
}
