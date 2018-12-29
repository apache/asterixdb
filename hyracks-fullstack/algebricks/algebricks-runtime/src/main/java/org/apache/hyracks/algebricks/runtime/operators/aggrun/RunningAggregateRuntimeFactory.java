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
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class RunningAggregateRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    protected final int[] runningAggOutColumns;

    protected final IRunningAggregateEvaluatorFactory[] runningAggFactories;

    /**
     * @param projectionColumns
     *            an array of columns to be projected
     * @param runningAggOutColumns
     *            a sorted array of columns into which the result is written to
     * @param runningAggFactories
     */
    public RunningAggregateRuntimeFactory(int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories) {
        super(projectionColumns);
        this.runningAggOutColumns = runningAggOutColumns;
        this.runningAggFactories = runningAggFactories;
    }

    @Override
    public AbstractOneInputOneOutputOneFramePushRuntime createOneOutputPushRuntime(IHyracksTaskContext ctx) {
        return new RunningAggregatePushRuntime(projectionList, runningAggOutColumns, runningAggFactories, ctx);
    }

    @Override
    public String toString() {
        return "running-aggregate " + Arrays.toString(runningAggOutColumns) + " := "
                + Arrays.toString(runningAggFactories);
    }
}
