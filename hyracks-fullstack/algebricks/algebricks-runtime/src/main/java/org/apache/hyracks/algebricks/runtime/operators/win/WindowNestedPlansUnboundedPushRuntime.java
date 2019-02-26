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

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;

/**
 * Optimized runtime for window operators that performs partition materialization and can evaluate running aggregates
 * as well as regular aggregates (in nested plans) over <b>unbounded</b> window frames.
 * An unbounded frame is equivalent to the whole partition, so nested aggregates can only
 * be evaluated once per partition and their results returned for each row in the partition
 * (the result remains the same for each row).
 * <p>
 * In addition to the unbounded frame specification the following conditions must be met:
 * <ul>
 * <li>no frame exclusion</li>
 * <li>no frame offset</li>
 * </ul>
 */
class WindowNestedPlansUnboundedPushRuntime extends AbstractWindowNestedPlansPushRuntime {

    private ArrayTupleBuilder nestedAggOutputBuilder;

    private final int frameMaxObjects;

    private int toWrite;

    WindowNestedPlansUnboundedPushRuntime(int[] partitionColumns,
            IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int frameMaxObjects, int[] projectionColumns,
            int[] runningAggOutColumns, IRunningAggregateEvaluatorFactory[] runningAggFactories,
            int nestedAggOutSchemaSize, WindowAggregatorDescriptorFactory nestedAggFactory, IHyracksTaskContext ctx,
            int memSizeInFrames, SourceLocation sourceLoc) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories, projectionColumns,
                runningAggOutColumns, runningAggFactories, nestedAggOutSchemaSize, nestedAggFactory, ctx,
                memSizeInFrames, sourceLoc);
        this.frameMaxObjects = frameMaxObjects;
    }

    @Override
    protected void init() throws HyracksDataException {
        super.init();
        nestedAggOutputBuilder = new ArrayTupleBuilder(nestedAggOutSchemaSize);
    }

    @Override
    protected void beginPartitionImpl() throws HyracksDataException {
        super.beginPartitionImpl();
        nestedAggInit();
        nestedAggOutputBuilder.reset();
        toWrite = frameMaxObjects;
    }

    @Override
    protected void partitionChunkImpl(long frameId, ByteBuffer frameBuffer, int tBeginIdx, int tEndIdx)
            throws HyracksDataException {
        super.partitionChunkImpl(frameId, frameBuffer, tBeginIdx, tEndIdx);
        tAccess.reset(frameBuffer);
        for (int t = tBeginIdx; t <= tEndIdx && toWrite != 0; t++) {
            nestedAggAggregate(tAccess, t);
            if (toWrite > 0) {
                toWrite--;
            }
        }
    }

    @Override
    protected void endPartitionImpl() throws HyracksDataException {
        nestedAggOutputFinalResult(nestedAggOutputBuilder);
        super.endPartitionImpl();
    }

    @Override
    protected void produceTuple(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
            FrameTupleReference tupleRef) throws HyracksDataException {
        super.produceTuple(tb, accessor, tIndex, tupleRef);
        TupleUtils.addFields(nestedAggOutputBuilder, tb);
    }
}
