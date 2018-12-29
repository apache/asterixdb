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
import org.apache.hyracks.algebricks.runtime.base.IWindowAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.operators.aggrun.AbstractRunningAggregatePushRuntime;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;

public abstract class AbstractWindowPushRuntime extends AbstractRunningAggregatePushRuntime<IWindowAggregateEvaluator> {

    private final int[] partitionColumns;
    private final IBinaryComparatorFactory[] partitionComparatorFactories;
    private IBinaryComparator[] partitionComparators;
    private final IBinaryComparatorFactory[] orderComparatorFactories;
    private IFrame copyFrame;
    private FrameTupleAccessor copyFrameAccessor;
    private FrameTupleAccessor frameAccessor;
    private long frameId;
    private boolean inPartition;

    AbstractWindowPushRuntime(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, IHyracksTaskContext ctx) {
        super(projectionColumns, runningAggOutColumns, runningAggFactories, IWindowAggregateEvaluator.class, ctx);
        this.partitionColumns = partitionColumns;
        this.partitionComparatorFactories = partitionComparatorFactories;
        this.orderComparatorFactories = orderComparatorFactories;
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        frameId = 0;
        inPartition = false;
    }

    @Override
    protected void init() throws HyracksDataException {
        super.init();
        partitionComparators = createBinaryComparators(partitionComparatorFactories);
        frameAccessor = new FrameTupleAccessor(inputRecordDesc);
        copyFrame = new VSizeFrame(ctx);
        copyFrameAccessor = new FrameTupleAccessor(inputRecordDesc);
        copyFrameAccessor.reset(copyFrame.getBuffer());
        IBinaryComparator[] orderComparators = createBinaryComparators(orderComparatorFactories);
        for (IWindowAggregateEvaluator runningAggEval : runningAggEvals) {
            runningAggEval.configure(orderComparators);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (inPartition) {
            endPartition();
        }
        super.close();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        frameAccessor.reset(buffer);
        int nTuple = frameAccessor.getTupleCount();
        if (nTuple == 0) {
            return;
        }

        if (frameId == 0) {
            beginPartition();
        } else {
            boolean samePartition = PreclusteredGroupWriter.sameGroup(copyFrameAccessor,
                    copyFrameAccessor.getTupleCount() - 1, frameAccessor, 0, partitionColumns, partitionComparators);
            if (!samePartition) {
                endPartition();
                beginPartition();
            }
        }
        if (nTuple == 1) {
            partitionChunk(frameId, buffer, 0, 0);
        } else {
            int tBeginIndex = 0;
            int tLastIndex = nTuple - 1;
            for (int tIndex = 1; tIndex <= tLastIndex; tIndex++) {
                boolean samePartition = PreclusteredGroupWriter.sameGroup(frameAccessor, tIndex - 1, frameAccessor,
                        tIndex, partitionColumns, partitionComparators);
                if (!samePartition) {
                    partitionChunk(frameId, buffer, tBeginIndex, tIndex - 1);
                    endPartition();
                    beginPartition();
                    tBeginIndex = tIndex;
                }
            }
            partitionChunk(frameId, buffer, tBeginIndex, tLastIndex);
        }

        copyFrame.resize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, copyFrame.getBuffer());
        copyFrameAccessor.reset(copyFrame.getBuffer());
        frameId++;
    }

    private void beginPartition() throws HyracksDataException {
        if (inPartition) {
            throw new IllegalStateException();
        }
        inPartition = true;
        beginPartitionImpl();
    }

    private void partitionChunk(long frameId, ByteBuffer frameBuffer, int beginTupleIdx, int endTupleIdx)
            throws HyracksDataException {
        if (!inPartition || frameId < 0) {
            throw new IllegalStateException();
        }
        partitionChunkImpl(frameId, frameBuffer, beginTupleIdx, endTupleIdx);
    }

    private void endPartition() throws HyracksDataException {
        if (!inPartition) {
            throw new IllegalStateException();
        }
        endPartitionImpl();
        inPartition = false;
    }

    void runningAggInitPartition(long partitionLength) throws HyracksDataException {
        for (IWindowAggregateEvaluator runningAggEval : runningAggEvals) {
            runningAggEval.initPartition(partitionLength);
        }
    }

    protected static IBinaryComparator[] createBinaryComparators(IBinaryComparatorFactory[] factories) {
        IBinaryComparator[] comparators = new IBinaryComparator[factories.length];
        for (int i = 0; i < factories.length; i++) {
            comparators[i] = factories[i].createBinaryComparator();
        }
        return comparators;
    }

    protected abstract void beginPartitionImpl() throws HyracksDataException;

    protected abstract void partitionChunkImpl(long frameId, ByteBuffer frameBuffer, int tBeginIdx, int tEndIdx)
            throws HyracksDataException;

    protected abstract void endPartitionImpl() throws HyracksDataException;
}
