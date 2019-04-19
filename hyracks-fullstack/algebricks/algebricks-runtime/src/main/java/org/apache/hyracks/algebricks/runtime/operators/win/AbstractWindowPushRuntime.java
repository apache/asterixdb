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
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PointableTupleReference;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;

public abstract class AbstractWindowPushRuntime extends AbstractRunningAggregatePushRuntime<IWindowAggregateEvaluator> {

    protected final SourceLocation sourceLoc;
    private final int[] partitionColumns;
    private final IBinaryComparatorFactory[] partitionComparatorFactories;
    private IBinaryComparator[] partitionComparators;
    private final IBinaryComparatorFactory[] orderComparatorFactories;
    private FrameTupleAccessor frameAccessor;
    private FrameTupleReference partitionColumnsRef;
    private PointableTupleReference partitionColumnsPrevCopy;
    private long frameId;
    private boolean inPartition;

    AbstractWindowPushRuntime(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, IHyracksTaskContext ctx,
            SourceLocation sourceLoc) {
        super(projectionColumns, runningAggOutColumns, runningAggFactories, IWindowAggregateEvaluator.class, ctx);
        this.partitionColumns = partitionColumns;
        this.partitionComparatorFactories = partitionComparatorFactories;
        this.orderComparatorFactories = orderComparatorFactories;
        this.sourceLoc = sourceLoc;
    }

    /**
     * Number of frames reserved by this operator: {@link #frame} + conservative estimate for
     * {@link #partitionColumnsPrevCopy}
     */
    int getReservedFrameCount() {
        return 2;
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
        partitionColumnsRef = new PermutingFrameTupleReference(partitionColumns);
        partitionColumnsPrevCopy =
                PointableTupleReference.create(partitionColumns.length, ArrayBackedValueStorage::new);
        IBinaryComparator[] orderComparators = createBinaryComparators(orderComparatorFactories);
        for (IWindowAggregateEvaluator runningAggEval : runningAggEvals) {
            runningAggEval.configure(orderComparators);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (inPartition && !failed) {
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
            boolean samePartition = PreclusteredGroupWriter.sameGroup(partitionColumnsPrevCopy, frameAccessor, 0,
                    partitionColumns, partitionComparators);
            if (!samePartition) {
                endPartition();
                beginPartition();
            }
        }
        int tLastIndex = nTuple - 1;
        if (tLastIndex == 0) {
            partitionChunk(frameId, buffer, 0, 0);
        } else {
            int tBeginIndex = 0;
            for (int tIndex = 1; tIndex <= tLastIndex; tIndex++) {
                partitionColumnsRef.reset(frameAccessor, tIndex - 1);
                boolean samePartition = PreclusteredGroupWriter.sameGroup(partitionColumnsRef, frameAccessor, tIndex,
                        partitionColumns, partitionComparators);
                if (!samePartition) {
                    partitionChunk(frameId, buffer, tBeginIndex, tIndex - 1);
                    endPartition();
                    beginPartition();
                    tBeginIndex = tIndex;
                }
            }
            partitionChunk(frameId, buffer, tBeginIndex, tLastIndex);
        }

        partitionColumnsRef.reset(frameAccessor, tLastIndex);
        partitionColumnsPrevCopy.set(partitionColumnsRef);
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

    static IBinaryComparator[] createBinaryComparators(IBinaryComparatorFactory[] factories) {
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
