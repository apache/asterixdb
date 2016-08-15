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
package org.apache.asterix.algebra.operators.physical;

import java.nio.ByteBuffer;

import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.RangeId;
import org.apache.hyracks.dataflow.std.misc.RangeForwardOperatorDescriptor.RangeForwardTaskState;

public class IntervalLocalRangeOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private static final int PARTITION_ACTIVITY_ID = 0;

    private static final int OUTPUT_ARITY = 3;

    private static final int INPUT_STARTS = 0;
    private static final int INPUT_COVERS = 2;
    private static final int INPUT_ENDS = 1;

    private final int key;
    private final RangeId rangeId;

    public IntervalLocalRangeOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] keys,
            RecordDescriptor recordDescriptor, RangeId rangeId) {
        super(spec, 1, OUTPUT_ARITY);
        for (int i = 0; i < outputArity; i++) {
            recordDescriptors[i] = recordDescriptor;
        }
        key = keys[0];
        this.rangeId = rangeId;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId aid = new ActivityId(odId, PARTITION_ACTIVITY_ID);
        IActivity phase = new PartitionActivityNode(aid);

        builder.addActivity(this, phase);
        builder.addSourceEdge(0, phase, 0);
        // Connect output
        builder.addTargetEdge(0, phase, 0);
        builder.addTargetEdge(1, phase, 1);
        builder.addTargetEdge(2, phase, 2);
    }

    private final class PartitionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public PartitionActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputOperatorNodePushable() {
                private final IFrameWriter[] writers = new IFrameWriter[getOutputArity()];
                private final FrameTupleAppender[] resultAppender = new FrameTupleAppender[getOutputArity()];
                private final RecordDescriptor rd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
                private final FrameTupleAccessor accessor = new FrameTupleAccessor(rd);
                private long nodeRangeStart;
                private long nodeRangeEnd;

                @Override
                public void close() throws HyracksDataException {
                    flush();
                    for (int i = 0; i < getOutputArity(); i++) {
                        writers[i].close();
                    }
                }

                @Override
                public void flush() throws HyracksDataException {
                    for (int i = 0; i < getOutputArity(); i++) {
                        FrameUtils.flushFrame(resultAppender[i].getBuffer(), writers[i]);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    for (int i = 0; i < getOutputArity(); i++) {
                        writers[i].fail();
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        int pid = localPartition(accessor, i, key);
                        if (pid < outputArity) {
                            FrameUtils.appendToWriter(writers[pid], resultAppender[pid], accessor, i);
                        }
                    }
                }

                private int localPartition(FrameTupleAccessor accessor, int i, int key) {
                    long start = IntervalJoinUtil.getIntervalStart(accessor, i, key);
                    if (start < nodeRangeStart) {
                        long end = IntervalJoinUtil.getIntervalEnd(accessor, i, key);
                        if (end < nodeRangeEnd) {
                            // Ends
                            return INPUT_ENDS;
                        } else {
                            // Covers (match will all intervals)
                            return INPUT_COVERS;
                        }
                    } else {
                        // Start (responsible for matches)
                        return INPUT_STARTS;
                    }
                }

                @Override
                public void open() throws HyracksDataException {
                    for (int i = 0; i < getOutputArity(); i++) {
                        writers[i].open();
                        resultAppender[i] = new FrameTupleAppender(new VSizeFrame(ctx), true);
                    }
                    RangeForwardTaskState rangeState = (RangeForwardTaskState) ctx.getStateObject(new RangeId(rangeId.getId(), ctx));
                    IRangeMap rangeMap = rangeState.getRangeMap();
                    nodeRangeStart = getPartitionBoundryStart(rangeMap);
                    nodeRangeEnd = getPartitionBoundryEnd(rangeMap);

                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    writers[index] = writer;
                }

                long getPartitionBoundryStart(IRangeMap rangeMap) {
                    int fieldIndex = 0;
                    int slot = partition - 1;
                    long boundary = Long.MIN_VALUE;
                    // All lookups are on typed values.
                    if (partition == 0) {
                        boundary = LongPointable.getLong(rangeMap.getMinByteArray(fieldIndex),
                                rangeMap.getMinStartOffset(fieldIndex) + 1);
                    } else if (partition <= rangeMap.getSplitCount()) {
                        boundary = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, slot),
                                rangeMap.getStartOffset(fieldIndex, slot) + 1);
                    } else if (partition > rangeMap.getSplitCount()) {
                        boundary = LongPointable.getLong(rangeMap.getMaxByteArray(fieldIndex),
                                rangeMap.getMaxStartOffset(fieldIndex) + 1);
                    }
                    return boundary;
                }

                long getPartitionBoundryEnd(IRangeMap rangeMap) {
                    int fieldIndex = 0;
                    int slot = partition;
                    long boundary = Long.MAX_VALUE;
                    // All lookups are on typed values.
                    if (partition < rangeMap.getSplitCount()) {
                        boundary = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, slot),
                                rangeMap.getStartOffset(fieldIndex, slot) + 1);
                    } else if (partition == rangeMap.getSplitCount()) {
                        boundary = LongPointable.getLong(rangeMap.getMaxByteArray(fieldIndex),
                                rangeMap.getMaxStartOffset(fieldIndex) + 1);
                    }
                    return boundary;
                }
            };
        }
    }

}
