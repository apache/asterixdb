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

package org.apache.asterix.runtime.operators.joins.intervalindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.buffermanager.IFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleBufferAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class IntervalIndexJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int BUILD_LEFT_ACTIVITY_ID = 0;
    private static final int BUILD_RIGHT_ACTIVITY_ID = 1;
    private static final int JOIN_ACTIVITY_ID = 2;

    private final int memsize;
    private final int leftKey;
    private final int rightKey;
    private final int[] leftKeys;
    private final int[] rightKeys;

    private final int leftCountInFrames;
    private final int rightCountInFrames;
    private final IIntervalMergeJoinCheckerFactory imjcf;

    private static final Logger LOGGER = Logger.getLogger(IntervalIndexJoinOperatorDescriptor.class.getName());

    public IntervalIndexJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memsize, int leftCountInFrames,
            int rightCountInFrames, int[] leftKeys, int[] rightKeys, RecordDescriptor recordDescriptor,
            IIntervalMergeJoinCheckerFactory imjcf) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.leftCountInFrames = leftCountInFrames;
        this.rightCountInFrames = rightCountInFrames;
        this.leftKey = leftKeys[0];
        this.rightKey = rightKeys[0];
        this.rightKeys = leftKeys;
        this.leftKeys = rightKeys;
        recordDescriptors[0] = recordDescriptor;
        this.imjcf = imjcf;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId leftAid = new ActivityId(odId, BUILD_LEFT_ACTIVITY_ID);
        ActivityId rightAid = new ActivityId(odId, BUILD_RIGHT_ACTIVITY_ID);
        ActivityId joinAid = new ActivityId(odId, JOIN_ACTIVITY_ID);

        IActivity phaseBuildLeft = new BuildIndexActivityNode(leftAid, leftKey);
        IActivity phaseBuildRight = new BuildIndexActivityNode(rightAid, rightKey);
        IActivity phaseJoin = new JoinActivityNode(joinAid, leftAid, rightAid);

        builder.addActivity(this, phaseBuildLeft);
        builder.addSourceEdge(0, phaseBuildLeft, 0);

        builder.addActivity(this, phaseBuildRight);
        builder.addSourceEdge(1, phaseBuildRight, 0);

        builder.addActivity(this, phaseJoin);
        builder.addTargetEdge(0, phaseJoin, 0);

        builder.addBlockingEdge(phaseBuildLeft, phaseJoin);
        builder.addBlockingEdge(phaseBuildRight, phaseJoin);
    }

    public static class IndexTaskState extends AbstractStateObject {
        private int partition;
        private int memoryForJoin;
        protected ITupleBufferManager bufferManager;
        protected PriorityQueue<EndPointIndexItem> indexQueue;
        protected byte point;
        protected Comparator<EndPointIndexItem> endPointComparator;

        public IndexTaskState() {
        }

        private IndexTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }

    }

    private class BuildIndexActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final int key;

        public BuildIndexActivityNode(ActivityId id, int key) {
            super(id);
            this.key = key;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {

            final RecordDescriptor rd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final FrameTupleAccessor accessor = new FrameTupleAccessor(rd);

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private IndexTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new IndexTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    ctx.setStateObject(state);
                    if (memsize <= 2) {
                        // Dedicated buffers: One buffer to read and one buffer for output
                        throw new HyracksDataException("not enough memory for join");
                    }
                    state.partition = partition;
                    state.memoryForJoin = memsize / 2;
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("IntervalPartitionJoin is starting the build phase with " + state.partition
                                + " interval partitions using " + state.memoryForJoin + " frames for memory.");
                    }
                    IFramePool framePool = new VariableFramePool(ctx, state.memoryForJoin * ctx.getInitialFrameSize());
                    state.bufferManager = new VariableDeletableTupleMemoryManager(framePool, rd);
                    state.point = imjcf.isOrderAsc() ? EndPointIndexItem.START_POINT : EndPointIndexItem.END_POINT;
                    state.endPointComparator = imjcf.isOrderAsc() ? EndPointIndexItem.EndPointAscComparator
                            : EndPointIndexItem.EndPointDescComparator;
                    state.indexQueue = new PriorityQueue<EndPointIndexItem>(16, state.endPointComparator);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; ++i) {
                        TuplePointer tuplePointer = new TuplePointer();
                        if (state.bufferManager.insertTuple(accessor, i, tuplePointer)) {
                            EndPointIndexItem s = new EndPointIndexItem(tuplePointer, EndPointIndexItem.START_POINT,
                                    IntervalJoinUtil.getIntervalStart(accessor, i, key));
                            state.indexQueue.add(s);
                            EndPointIndexItem e = new EndPointIndexItem(tuplePointer, EndPointIndexItem.END_POINT,
                                    IntervalJoinUtil.getIntervalEnd(accessor, i, key));
                            state.indexQueue.add(e);
                        }
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("IntervalPartitionJoin closed its build phase");
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                }

            };
            return op;
        }
    }

    private class JoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId leftAid;
        private final ActivityId rightAid;

        public JoinActivityNode(ActivityId id, ActivityId leftAid, ActivityId rightAid) {
            super(id);
            this.leftAid = leftAid;
            this.rightAid = rightAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {
            FrameTupleAppender resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));

            LinkedList<TuplePointer> leftActive = new LinkedList<TuplePointer>();
            LinkedList<TuplePointer> rightActive = new LinkedList<TuplePointer>();
            LinkedList<TuplePointer> buffer = new LinkedList<TuplePointer>();

            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    IndexTaskState leftState = (IndexTaskState) ctx.getStateObject(new TaskId(leftAid, partition));
                    IndexTaskState rightState = (IndexTaskState) ctx.getStateObject(new TaskId(rightAid, partition));
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("IntervalIndexJoin is starting the join phase.");
                    }
                    IIntervalMergeJoinChecker imjc = imjcf.createMergeJoinChecker(rightKeys, leftKeys, partition);

                    ITupleBufferAccessor leftAccessor = leftState.bufferManager.getTupleAccessor();
                    ITupleBufferAccessor rightAccessor = rightState.bufferManager.getTupleAccessor();

                    EndPointIndexItem leftItem;
                    EndPointIndexItem rightItem;

                    try {
                        writer.open();

                        leftItem = leftState.indexQueue.poll();
                        rightItem = rightState.indexQueue.poll();

                        while (leftItem != null && rightItem != null) {

                            if (leftState.endPointComparator.compare(leftItem, rightItem) < 0) {
                                // Process endpoints
                                do {
                                    if (leftItem.getStart() == leftState.point) {
                                        leftActive.add(leftItem.getTuplePointer());
                                        buffer.add(leftItem.getTuplePointer());
                                    } else if (imjc.checkToRemoveLeftActive()) {
                                        // Conditionally remove based on after/before interval condition.
                                        leftActive.remove(leftItem.getTuplePointer());
                                    }
                                    leftItem = leftState.indexQueue.poll();
                                } while (leftItem != null
                                        && leftState.endPointComparator.compare(rightItem, leftItem) >= 0);

                                // Add Results
                                if (!buffer.isEmpty()) {
                                    for (TuplePointer rightTp : rightActive) {
                                        rightAccessor.reset(rightTp);
                                        for (TuplePointer leftTp : buffer) {
                                            leftAccessor.reset(leftTp);
                                            if (imjc.checkToSaveInResult(leftAccessor, leftTp.tupleIndex, rightAccessor,
                                                    rightTp.tupleIndex)) {
                                                addToResult(leftAccessor, leftTp.tupleIndex, rightAccessor,
                                                        rightTp.tupleIndex, writer);
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Process endpoints
                                do {
                                    if (rightItem.getStart() == leftState.point) {
                                        rightActive.add(rightItem.getTuplePointer());
                                        buffer.add(rightItem.getTuplePointer());
                                    } else if (imjc.checkToRemoveRightActive()) {
                                        // Conditionally remove based on after/before interval condition.
                                        rightActive.remove(rightItem.getTuplePointer());
                                    }
                                    rightItem = rightState.indexQueue.poll();
                                } while (rightItem != null
                                        && leftState.endPointComparator.compare(leftItem, rightItem) >= 0);

                                // Add Results
                                if (!buffer.isEmpty()) {
                                    for (TuplePointer leftTp : leftActive) {
                                        leftAccessor.reset(leftTp);
                                        for (TuplePointer rightTp : buffer) {
                                            rightAccessor.reset(rightTp);
                                            if (imjc.checkToSaveInResult(leftAccessor, leftTp.tupleIndex, rightAccessor,
                                                    rightTp.tupleIndex)) {
                                                addToResult(leftAccessor, leftTp.tupleIndex, rightAccessor,
                                                        rightTp.tupleIndex, writer);
                                            }
                                        }
                                    }
                                }
                            }
                            buffer.clear();
                        }
                        closeResult(writer);
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("IntervalIndexJoin closed its join phase");
                        }
                    } catch (Throwable th) {
                        writer.fail();
                        throw new HyracksDataException(th);
                    } finally {
                        writer.close();
                    }
                }

                private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2,
                        int index2, IFrameWriter writer) throws HyracksDataException {
                    FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
                }

                public void closeResult(IFrameWriter writer) throws HyracksDataException {
                    resultAppender.write(writer, true);
                }

            };
        }
    }
}