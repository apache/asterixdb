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

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
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
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.join.MergeJoinLocks;

public class IntervalIndexJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int LEFT_ACTIVITY_ID = 0;
    private static final int RIGHT_ACTIVITY_ID = 1;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final int memoryForJoin;
    private final IIntervalMergeJoinCheckerFactory imjcf;

    private static final Logger LOGGER = Logger.getLogger(IntervalIndexJoinOperatorDescriptor.class.getName());

    public IntervalIndexJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoin, int[] leftKeys,
            int[] rightKeys, RecordDescriptor recordDescriptor, IIntervalMergeJoinCheckerFactory imjcf) {
        super(spec, 2, 1);
        recordDescriptors[0] = recordDescriptor;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.memoryForJoin = memoryForJoin;
        this.imjcf = imjcf;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MergeJoinLocks locks = new MergeJoinLocks();

        ActivityId leftAid = new ActivityId(odId, LEFT_ACTIVITY_ID);
        ActivityId rightAid = new ActivityId(odId, RIGHT_ACTIVITY_ID);

        IActivity leftAN = new LeftJoinerActivityNode(leftAid, rightAid, locks);
        IActivity rightAN = new RightDataActivityNode(rightAid, leftAid, locks);

        builder.addActivity(this, rightAN);
        builder.addSourceEdge(1, rightAN, 0);

        builder.addActivity(this, leftAN);
        builder.addSourceEdge(0, leftAN, 0);
        builder.addTargetEdge(0, leftAN, 0);
    }

    private class LeftJoinerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final MergeJoinLocks locks;

        public LeftJoinerActivityNode(ActivityId id, ActivityId joinAid, MergeJoinLocks locks) {
            super(id);
            this.locks = locks;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            locks.setPartitions(nPartitions);
            final RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new LeftJoinerOperator(ctx, partition, inRecordDesc);
        }

        private class LeftJoinerOperator extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

            private final IHyracksTaskContext ctx;
            private final int partition;
            private final RecordDescriptor leftRd;
            private IndexJoinTaskState state;
            private boolean first = true;

            public LeftJoinerOperator(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecordDesc) {
                this.ctx = ctx;
                this.partition = partition;
                this.leftRd = inRecordDesc;
            }

            @Override
            public void open() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    writer.open();
                    state = new IndexJoinTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));;
                    state.leftRd = leftRd;
                    state.point = imjcf.isOrderAsc() ? EndPointIndexItem.START_POINT : EndPointIndexItem.END_POINT;
                    state.endPointComparator = imjcf.isOrderAsc() ? EndPointIndexItem.EndPointAscComparator
                            : EndPointIndexItem.EndPointDescComparator;
                    ctx.setStateObject(state);
                    locks.getRight(partition).signal();

                    do {
                        // Continue after joiner created in right branch.
                        if (state.indexJoiner == null) {
                            locks.getLeft(partition).await();
                        }
                    } while (state.indexJoiner == null);
                    state.status.branch[LEFT_ACTIVITY_ID].setStageOpen();
                    locks.getRight(partition).signal();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                locks.getLock(partition).lock();
                if (first) {
                    state.status.branch[LEFT_ACTIVITY_ID].setStageData();
                    first = false;
                }
                try {
                    state.indexJoiner.setFrame(LEFT_ACTIVITY_ID, buffer);
                    state.indexJoiner.processMergeUsingLeftTuple(writer);
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    state.failed = true;
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            @Override
            public void close() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    state.status.branch[LEFT_ACTIVITY_ID].noMore();
                    if (state.failed) {
                        writer.fail();
                    } else {
                        state.indexJoiner.processMergeUsingLeftTuple(writer);
                        state.indexJoiner.closeResult(writer);
                        writer.close();
                    }
                    state.status.branch[LEFT_ACTIVITY_ID].setStageClose();
                    locks.getRight(partition).signal();
                } finally {
                    locks.getLock(partition).unlock();
                }
            }
        }
    }

    private class RightDataActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId joinAid;
        private MergeJoinLocks locks;

        public RightDataActivityNode(ActivityId id, ActivityId joinAid, MergeJoinLocks locks) {
            super(id);
            this.joinAid = joinAid;
            this.locks = locks;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            locks.setPartitions(nPartitions);
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new RightDataOperator(ctx, partition, inRecordDesc);
        }

        private class RightDataOperator extends AbstractUnaryInputSinkOperatorNodePushable {

            private int partition;
            private IHyracksTaskContext ctx;
            private final RecordDescriptor rightRd;
            private IndexJoinTaskState state;
            private boolean first = true;

            public RightDataOperator(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecordDesc) {
                this.ctx = ctx;
                this.partition = partition;
                this.rightRd = inRecordDesc;
            }

            @Override
            public void open() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    do {
                        // Wait for the state to be set in the context form Left.
                        state = (IndexJoinTaskState) ctx.getStateObject(new TaskId(joinAid, partition));
                        if (state == null) {
                            locks.getRight(partition).await();
                        }
                    } while (state == null);
                    state.rightRd = rightRd;
                    state.indexJoiner = new IntervalIndexJoiner(ctx, memoryForJoin, partition, state.status, locks,
                            state.endPointComparator, imjcf, leftKeys, rightKeys, state.leftRd, state.rightRd);
                    state.status.branch[RIGHT_ACTIVITY_ID].setStageOpen();
                    locks.getLeft(partition).signal();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                locks.getLock(partition).lock();
                if (first) {
                    state.status.branch[RIGHT_ACTIVITY_ID].setStageData();
                    first = false;
                }
                try {
                    while (!state.status.continueRightLoad && state.status.branch[LEFT_ACTIVITY_ID].hasMore()) {
                        // Wait for the state to request right frame unless left has finished.
                        locks.getRight(partition).await();
                    }
                    state.indexJoiner.setFrame(RIGHT_ACTIVITY_ID, buffer);
                    state.status.continueRightLoad = false;
                    locks.getLeft(partition).signal();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    state.failed = true;
                    locks.getLeft(partition).signal();
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            @Override
            public void close() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    state.status.branch[RIGHT_ACTIVITY_ID].setStageClose();
                    locks.getLeft(partition).signal();
                } finally {
                    locks.getLock(partition).unlock();
                }
            }
        }
    }
}