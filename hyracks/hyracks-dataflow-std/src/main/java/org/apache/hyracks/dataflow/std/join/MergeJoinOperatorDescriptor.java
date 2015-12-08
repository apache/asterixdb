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
package org.apache.hyracks.dataflow.std.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * The merge join is made up of two operators: left and right.
 * The right operator loads right stream into memory for the merge process.
 * The left operator streams the left input and the right memory store to merge and join the data.
 *
 * @author prestonc
 */
public class MergeJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int LEFT_ACTIVITY_ID = 0;
    private static final int RIGHT_ACTIVITY_ID = 1;
    private final IMergeJoinCheckerFactory mergeJoinCheckerFactory;
    private final int[] keys0;
    private final int[] keys1;
    private final int memSize;

    public MergeJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memSize, RecordDescriptor recordDescriptor,
            int[] keys0, int[] keys1, IMergeJoinCheckerFactory mergeJoinCheckerFactory) {
        super(spec, 2, 1);
        recordDescriptors[0] = recordDescriptor;
        this.mergeJoinCheckerFactory = mergeJoinCheckerFactory;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.memSize = memSize;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MergeJoinLocks locks = new MergeJoinLocks();
        ActivityId leftAid = new ActivityId(odId, LEFT_ACTIVITY_ID);
        ActivityId rightAid = new ActivityId(odId, RIGHT_ACTIVITY_ID);
        LeftJoinerActivityNode leftAN = new LeftJoinerActivityNode(leftAid, rightAid, locks);
        RightDataActivityNode rightAN = new RightDataActivityNode(rightAid, leftAid, locks);

        builder.addActivity(this, rightAN);
        builder.addSourceEdge(1, rightAN, 0);

        builder.addActivity(this, leftAN);
        builder.addSourceEdge(0, leftAN, 0);
        builder.addTargetEdge(0, leftAN, 0);
    }

    public static class SortMergeIntervalJoinTaskState extends AbstractStateObject {
        private MergeStatus status;
        private MergeJoiner joiner;
        private boolean failed;

        private SortMergeIntervalJoinTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
            status = new MergeStatus();
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
        }
    }

    private class LeftJoinerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId joinAid;
        private final MergeJoinLocks locks;

        public LeftJoinerActivityNode(ActivityId id, ActivityId joinAid, MergeJoinLocks locks) {
            super(id);
            this.joinAid = joinAid;
            this.locks = locks;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                        throws HyracksDataException {
            locks.setPartitions(nPartitions);
            final RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            IMergeJoinChecker mjc = mergeJoinCheckerFactory.createMergeJoinChecker(keys0, keys1, partition);
            return new LeftJoinerOperator(ctx, partition, inRecordDesc, mjc);
        }

        private class LeftJoinerOperator extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

            private final IHyracksTaskContext ctx;
            private final int partition;
            private final RecordDescriptor leftRD;
            private final IMergeJoinChecker mjc;

            public LeftJoinerOperator(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecordDesc,
                    IMergeJoinChecker mjc) {
                this.ctx = ctx;
                this.partition = partition;
                this.leftRD = inRecordDesc;
                this.mjc = mjc;
            }

            private SortMergeIntervalJoinTaskState state;
            private boolean first = true;

            public void open() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    writer.open();
                    state = new SortMergeIntervalJoinTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.status.openLeft();
                    state.joiner = new MergeJoiner(ctx, memSize, partition, state.status, locks, mjc, leftRD);
                    ctx.setStateObject(state);
                    locks.getRight(partition).signal();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                locks.getLock(partition).lock();
                if (first) {
                    state.status.dataLeft();
                    first = false;
                }
                try {
                    state.joiner.setLeftFrame(buffer);
                    state.joiner.processMergeUsingLeftTuple(writer);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            public void fail() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    state.failed = true;
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            public void close() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    state.status.leftHasMore = false;
                    if (state.failed) {
                        writer.fail();
                    } else {
                        state.joiner.processMergeUsingLeftTuple(writer);
                        state.joiner.closeResult(writer);
                        writer.close();
                    }
                    state.status.closeLeft();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
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
            private final RecordDescriptor rightRD;

            public RightDataOperator(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecordDesc) {
                this.ctx = ctx;
                this.partition = partition;
                this.rightRD = inRecordDesc;
            }

            private SortMergeIntervalJoinTaskState state;
            private boolean first = true;

            @Override
            public void open() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    do {
                        // Wait for the state to be set in the context form Left.
                        state = (SortMergeIntervalJoinTaskState) ctx.getStateObject(new TaskId(joinAid, partition));
                        if (state == null) {
                            locks.getRight(partition).await();
                        }
                    } while (state == null);
                    state.joiner.setRightRecordDescriptor(rightRD);
                    state.status.openRight();
                } catch (InterruptedException e) {
                    throw new HyracksDataException("RightOperator interrupted exceptrion", e);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                locks.getLock(partition).lock();
                if (first) {
                    state.status.dataRight();
                    first = false;
                }
                try {
                    while (state.status.loadRightFrame == false && state.status.leftHasMore == true) {
                        // Wait for the state to request right frame unless left has finished.
                        locks.getRight(partition).await();
                    };
                    state.joiner.setRightFrame(buffer);
                    locks.getLeft(partition).signal();
                } catch (InterruptedException e) {
                    throw new HyracksDataException("RightOperator interrupted exceptrion", e);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
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
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } finally {
                    locks.getLock(partition).unlock();
                }
            }

            @Override
            public void close() throws HyracksDataException {
                locks.getLock(partition).lock();
                try {
                    state.status.closeRight();
                    locks.getLeft(partition).signal();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } finally {
                    locks.getLock(partition).unlock();
                }
            }
        }
    }

}