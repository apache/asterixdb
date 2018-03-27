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
package org.apache.hyracks.dataflow.std.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;

public class InMemorySortOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int SORT_ACTIVITY_ID = 0;
    private static final int MERGE_ACTIVITY_ID = 1;

    private final int[] sortFields;
    private final INormalizedKeyComputerFactory[] keyNormalizerFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;

    public InMemorySortOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, sortFields, null, comparatorFactories, recordDescriptor);
    }

    public InMemorySortOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.sortFields = sortFields;
        this.keyNormalizerFactories = keyNormalizerFactories;
        this.comparatorFactories = comparatorFactories;
        outRecDescs[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SortActivity sa = new SortActivity(new ActivityId(odId, SORT_ACTIVITY_ID));
        MergeActivity ma = new MergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    private static class SortTaskState extends AbstractStateObject {
        private FrameSorterMergeSort frameSorter;

        private SortTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
        }
    }

    private class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SortActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private SortTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new SortTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));

                    IFrameBufferManager frameBufferManager = new VariableFrameMemoryManager(
                            new VariableFramePool(ctx, VariableFramePool.UNLIMITED_MEMORY),
                            FrameFreeSlotPolicyFactory.createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT));

                    state.frameSorter =
                            new FrameSorterMergeSort(ctx, frameBufferManager, VariableFramePool.UNLIMITED_MEMORY,
                                    sortFields, keyNormalizerFactories, comparatorFactories, outRecDescs[0]);
                    state.frameSorter.reset();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (!state.frameSorter.insertFrame(buffer)) {
                        throw new HyracksDataException("Failed to insert the given frame into sorting buffer. "
                                + "Please increase the sorting memory budget to enable the in-memory sorting, "
                                + "or you could use ExternalSort instead.");
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    state.frameSorter.sort();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
            return op;
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    try {
                        writer.open();
                        SortTaskState state = (SortTaskState) ctx.getStateObject(
                                new TaskId(new ActivityId(getOperatorId(), SORT_ACTIVITY_ID), partition));
                        state.frameSorter.flush(writer);
                    } catch (Throwable th) {
                        writer.fail();
                        throw HyracksDataException.create(th);
                    } finally {
                        writer.close();
                    }
                }
            };
            return op;
        }
    }
}
