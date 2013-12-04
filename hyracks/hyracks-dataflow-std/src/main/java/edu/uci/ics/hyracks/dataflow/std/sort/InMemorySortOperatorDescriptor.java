/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class InMemorySortOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int SORT_ACTIVITY_ID = 0;
    private static final int MERGE_ACTIVITY_ID = 1;

    private final int[] sortFields;
    private INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private IBinaryComparatorFactory[] comparatorFactories;

    public InMemorySortOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, sortFields, null, comparatorFactories, recordDescriptor);
    }

    public InMemorySortOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.sortFields = sortFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        recordDescriptors[0] = recordDescriptor;
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

    public static class SortTaskState extends AbstractStateObject {
        private FrameSorterMergeSort frameSorter;

        public SortTaskState() {
        }

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
                    state = new SortTaskState(ctx.getJobletContext().getJobId(), new TaskId(getActivityId(), partition));
                    state.frameSorter = new FrameSorterMergeSort(ctx, sortFields, firstKeyNormalizerFactory,
                            comparatorFactories, recordDescriptors[0]);
                    state.frameSorter.reset();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.frameSorter.insertFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.frameSorter.sortFrames();
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
                    writer.open();
                    try {
                        SortTaskState state = (SortTaskState) ctx.getStateObject(new TaskId(new ActivityId(
                                getOperatorId(), SORT_ACTIVITY_ID), partition));
                        state.frameSorter.flushFrames(writer);
                    } catch (Exception e) {
                        writer.fail();
                        throw new HyracksDataException(e);
                    } finally {
                        writer.close();
                    }
                }
            };
            return op;
        }
    }
}