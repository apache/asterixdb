/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

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
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractTaskState;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class HashGroupOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int HASH_BUILD_ACTIVITY_ID = 0;

    private static final int OUTPUT_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;

    private final int[] keys;
    private final ITuplePartitionComputerFactory tpcf;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAccumulatingAggregatorFactory aggregatorFactory;
    private final int tableSize;

    public HashGroupOperatorDescriptor(JobSpecification spec, int[] keys, ITuplePartitionComputerFactory tpcf,
            IBinaryComparatorFactory[] comparatorFactories, IAccumulatingAggregatorFactory aggregatorFactory,
            RecordDescriptor recordDescriptor, int tableSize) {
        super(spec, 1, 1);
        this.keys = keys;
        this.tpcf = tpcf;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        recordDescriptors[0] = recordDescriptor;
        this.tableSize = tableSize;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        HashBuildActivity ha = new HashBuildActivity(new ActivityId(odId, HASH_BUILD_ACTIVITY_ID));
        builder.addActivity(ha);

        OutputActivity oa = new OutputActivity(new ActivityId(odId, OUTPUT_ACTIVITY_ID));
        builder.addActivity(oa);

        builder.addSourceEdge(0, ha, 0);
        builder.addTargetEdge(0, oa, 0);
        builder.addBlockingEdge(ha, oa);
    }

    public static class HashBuildActivityState extends AbstractTaskState {
        private GroupingHashTable table;

        public HashBuildActivityState() {
        }

        private HashBuildActivityState(JobId jobId, TaskId tId) {
            super(jobId, tId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }
    }

    private class HashBuildActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public HashBuildActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IRecordDescriptorProvider recordDescProvider,
                final int partition, int nPartitions) {
            final FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(),
                    recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0));
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private HashBuildActivityState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new HashBuildActivityState(ctx.getJobletContext().getJobId(), new TaskId(getActivityId(),
                            partition));
                    state.table = new GroupingHashTable(ctx, keys, comparatorFactories, tpcf, aggregatorFactory,
                            recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0), recordDescriptors[0],
                            tableSize);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; ++i) {
                        state.table.insert(accessor, i);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    ctx.setTaskState(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    private class OutputActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public OutputActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    HashBuildActivityState buildState = (HashBuildActivityState) ctx.getTaskState(new TaskId(
                            new ActivityId(getOperatorId(), HASH_BUILD_ACTIVITY_ID), partition));
                    GroupingHashTable table = buildState.table;
                    writer.open();
                    try {
                        table.write(writer);
                    } catch (Exception e) {
                        writer.fail();
                        throw new HyracksDataException(e);
                    } finally {
                        writer.close();
                    }
                }
            };
        }
    }
}