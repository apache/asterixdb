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
package edu.uci.ics.hyracks.dataflow.std.join;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;

public class GraceHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int RPARTITION_ACTIVITY_ID = 0;
    private static final int SPARTITION_ACTIVITY_ID = 1;
    private static final int JOIN_ACTIVITY_ID = 2;

    private static final long serialVersionUID = 1L;
    private final int[] keys0;
    private final int[] keys1;
    private final int inputsize0;
    private final int recordsPerFrame;
    private final int memsize;
    private final double factor;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final boolean isLeftOuter;
    private final INullWriterFactory[] nullWriterFactories1;

    public GraceHashJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0, int recordsPerFrame,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.recordsPerFrame = recordsPerFrame;
        this.factor = factor;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        this.isLeftOuter = false;
        this.nullWriterFactories1 = null;
        recordDescriptors[0] = recordDescriptor;
    }

    public GraceHashJoinOperatorDescriptor(JobSpecification spec, int memsize, int inputsize0, int recordsPerFrame,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, boolean isLeftOuter,
            INullWriterFactory[] nullWriterFactories1) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.recordsPerFrame = recordsPerFrame;
        this.factor = factor;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        this.isLeftOuter = isLeftOuter;
        this.nullWriterFactories1 = nullWriterFactories1;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        HashPartitionActivityNode rpart = new HashPartitionActivityNode(new ActivityId(odId, RPARTITION_ACTIVITY_ID),
                keys0, 0);
        HashPartitionActivityNode spart = new HashPartitionActivityNode(new ActivityId(odId, SPARTITION_ACTIVITY_ID),
                keys1, 1);
        JoinActivityNode join = new JoinActivityNode(new ActivityId(odId, JOIN_ACTIVITY_ID));

        builder.addActivity(rpart);
        builder.addSourceEdge(0, rpart, 0);

        builder.addActivity(spart);
        builder.addSourceEdge(1, spart, 0);

        builder.addActivity(join);
        builder.addBlockingEdge(rpart, spart);
        builder.addBlockingEdge(spart, join);

        builder.addTargetEdge(0, join, 0);
    }

    public int getMemorySize() {
        return memsize;
    }

    private class HashPartitionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private int operatorInputIndex;
        private int keys[];

        public HashPartitionActivityNode(ActivityId id, int keys[], int operatorInputIndex) {
            super(id);
            this.keys = keys;
            this.operatorInputIndex = operatorInputIndex;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new GraceHashJoinPartitionBuildOperatorNodePushable(ctx, new TaskId(getActivityId(), partition),
                    keys, hashFunctionFactories, comparatorFactories, (int) Math.ceil(Math.sqrt(inputsize0 * factor
                            / nPartitions)), recordDescProvider.getInputRecordDescriptor(getOperatorId(),
                            operatorInputIndex));
        }
    }

    private class JoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public JoinActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            int numPartitions = (int) Math.ceil(Math.sqrt(inputsize0 * factor / nPartitions));

            return new GraceHashJoinOperatorNodePushable(ctx, new TaskId(new ActivityId(getOperatorId(),
                    RPARTITION_ACTIVITY_ID), partition), new TaskId(new ActivityId(getOperatorId(),
                    SPARTITION_ACTIVITY_ID), partition), recordsPerFrame, factor, keys0, keys1, hashFunctionFactories,
                    comparatorFactories, nullWriterFactories1, rd1, rd0, recordDescriptors[0], numPartitions,
                    isLeftOuter);
        }
    }
}