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
package org.apache.hyracks.dataflow.std.group.hash;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

/**
 *
 */
public class HashGroupOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final int HASH_BUILD_ACTIVITY_ID = 0;

    private static final int OUTPUT_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;

    private final int[] keys;
    private final ITuplePartitionComputerFactory tpcf;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private final IAggregatorDescriptorFactory aggregatorFactory;

    private final int tableSize;

    public HashGroupOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] keys,
            ITuplePartitionComputerFactory tpcf, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor outRecordDescriptor, int tableSize) {
        super(spec, 1, 1);
        this.keys = keys;
        this.tpcf = tpcf;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        recordDescriptors[0] = outRecordDescriptor;
        this.tableSize = tableSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hyracks.api.dataflow.IOperatorDescriptor#contributeActivities
     * (org.apache.hyracks.api.dataflow.IActivityGraphBuilder)
     */
    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        HashBuildActivity ha = new HashBuildActivity(new ActivityId(odId, HASH_BUILD_ACTIVITY_ID));
        builder.addActivity(this, ha);

        OutputActivity oa = new OutputActivity(new ActivityId(odId, OUTPUT_ACTIVITY_ID));
        builder.addActivity(this, oa);

        builder.addSourceEdge(0, ha, 0);
        builder.addTargetEdge(0, oa, 0);
        builder.addBlockingEdge(ha, oa);
    }

    private class HashBuildActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public HashBuildActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new HashGroupBuildOperatorNodePushable(ctx, new TaskId(getActivityId(), partition), keys, tpcf,
                    comparatorFactories, aggregatorFactory, tableSize, recordDescProvider.getInputRecordDescriptor(
                            getActivityId(), 0), recordDescriptors[0]);
        }
    }

    private class OutputActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public OutputActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new HashGroupOutputOperatorNodePushable(ctx, new TaskId(new ActivityId(getOperatorId(),
                    HASH_BUILD_ACTIVITY_ID), partition));
        }
    }
}