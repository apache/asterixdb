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
package edu.uci.ics.hyracks.dataflow.std.group.external;

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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.ISpillableTableFactory;

/**
 *
 */
public class ExternalGroupOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final int AGGREGATE_ACTIVITY_ID = 0;

    private static final int MERGE_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private final int[] keyFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final IAggregatorDescriptorFactory mergerFactory;

    private final int framesLimit;
    private final ISpillableTableFactory spillableTableFactory;
    private final boolean isOutputSorted;

    public ExternalGroupOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] keyFields, int framesLimit,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor, ISpillableTableFactory spillableTableFactory, boolean isOutputSorted) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        if (framesLimit <= 1) {
            /**
             * Minimum of 2 frames: 1 for input records, and 1 for output
             * aggregation results.
             */
            throw new IllegalStateException("frame limit should at least be 2, but it is " + framesLimit + "!");
        }
        this.aggregatorFactory = aggregatorFactory;
        this.mergerFactory = mergerFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.spillableTableFactory = spillableTableFactory;
        this.isOutputSorted = isOutputSorted;

        /**
         * Set the record descriptor. Note that since this operator is a unary
         * operator, only the first record descriptor is used here.
         */
        recordDescriptors[0] = recordDescriptor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor#contributeActivities
     * (edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder)
     */
    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        AggregateActivity aggregateAct = new AggregateActivity(new ActivityId(getOperatorId(), AGGREGATE_ACTIVITY_ID));
        MergeActivity mergeAct = new MergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, aggregateAct);
        builder.addSourceEdge(0, aggregateAct, 0);

        builder.addActivity(this, mergeAct);
        builder.addTargetEdge(0, mergeAct, 0);

        builder.addBlockingEdge(aggregateAct, mergeAct);
    }

    private class AggregateActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public AggregateActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new ExternalGroupBuildOperatorNodePushable(ctx, new TaskId(getActivityId(), partition), keyFields,
                    framesLimit, comparatorFactories, firstNormalizerFactory, aggregatorFactory,
                    recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), recordDescriptors[0],
                    spillableTableFactory);
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new ExternalGroupMergeOperatorNodePushable(ctx, new TaskId(new ActivityId(getOperatorId(),
                    AGGREGATE_ACTIVITY_ID), partition), comparatorFactories, firstNormalizerFactory, keyFields,
                    mergerFactory, isOutputSorted, framesLimit, recordDescriptors[0]);
        }

    }

}
