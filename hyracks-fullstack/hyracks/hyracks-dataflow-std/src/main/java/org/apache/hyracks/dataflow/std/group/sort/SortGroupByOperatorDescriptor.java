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
package org.apache.hyracks.dataflow.std.group.sort;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunMerger;
import org.apache.hyracks.dataflow.std.sort.AbstractSorterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.IRunGenerator;
import org.apache.hyracks.dataflow.std.sort.TimedRunGenerator;

/**
 * This Operator pushes group-by aggregation into the external sort.
 * After the in-memory sort, it aggregates the sorted data before writing it to a run file.
 * During the merge phase, it does an aggregation over sorted results.
 *
 * @author yingyib
 */
public class SortGroupByOperatorDescriptor extends AbstractSorterOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int[] groupFields;
    private final IAggregatorDescriptorFactory mergeAggregatorFactory;
    private final IAggregatorDescriptorFactory partialAggregatorFactory;
    private final RecordDescriptor partialAggRecordDesc;
    private final RecordDescriptor outputRecordDesc;
    private final boolean finalStage;
    private static final Algorithm ALG = Algorithm.MERGE_SORT;

    /**
     * @param spec
     *            the Hyracks job specification
     * @param framesLimit
     *            the frame limit for this operator
     * @param sortFields
     *            the fields to sort
     * @param groupFields
     *            the fields to group, which can be a prefix subset of sortFields
     * @param firstKeyNormalizerFactory
     *            the normalized key computer factory of the first key
     * @param comparatorFactories
     *            the comparator factories of sort keys
     * @param partialAggregatorFactory
     *            for aggregating the input of this operator
     * @param mergeAggregatorFactory
     *            for aggregating the intermediate data of this operator
     * @param partialAggRecordDesc
     *            the record descriptor of intermediate data
     * @param outRecordDesc
     *            the record descriptor of output data
     * @param finalStage
     *            whether the operator is used for final stage aggregation
     */
    public SortGroupByOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            int[] groupFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory partialAggregatorFactory,
            IAggregatorDescriptorFactory mergeAggregatorFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, boolean finalStage) {
        this(spec, framesLimit, sortFields, groupFields,
                firstKeyNormalizerFactory != null ? new INormalizedKeyComputerFactory[] { firstKeyNormalizerFactory }
                        : null,
                comparatorFactories, partialAggregatorFactory, mergeAggregatorFactory, partialAggRecordDesc,
                outRecordDesc, finalStage);
    }

    /**
     * @param spec
     *            the Hyracks job specification
     * @param framesLimit
     *            the frame limit for this operator
     * @param sortFields
     *            the fields to sort
     * @param groupFields
     *            the fields to group, which can be a prefix subset of sortFields
     * @param keyNormalizerFactories
     *            the normalized key computer factories for the prefix the sortFields
     * @param comparatorFactories
     *            the comparator factories of sort keys
     * @param partialAggregatorFactory
     *            for aggregating the input of this operator
     * @param mergeAggregatorFactory
     *            for aggregating the intermediate data of this operator
     * @param partialAggRecordDesc
     *            the record descriptor of intermediate data
     * @param outRecordDesc
     *            the record descriptor of output data
     * @param finalStage
     *            whether the operator is used for final stage aggregation
     */
    public SortGroupByOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            int[] groupFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory partialAggregatorFactory,
            IAggregatorDescriptorFactory mergeAggregatorFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, boolean finalStage) {
        super(spec, framesLimit, sortFields, keyNormalizerFactories, comparatorFactories, outRecordDesc);
        if (framesLimit <= 1) {
            throw new IllegalStateException(); // minimum of 2 frames (1 in,1 out)
        }

        this.groupFields = groupFields;
        this.mergeAggregatorFactory = mergeAggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outputRecordDesc = outRecordDesc;
        this.finalStage = finalStage;
    }

    @Override
    public AbstractSorterOperatorDescriptor.SortActivity getSortActivity(ActivityId id) {
        return new AbstractSorterOperatorDescriptor.SortActivity(id) {
            private static final long serialVersionUID = 1L;

            @Override
            protected IRunGenerator getRunGenerator(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescriptorProvider) throws HyracksDataException {
                final boolean profile = ctx.getJobFlags().contains(JobFlag.PROFILE_RUNTIME);
                IRunGenerator runGen = new ExternalSortGroupByRunGenerator(ctx, sortFields,
                        recordDescriptorProvider.getInputRecordDescriptor(this.getActivityId(), 0), framesLimit,
                        groupFields, keyNormalizerFactories, comparatorFactories, partialAggregatorFactory,
                        partialAggRecordDesc, ALG);
                return profile ? TimedRunGenerator.time(runGen, ctx, "GroupBy (Sort Runs)") : runGen;
            }
        };
    }

    @Override
    public AbstractSorterOperatorDescriptor.MergeActivity getMergeActivity(ActivityId id) {
        return new AbstractSorterOperatorDescriptor.MergeActivity(id) {
            private static final long serialVersionUID = 1L;

            @Override
            protected AbstractExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, List<GeneratedRunFileReader> runs,
                    IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, int necessaryFrames) {
                return new ExternalSortGroupByRunMerger(ctx, runs, sortFields,
                        recordDescProvider.getInputRecordDescriptor(new ActivityId(odId, SORT_ACTIVITY_ID), 0),
                        partialAggRecordDesc, outputRecordDesc, necessaryFrames, groupFields, nmkComputer, comparators,
                        partialAggregatorFactory, mergeAggregatorFactory, !finalStage);
            }
        };
    }

    @Override
    public String getDisplayName() {
        return "GroupBy (Sort)";
    }

    @Override
    public String toString() {
        return getDisplayName();
    }
}
