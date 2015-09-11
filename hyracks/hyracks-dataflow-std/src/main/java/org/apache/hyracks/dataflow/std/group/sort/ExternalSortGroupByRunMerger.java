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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import org.apache.hyracks.dataflow.std.sort.ISorter;
import org.apache.hyracks.dataflow.std.sort.RunAndMaxFrameSizePair;

/**
 * Group-by aggregation is pushed into multi-pass merge of external sort.
 *
 * @author yingyib
 */
public class ExternalSortGroupByRunMerger extends ExternalSortRunMerger {

    private final RecordDescriptor inputRecordDesc;
    private final RecordDescriptor partialAggRecordDesc;
    private final RecordDescriptor outRecordDesc;

    private final int[] groupFields;
    private final IAggregatorDescriptorFactory mergeAggregatorFactory;
    private final IAggregatorDescriptorFactory partialAggregatorFactory;
    private final boolean localSide;

    private final int[] mergeSortFields;
    private final int[] mergeGroupFields;
    private final IBinaryComparator[] groupByComparators;

    public ExternalSortGroupByRunMerger(IHyracksTaskContext ctx, ISorter frameSorter, List<RunAndMaxFrameSizePair> runs,
            int[] sortFields, RecordDescriptor inRecordDesc, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, int framesLimit, IFrameWriter writer, int[] groupFields,
            INormalizedKeyComputer nmk, IBinaryComparator[] comparators,
            IAggregatorDescriptorFactory partialAggregatorFactory, IAggregatorDescriptorFactory aggregatorFactory,
            boolean localStage) {
        super(ctx, frameSorter, runs, sortFields, comparators, nmk, partialAggRecordDesc, framesLimit,
                writer);
        this.inputRecordDesc = inRecordDesc;
        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outRecordDesc = outRecordDesc;

        this.groupFields = groupFields;
        this.mergeAggregatorFactory = aggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.localSide = localStage;

        //create merge sort fields
        int numSortFields = sortFields.length;
        mergeSortFields = new int[numSortFields];
        for (int i = 0; i < numSortFields; i++) {
            mergeSortFields[i] = i;
        }

        //create merge group fields
        int numGroupFields = groupFields.length;
        mergeGroupFields = new int[numGroupFields];
        for (int i = 0; i < numGroupFields; i++) {
            mergeGroupFields[i] = i;
        }

        //setup comparators for grouping
        groupByComparators = new IBinaryComparator[Math.min(mergeGroupFields.length, comparators.length)];
        for (int i = 0; i < groupByComparators.length; i++) {
            groupByComparators[i] = comparators[i];
        }
    }

    @Override
    protected IFrameWriter prepareSkipMergingFinalResultWriter(IFrameWriter nextWriter) throws HyracksDataException {
        IAggregatorDescriptorFactory aggregatorFactory = localSide ? partialAggregatorFactory : mergeAggregatorFactory;
        boolean outputPartial = false;
        return new PreclusteredGroupWriter(ctx, groupFields, groupByComparators,
                aggregatorFactory, inputRecordDesc, outRecordDesc, nextWriter, outputPartial);
    }

    @Override
    protected RunFileWriter prepareIntermediateMergeRunFile() throws HyracksDataException {
        FileReference newRun = ctx.createManagedWorkspaceFile(ExternalSortGroupByRunMerger.class.getSimpleName());
        return new RunFileWriter(newRun, ctx.getIOManager());
    }

    @Override
    protected IFrameWriter prepareIntermediateMergeResultWriter(RunFileWriter mergeFileWriter)
            throws HyracksDataException {
        IAggregatorDescriptorFactory aggregatorFactory = localSide ? mergeAggregatorFactory : partialAggregatorFactory;
        boolean outputPartial = true;
        return new PreclusteredGroupWriter(ctx, mergeGroupFields, groupByComparators, aggregatorFactory,
                partialAggRecordDesc, partialAggRecordDesc, mergeFileWriter, outputPartial);
    }

    @Override
    protected IFrameWriter prepareFinalMergeResultWriter(IFrameWriter nextWriter) throws HyracksDataException {
        boolean outputPartial = false;
        return new PreclusteredGroupWriter(ctx, mergeGroupFields, groupByComparators,
                mergeAggregatorFactory, partialAggRecordDesc, outRecordDesc, nextWriter, outputPartial);
    }

    @Override
    protected int[] getSortFields() {
        return mergeSortFields;
    }
}
