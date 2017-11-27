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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;
import org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.Algorithm;

/**
 * Group-by aggregation is pushed before run file generation.
 *
 * @author yingyib
 */
public class ExternalSortGroupByRunGenerator extends AbstractExternalSortRunGenerator {

    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor inRecordDesc;
    private final RecordDescriptor outRecordDesc;

    public ExternalSortGroupByRunGenerator(IHyracksTaskContext ctx, int[] sortFields, RecordDescriptor inputRecordDesc,
            int framesLimit, int[] groupFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor outRecordDesc, Algorithm alg) throws HyracksDataException {
        this(ctx, sortFields, inputRecordDesc, framesLimit, groupFields,
                firstKeyNormalizerFactory != null ? new INormalizedKeyComputerFactory[] { firstKeyNormalizerFactory }
                        : null,
                comparatorFactories, aggregatorFactory, outRecordDesc, alg, EnumFreeSlotPolicy.LAST_FIT);
    }

    public ExternalSortGroupByRunGenerator(IHyracksTaskContext ctx, int[] sortFields, RecordDescriptor inputRecordDesc,
            int framesLimit, int[] groupFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor outRecordDesc, Algorithm alg) throws HyracksDataException {
        this(ctx, sortFields, inputRecordDesc, framesLimit, groupFields, keyNormalizerFactories, comparatorFactories,
                aggregatorFactory, outRecordDesc, alg, EnumFreeSlotPolicy.LAST_FIT);
    }

    public ExternalSortGroupByRunGenerator(IHyracksTaskContext ctx, int[] sortFields, RecordDescriptor inputRecordDesc,
            int framesLimit, int[] groupFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor outRecordDesc, Algorithm alg, EnumFreeSlotPolicy policy) throws HyracksDataException {
        super(ctx, sortFields, keyNormalizerFactories, comparatorFactories, inputRecordDesc, alg, policy, framesLimit);

        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        this.inRecordDesc = inputRecordDesc;
        this.outRecordDesc = outRecordDesc;
    }

    @Override
    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext()
                .createManagedWorkspaceFile(ExternalSortGroupByRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIoManager());
    }

    @Override
    protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException {
        //create group-by comparators
        IBinaryComparator[] comparators =
                new IBinaryComparator[Math.min(groupFields.length, comparatorFactories.length)];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        return new PreclusteredGroupWriter(ctx, groupFields, comparators, aggregatorFactory, this.inRecordDesc,
                this.outRecordDesc, writer, true);
    }
}
