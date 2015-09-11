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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.sort.buffermanager.EnumFreeSlotPolicy;

import java.util.List;

public class ExternalSortOperatorDescriptor extends AbstractSorterOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private Algorithm alg = Algorithm.MERGE_SORT;
    private EnumFreeSlotPolicy policy = EnumFreeSlotPolicy.LAST_FIT;
    private final int outputLimit;

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, Algorithm alg) {
        this(spec, framesLimit, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor, alg,
                EnumFreeSlotPolicy.LAST_FIT);
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, sortFields, null, comparatorFactories, recordDescriptor);
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                Algorithm.MERGE_SORT, EnumFreeSlotPolicy.LAST_FIT);
    }

    @Override
    public AbstractSorterOperatorDescriptor.SortActivity getSortActivity(ActivityId id) {
        return new AbstractSorterOperatorDescriptor.SortActivity(id) {
            @Override
            protected AbstractSortRunGenerator getRunGenerator(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider) throws HyracksDataException {
                return new ExternalSortRunGenerator(ctx, sortFields, firstKeyNormalizerFactory,
                        comparatorFactories, recordDescriptors[0], alg, policy, framesLimit, outputLimit);
            }
        };
    }

    @Override
    public AbstractSorterOperatorDescriptor.MergeActivity getMergeActivity(ActivityId id) {
        return new AbstractSorterOperatorDescriptor.MergeActivity(id) {
            @Override
            protected ExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, IFrameWriter writer, ISorter sorter, List<RunAndMaxFrameSizePair> runs, IBinaryComparator[] comparators,
                    INormalizedKeyComputer nmkComputer, int necessaryFrames) {
                return new ExternalSortRunMerger(ctx, sorter, runs, sortFields, comparators,
                        nmkComputer, recordDescriptors[0], necessaryFrames, outputLimit, writer);
            }
        };
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, Algorithm alg, EnumFreeSlotPolicy policy) {
        this(spec, framesLimit, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor, alg,
                policy, Integer.MAX_VALUE);
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, Algorithm alg, EnumFreeSlotPolicy policy, int outputLimit) {
        super(spec, framesLimit, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor);
        if (framesLimit <= 1) {
            throw new IllegalStateException();// minimum of 2 fames (1 in,1 out)
        }
        this.alg = alg;
        this.policy = policy;
        this.outputLimit = outputLimit;
    }

}