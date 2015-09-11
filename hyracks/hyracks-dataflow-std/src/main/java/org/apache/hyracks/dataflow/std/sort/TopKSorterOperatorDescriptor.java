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
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;

import java.util.List;

public class TopKSorterOperatorDescriptor extends AbstractSorterOperatorDescriptor {

    private final int topK;

    public TopKSorterOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int topK, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(spec, framesLimit, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor);
        this.topK = topK;
    }

    @Override
    public SortActivity getSortActivity(ActivityId id) {
        return new SortActivity(id) {
            @Override
            protected AbstractSortRunGenerator getRunGenerator(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider) {
                return new HybridTopKSortRunGenerator(ctx, framesLimit, topK, sortFields, firstKeyNormalizerFactory,
                        comparatorFactories, recordDescriptors[0]);

            }
        };
    }

    @Override
    public MergeActivity getMergeActivity(ActivityId id) {
        return new MergeActivity(id) {
            @Override
            protected ExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, IFrameWriter writer, ISorter sorter, List<RunAndMaxFrameSizePair> runs, IBinaryComparator[] comparators,
                    INormalizedKeyComputer nmkComputer, int necessaryFrames) {
                return new ExternalSortRunMerger(ctx, sorter, runs, sortFields, comparators,
                        nmkComputer, recordDescriptors[0], necessaryFrames, topK, writer);
            }
        };
    }
}
