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

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;

public class TopKSorterOperatorDescriptor extends AbstractSorterOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int topK;

    public TopKSorterOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int topK, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, topK,
                sortFields, firstKeyNormalizerFactory != null
                        ? new INormalizedKeyComputerFactory[] { firstKeyNormalizerFactory } : null,
                comparatorFactories, recordDescriptor);
    }

    private TopKSorterOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int topK, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(spec, framesLimit, sortFields, keyNormalizerFactories, comparatorFactories, recordDescriptor);
        this.topK = topK;
    }

    @Override
    public SortActivity getSortActivity(ActivityId id) {
        return new SortActivity(id) {
            private static final long serialVersionUID = 1L;

            @Override
            protected IRunGenerator getRunGenerator(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider) {
                final boolean profile = ctx.getJobFlags().contains(JobFlag.PROFILE_RUNTIME);
                IRunGenerator runGen = new HybridTopKSortRunGenerator(ctx, framesLimit, topK, sortFields,
                        keyNormalizerFactories, comparatorFactories, outRecDescs[0]);
                return profile ? TimedRunGenerator.time(runGen, ctx, "TopKSort (Sort)") : runGen;

            }
        };
    }

    @Override
    public MergeActivity getMergeActivity(ActivityId id) {
        return new MergeActivity(id) {
            private static final long serialVersionUID = 1L;

            @Override
            protected AbstractExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, List<GeneratedRunFileReader> runs,
                    IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, int necessaryFrames) {
                return new ExternalSortRunMerger(ctx, runs, sortFields, comparators, nmkComputer, outRecDescs[0],
                        necessaryFrames, topK);
            }
        };
    }

    @Override
    public String getDisplayName() {
        return "Top K Sort";
    }

}
