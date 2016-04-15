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

package org.apache.hyracks.dataflow.std.parallel.histogram;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.parallel.HistogramAlgorithm;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.FrameSorterMergeSort;
import org.apache.hyracks.dataflow.std.sort.FrameSorterQuickSort;
import org.apache.hyracks.dataflow.std.sort.IFrameSorter;

/**
 * @author michael
 */
public class MergeSampleOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final static int GLOBAL_MERGE_FACTOR = 2;

    private final static int MERGE_SAMPLER_ACTIVITY_ID = 0;
    private final static int SAMPLE_READER_ACTIVITY_ID = 1;

    private final int frameLimit;
    private final int outputLimit;
    private final HistogramAlgorithm algorithm;

    private final int[] sampleFields;
    private final int sampleBasis;
    private IBinaryComparatorFactory[] comparatorFactories;
    private INormalizedKeyComputerFactory firstKeyNormalizerFactory;

    private RecordDescriptor outDesc;
    private RecordDescriptor inDesc;
    boolean needMaterialization;

    /**
     * @param spec
     * @param inputArity
     * @param outputArity
     * @throws HyracksDataException
     */
    public MergeSampleOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int[] sampleFields,
            RecordDescriptor inDesc, int outputLimit, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] compFactories, HistogramAlgorithm alg, boolean needMaterialization)
            throws HyracksDataException {
        super(spec, 1, 1);
        this.frameLimit = frameLimit;
        this.outputLimit = outputLimit;
        this.algorithm = alg;
        this.sampleBasis = GLOBAL_MERGE_FACTOR;
        this.sampleFields = sampleFields;
        this.comparatorFactories = compFactories;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.inDesc = inDesc;
        this.needMaterialization = needMaterialization;
        this.outDesc = inDesc;
        this.recordDescriptors[0] = inDesc;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        if (!needMaterialization) {
            MergeSampleActivityNode msa = new MergeSampleActivityNode(new ActivityId(odId, MERGE_SAMPLER_ACTIVITY_ID));
            builder.addActivity(this, msa);
            builder.addSourceEdge(0, msa, 0);
            builder.addTargetEdge(0, msa, 0);
        } else {
            MergeSampleActivityNode msa = new MergeSampleActivityNode(new ActivityId(odId, MERGE_SAMPLER_ACTIVITY_ID));
            SampleReaderActivityNode sra = new SampleReaderActivityNode(new ActivityId(odId, SAMPLE_READER_ACTIVITY_ID));
            builder.addActivity(this, msa);
            builder.addSourceEdge(0, msa, 0);
            builder.addActivity(this, sra);
            builder.addTargetEdge(0, sra, 0);
            builder.addBlockingEdge(msa, sra);
        }
    }

    private class MergeSampleActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeSampleActivityNode(ActivityId Id) {
            super(Id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {
            return new MergeSampleOperatorNodePushable(ctx, new TaskId(getActivityId(), partition), sampleFields,
                    sampleBasis, frameLimit, recordDescProvider, outputLimit, inDesc, outDesc,
                    firstKeyNormalizerFactory, comparatorFactories, algorithm, partition, nPartitions);
        }
    }

    private class SampleReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SampleReaderActivityNode(ActivityId Id) {
            super(Id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {

                }

                @Override
                public void deinitialize() throws HyracksDataException {

                }
            };
        }
    }
}
