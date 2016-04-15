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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.parallel.HistogramAlgorithm;
import org.apache.hyracks.dataflow.std.parallel.base.AbstractSamplingWriter;
import org.apache.hyracks.dataflow.std.parallel.base.MergeOrderedSampleWriter;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.FrameSorterMergeSort;
import org.apache.hyracks.dataflow.std.sort.FrameSorterQuickSort;
import org.apache.hyracks.dataflow.std.sort.IFrameSorter;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;

/**
 * @author michael
 */
public class MergeSampleOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final EnumFreeSlotPolicy policy = EnumFreeSlotPolicy.BIGGEST_FIT;
    private final HistogramAlgorithm sortAlg = HistogramAlgorithm.ORDERED_HISTOGRAM;
    private final IFrameSorter frameSorter;
    private final Object stateId;
    private final int frameLimit;
    private final int outputLimit;
    private final int[] sampleFields;
    private final int sampleBasis;
    private final HistogramAlgorithm algorithm;
    private final RecordDescriptor inDesc;
    private final RecordDescriptor outDesc;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private AbstractSamplingWriter sw;

    /*private MaterializingSampleTaskState state;*/

    /**
     * @throws HyracksDataException
     */
    public MergeSampleOperatorNodePushable(final IHyracksTaskContext ctx, Object stateId, int[] sampleFields,
            int sampleBasis, int frameLimit, IRecordDescriptorProvider recordDescProvider, int outputLimit,
            RecordDescriptor inDesc, RecordDescriptor outDesc, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, HistogramAlgorithm alg, final int partition,
            final int nPartitions) throws HyracksDataException {
        this.ctx = ctx;
        this.stateId = stateId;
        this.frameLimit = frameLimit;
        this.outputLimit = outputLimit;
        this.inDesc = inDesc;
        this.outDesc = outDesc;
        this.comparatorFactories = comparatorFactories;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.sampleFields = sampleFields;
        this.sampleBasis = sampleBasis;
        this.algorithm = alg;
        IFrameFreeSlotPolicy freeSlotPolicy = null;
        switch (policy) {
            case BIGGEST_FIT:
                freeSlotPolicy = FrameFreeSlotPolicyFactory.createFreeSlotPolicy(EnumFreeSlotPolicy.BIGGEST_FIT,
                        frameLimit - 1);
                break;
            case SMALLEST_FIT:
                freeSlotPolicy = FrameFreeSlotPolicyFactory.createFreeSlotPolicy(EnumFreeSlotPolicy.SMALLEST_FIT);
                break;
            case LAST_FIT:
                freeSlotPolicy = FrameFreeSlotPolicyFactory.createFreeSlotPolicy(EnumFreeSlotPolicy.LAST_FIT,
                        frameLimit - 1);
                break;
        }
        IFrameBufferManager bufferManager = new VariableFrameMemoryManager(new VariableFramePool(ctx, (frameLimit - 1)
                * ctx.getInitialFrameSize()), freeSlotPolicy);
        if (sortAlg == HistogramAlgorithm.ORDERED_HISTOGRAM) {
            frameSorter = new FrameSorterMergeSort(ctx, bufferManager, sampleFields, firstKeyNormalizerFactory,
                    comparatorFactories, inDesc, outputLimit);
        } else {
            frameSorter = new FrameSorterQuickSort(ctx, bufferManager, sampleFields, firstKeyNormalizerFactory,
                    comparatorFactories, inDesc, outputLimit);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        /*writer.open();*/
        switch (algorithm) {
            case ORDERED_HISTOGRAM:
                sw = new MergeOrderedSampleWriter(ctx, sampleFields, sampleBasis, comparators, inDesc, outDesc, writer);
                sw.open();
                /*state = new MaterializingSampleTaskState(ctx.getJobletContext().getJobId(), stateId);
                state.open(ctx);*/
                break;

            case UNIFORM_HISTOGRAM:
            case RANDOM_HISTOGRAM:
            case WAVELET_HISTOGRAM:
                break;
            default:
                break;
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        sw.nextFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        sw.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        /*state.close();*/
        sw.close();
    }
}
