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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.parallel.HistogramAlgorithm;
import org.apache.hyracks.dataflow.std.parallel.base.AbstractSamplingWriter;
import org.apache.hyracks.dataflow.std.parallel.base.MaterializingSampleTaskState;
import org.apache.hyracks.dataflow.std.parallel.base.OrderedSamplingWriter;
import org.apache.hyracks.dataflow.std.parallel.base.QuantileSamplingWriter;

/**
 * @author michael
 */
public class MaterializingSampleOperatorNodePushable extends AbstractUnaryInputOperatorNodePushable {

    private AbstractSamplingWriter sw;
    //    private RunFileWriter swriter;
    private final IHyracksTaskContext ctx;
    private final int[] sampleFields;
    private final int sampleBasis;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final HistogramAlgorithm alg;
    private final RecordDescriptor inDesc;
    private final RecordDescriptor outDesc;
    private final boolean requiresMaterialization;
    private final int partition;
    private final int nNonMaterialization;
    private final Object stateId;

    private MaterializingSampleTaskState state;
    private final IFrameWriter[] writers;

    public MaterializingSampleOperatorNodePushable(IHyracksTaskContext ctx, Object stateId, int[] sampleFields,
            int sampleBasis, IBinaryComparatorFactory[] comparatorFactories, HistogramAlgorithm alg,
            RecordDescriptor inDesc, RecordDescriptor outDesc, int nNonMaterialization,
            boolean requiresMaterialization, int partition) {
        this.ctx = ctx;
        this.stateId = stateId;
        this.sampleFields = sampleFields;
        this.sampleBasis = sampleBasis;
        this.comparatorFactories = comparatorFactories;
        this.alg = alg;
        this.inDesc = inDesc;
        this.outDesc = outDesc;
        this.requiresMaterialization = requiresMaterialization;
        this.partition = partition;
        this.nNonMaterialization = nNonMaterialization;
        this.writers = new IFrameWriter[nNonMaterialization];
    }

    @Override
    public void open() throws HyracksDataException {
        // sample first
        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        // extract rangePartition and dynamically promote the partition param, get the RangeConnector and change the rangeMap.
        if (requiresMaterialization) {
            state = new MaterializingSampleTaskState(ctx.getJobletContext().getJobId(), stateId);
            state.open(ctx);
        }
        for (int i = 0; i < nNonMaterialization; i++) {
            writers[i].open();
        }

        // Here for single input sampler only and will be merged for multiway sampling in the merge part.
        switch (alg) {
            case ORDERED_HISTOGRAM:
                sw = new OrderedSamplingWriter(ctx, sampleFields, sampleBasis, comparators, inDesc, outDesc,
                        writers[0], false);
                break;
            case UNIFORM_HISTOGRAM:
                sw = new QuantileSamplingWriter(ctx, sampleFields, sampleBasis, comparators, inDesc, outDesc,
                        writers[0], false);
                break;
        }
        sw.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        sw.nextFrame(buffer);
        buffer.rewind();
        //        if (requiresMaterialization) {
        state.appendFrame(buffer);
        //        }
        for (int i = 1; i < nNonMaterialization; i++) {
            FrameUtils.flushFrame(buffer, writers[i]);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        sw.fail();
        for (int i = 0; i < nNonMaterialization; i++) {
            writers[i].fail();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (requiresMaterialization) {
            state.close();
            ctx.setStateObject(state);
        }
        sw.close();
        for (int i = 1; i < nNonMaterialization; i++) {
            state.writeOut(writers[i], new VSizeFrame(ctx));
            writers[i].close();
        }
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recrdDesc) {
        //        this.writer = writer;
        writers[index] = writer;
    }
}
