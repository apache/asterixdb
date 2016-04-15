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

package org.apache.hyracks.dataflow.std.parallel.base;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;

/**
 * @author michael
 */
public class MergeOrderedSampleWriter extends AbstractSamplingWriter {

    private final static int DEFAULT_MERGE_NUMBER = 3750;

    private int accumTick;
    private final int mergeField;
    private IFrame lastFrame;
    private IFrameTupleAccessor lastAccessor;
    private int lastTupleTick;
    private boolean updatedFrame;
    private boolean updatedRange;

    /**
     * @param ctx
     * @param sampleFields
     * @param sampleBasis
     * @param comparators
     * @param inRecordDesc
     * @param outRecordDesc
     * @param writer
     * @param outputPartial
     * @throws HyracksDataException
     */
    public MergeOrderedSampleWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer, boolean outputPartial) throws HyracksDataException {
        super(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer, outputPartial);
        this.accumTick = 0;
        this.mergeField = inRecordDesc.getFieldCount() - 1;
        this.aggregator = new IntSumFieldAggregatorFactory(mergeField, true).createAggregator(ctx, inRecordDesc,
                outRecordDesc);
        this.aggregateState = aggregator.createState();
        this.sampleBasis = DEFAULT_MERGE_NUMBER;
    }

    /**
     * @param ctx
     * @param sampleFields
     * @param sampleBasis
     * @param comparators
     * @param inRecordDesc
     * @param outRecordDesc
     * @param writer
     * @throws HyracksDataException
     */
    public MergeOrderedSampleWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer) throws HyracksDataException {
        super(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer);
        this.accumTick = 0;
        this.mergeField = inRecordDesc.getFieldCount() - 1;
        this.aggregator = new IntSumFieldAggregatorFactory(mergeField, true).createAggregator(ctx, inRecordDesc,
                outRecordDesc);
        this.aggregateState = aggregator.createState();
        this.sampleBasis = DEFAULT_MERGE_NUMBER;
        lastFrame = new VSizeFrame(ctx);
        lastAccessor = new FrameTupleAccessor(inRecordDesc);
        lastAccessor.reset(lastFrame.getBuffer());
        this.lastTupleTick = 0;
        this.updatedRange = false;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int nTuples = inFrameAccessor.getTupleCount();
        for (int i = 0; i < nTuples; i++) {
            accumTick += IntegerPointable.getInteger(
                    inFrameAccessor.getBuffer().array(),
                    inFrameAccessor.getTupleStartOffset(i) + inFrameAccessor.getFieldSlotsLength()
                            + inFrameAccessor.getFieldStartOffset(i, mergeField));
            if (isFirst) {
                updatedFrame = true;
                tupleBuilder.reset();
                for (int j = 0; j < sampleFields.length; j++) {
                    tupleBuilder.addField(inFrameAccessor, i, sampleFields[j]);
                }
                updateRangeAccessor(inFrameAccessor.getBuffer());
                lastTupleTick = i;
                aggregator.init(inFrameAccessor, i, tupleBuilder.getDataOutput(), aggregateState);
                isFirst = false;
            } else {
                if (i == 0) {
                    updatedFrame = true;
                    switchBinsIfRequired(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1, inFrameAccessor, i);
                } else {
                    switchBinsIfRequired(inFrameAccessor, i - 1, inFrameAccessor, i);
                }
            }
        }
        copyFrame.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, copyFrame.getBuffer());
        copyFrameAccessor.reset(copyFrame.getBuffer());
    }

    private void updateRangeAccessor(ByteBuffer buf) throws HyracksDataException {
        updatedRange = false;
        if (updatedFrame) {
            lastFrame.ensureFrameSize(buf.capacity());
            FrameUtils.copyAndFlip(buf, lastFrame.getBuffer());
            lastAccessor.reset(lastFrame.getBuffer());
            updatedFrame = false;
        }
    }

    @Override
    protected void switchBinsIfRequired(IFrameTupleAccessor prevTA, int prevIdx, IFrameTupleAccessor currTA, int currIdx)
            throws HyracksDataException {
        if (!aggregatingWithBalanceGuaranteed(prevTA, prevIdx, currTA, currIdx) && accumTick > sampleBasis) {
            if (updatedRange) {
                writeOutput(lastAccessor, lastTupleTick);
                updateRangeAccessor(currTA.getBuffer());
                lastTupleTick = currIdx;
                accumTick = IntegerPointable.getInteger(currTA.getBuffer().array(), currTA.getTupleStartOffset(currIdx)
                        + currTA.getFieldSlotsLength() + currTA.getFieldStartOffset(currIdx, mergeField));
                tupleBuilder.reset();
                for (int i = 0; i < sampleFields.length; i++) {
                    tupleBuilder.addField(currTA, currIdx, sampleFields[i]);
                }
                aggregator.init(currTA, currIdx, tupleBuilder.getDataOutput(), aggregateState);
            } else {
                updatedRange = true;
                aggregator.aggregate(currTA, currIdx, null, 0, aggregateState);
            }
        } else {
            aggregator.aggregate(currTA, currIdx, null, 0, aggregateState);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (!isFailed && !isFirst) {
            assert (lastAccessor.getTupleCount() > 0);
            writeOutput(lastAccessor, lastTupleTick);
        }
        super.close();
    }
}
