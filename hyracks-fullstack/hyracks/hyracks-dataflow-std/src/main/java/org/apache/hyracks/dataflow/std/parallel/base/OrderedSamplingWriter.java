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
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * @author michael
 */
public class OrderedSamplingWriter extends AbstractSamplingWriter {
    private static final Logger LOGGER = Logger.getLogger(OrderedSamplingWriter.class.getName());

    private final static int DEFAULT_TICKS_NUMBER = 501;
    private final static int DEFAULT_TUPLE_NUMBER = 500;
    private final int sampleTick;
    private IFrame lastFrame;
    private IFrameTupleAccessor lastAccessor;
    private int lastTupleTick;
    private int localTicks = 0;
    private boolean updatedFrame;

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
    public OrderedSamplingWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer, boolean outputPartial) throws HyracksDataException {
        super(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer, outputPartial);
        this.sampleTick = DEFAULT_TICKS_NUMBER/*DEFAULT_TUPLE_NUMBER / this.sampleBasis + 1*/;
        lastFrame = new VSizeFrame(ctx);
        lastAccessor = new FrameTupleAccessor(inRecordDesc);
        lastAccessor.reset(lastFrame.getBuffer());
        this.lastTupleTick = 0;
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
    public OrderedSamplingWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer) throws HyracksDataException {
        super(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer);
        // TODO Auto-generated constructor stub
        this.sampleTick = DEFAULT_TICKS_NUMBER/*DEFAULT_TUPLE_NUMBER / this.sampleBasis + 1*/;
        lastFrame = new VSizeFrame(ctx);
        lastAccessor = new FrameTupleAccessor(inRecordDesc);
        lastAccessor.reset(lastFrame.getBuffer());
        this.lastTupleTick = 0;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int nTuples = inFrameAccessor.getTupleCount();
        for (int i = 0; i < nTuples; i++) {
            localTicks++;
            if (isFirst) {
                updatedFrame = true;
                tupleBuilder.reset();
                for (int j = 0; j < sampleFields.length; j++) {
                    tupleBuilder.addField(inFrameAccessor, i, sampleFields[j]);
                }
                cacheLastAccessor(inFrameAccessor.getBuffer());
                lastTupleTick = i;
                aggregator.init(lastAccessor, lastTupleTick, tupleBuilder.getDataOutput(), aggregateState);
                isFirst = false;
            } else {
                // each frame need to be at least sampled once.
                if (i == 0)
                    updatedFrame = true;
                switchBinsIfRequired(lastAccessor, lastTupleTick, inFrameAccessor, i);
            }
        }
    }

    private void cacheLastAccessor(ByteBuffer buf) throws HyracksDataException {
        if (updatedFrame) {
            lastFrame.ensureFrameSize(buf.capacity());
            FrameUtils.copyAndFlip(buf, lastFrame.getBuffer());
            lastAccessor.reset(lastFrame.getBuffer());
            updatedFrame = false;
        }
    }

    @Override
    protected boolean aggregatingWithBalanceGuaranteed(IFrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
            IFrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
        for (int i = 0; i < comparators.length; i++) {
            int fIdx = sampleFields[i];
            int s1 = prevTupleAccessor.getAbsoluteFieldStartOffset(prevTupleIndex, fIdx);
            int l1 = prevTupleAccessor.getFieldLength(prevTupleIndex, fIdx);
            int s2 = currTupleAccessor.getAbsoluteFieldStartOffset(currTupleIndex, fIdx);
            int l2 = currTupleAccessor.getFieldLength(currTupleIndex, fIdx);
            if (0 != comparators[i].compare(prevTupleAccessor.getBuffer().array(), s1, l1, currTupleAccessor
                    .getBuffer().array(), s2, l2))
                return false;
        }
        return true;
    }

    @Override
    protected void switchBinsIfRequired(IFrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
            IFrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
        if (localTicks >= sampleTick) {
            if (!aggregatingWithBalanceGuaranteed(lastAccessor, lastTupleTick, currTupleAccessor, currTupleIndex)) {
                writeOutput(lastAccessor, lastTupleTick);
                cacheLastAccessor(currTupleAccessor.getBuffer());
                lastTupleTick = currTupleIndex;
                tupleBuilder.reset();
                for (int i = 0; i < sampleFields.length; i++) {
                    tupleBuilder.addField(currTupleAccessor, currTupleIndex, sampleFields[i]);
                }
                aggregator.init(currTupleAccessor, currTupleIndex, tupleBuilder.getDataOutput(), aggregateState);
                localTicks = 1;
            } else {
                try {
                    aggregator.aggregate(lastAccessor, lastTupleTick, null, 0, aggregateState);
                } catch (Exception e) {
                    LOGGER.info("Sampling error: " + tupleBuilder.getDataOutput().getClass().getName());
                    throw new HyracksDataException("Failed to sample the immediate bins");
                }
            }
        } else {
            aggregator.aggregate(lastAccessor, lastTupleTick, null, 0, aggregateState);
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
