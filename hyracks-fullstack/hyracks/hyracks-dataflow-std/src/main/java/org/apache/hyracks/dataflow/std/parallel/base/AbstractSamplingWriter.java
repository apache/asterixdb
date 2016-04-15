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
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppenderWrapper;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptor;
import org.apache.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;

/**
 * @author michael
 */
public abstract class AbstractSamplingWriter implements IFrameWriter {
    private static final Logger LOGGER = Logger.getLogger(AbstractSamplingWriter.class.getName());
    //        private final IHyracksTaskContext ctx;
    //    private final SampleAlgorithm alg = SampleAlgorithm.ORDERED_SAMPLE;
    protected final FrameTupleAppenderWrapper appenderWrapper;
    protected boolean outputPartial = false;

    protected boolean isFailed = false;
    protected final int[] sampleFields;
    protected int sampleBasis;
    protected final IBinaryComparator[] comparators;
    protected final IFrame copyFrame;
    protected final FrameTupleAccessor inFrameAccessor;
    protected final FrameTupleAccessor copyFrameAccessor;
    protected IFieldAggregateDescriptor aggregator;
    protected AggregateState aggregateState;
    protected final ArrayTupleBuilder tupleBuilder;
    protected boolean isFirst;

    public AbstractSamplingWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer, boolean outputPartial) throws HyracksDataException {
        this(ctx, sampleFields, sampleBasis, comparators, inRecordDesc, outRecordDesc, writer);
        this.outputPartial = outputPartial;
    }

    public AbstractSamplingWriter(IHyracksTaskContext ctx, int[] sampleFields, int sampleBasis,
            IBinaryComparator[] comparators, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc,
            IFrameWriter writer) throws HyracksDataException {
        //                this.ctx = ctx;
        this.sampleFields = sampleFields;
        this.sampleBasis = sampleBasis;
        this.comparators = comparators;
        copyFrame = new VSizeFrame(ctx);
        inFrameAccessor = new FrameTupleAccessor(inRecordDesc);
        copyFrameAccessor = new FrameTupleAccessor(inRecordDesc);
        copyFrameAccessor.reset(copyFrame.getBuffer());
        aggregator = new CountFieldAggregatorFactory(true).createAggregator(ctx, inRecordDesc, outRecordDesc);
        aggregateState = aggregator.createState();

        VSizeFrame outFrame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(outFrame, true);
        appenderWrapper = new FrameTupleAppenderWrapper(appender, writer);

        tupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFields().length);
    }

    @Override
    public void open() throws HyracksDataException {
        appenderWrapper.open();
        isFirst = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int nTuples = inFrameAccessor.getTupleCount();
        /*switch (alg) {
            case ORDERED_SAMPLE: {*/
        for (int i = 0; i < nTuples; i++) {
            if (isFirst) {
                tupleBuilder.reset();
                for (int j = 0; j < sampleFields.length; j++) {
                    tupleBuilder.addField(inFrameAccessor, i, sampleFields[j]);
                }
                aggregator.init(inFrameAccessor, i, tupleBuilder.getDataOutput(), aggregateState);
                isFirst = false;
            } else {
                // each frame need to be at least sampled once.
                if (i == 0) {
                    switchBinsIfRequired(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1, inFrameAccessor, i);
                } else {
                    try {
                        switchBinsIfRequired(inFrameAccessor, i - 1, inFrameAccessor, i);
                    } catch (Exception e) {
                        LOGGER.info("Sampling error: " + tupleBuilder.getDataOutput().getClass().getName());
                        throw new HyracksDataException("Failed to get the proper sampling bins");
                    }
                }
            }
        }
        copyFrame.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, copyFrame.getBuffer());
        copyFrameAccessor.reset(copyFrame.getBuffer());
        /*break;
        }
        case RANDOM_SAMPLE:
        case UNIFORM_SAMPLE:
        case WAVELET_SAMPLE:
        default:
        break;
        }*/
    }

    protected boolean writeFieldsOutput(final IFrameTupleAccessor lastTupleAccessor, int lastTupleIndex)
            throws HyracksDataException {
        int tupleOffset = lastTupleAccessor.getTupleStartOffset(lastTupleIndex);

        if (outputPartial) {
            int fieldOffset = lastTupleAccessor.getFieldStartOffset(lastTupleIndex, sampleFields.length);
            aggregator.outputPartialResult(tupleBuilder.getDataOutput(), lastTupleAccessor.getBuffer().array(),
                    tupleOffset + fieldOffset + lastTupleAccessor.getFieldSlotsLength(), aggregateState);
            tupleBuilder.addFieldEndOffset();
        } else {
            if (aggregator.needsBinaryState()) {
                int fieldOffset = lastTupleAccessor.getFieldStartOffset(lastTupleIndex, sampleFields.length);
                aggregator.outputFinalResult(tupleBuilder.getDataOutput(), lastTupleAccessor.getBuffer().array(),
                        tupleOffset + fieldOffset + lastTupleAccessor.getFieldSlotsLength(), aggregateState);
            } else {
                /*int fieldOffset = lastTupleAccessor.getFieldStartOffset(lastTupleIndex, sampleFields.length);
                aggregator.outputFinalResult(tupleBuilder.getDataOutput(), lastTupleAccessor.getBuffer().array(),
                        tupleOffset + fieldOffset + lastTupleAccessor.getFieldSlotsLength(), aggregateState);*/
                aggregator.outputFinalResult(tupleBuilder.getDataOutput(), null, 0, aggregateState);
            }
            tupleBuilder.addFieldEndOffset();
        }
        return true;
    }

    protected void writeOutput(final IFrameTupleAccessor lastTupleAccessor, int lastTupleIndex)
            throws HyracksDataException {
        tupleBuilder.reset();
        for (int j = 0; j < sampleFields.length; j++) {
            tupleBuilder.addField(lastTupleAccessor, lastTupleIndex, sampleFields[j]);
        }
        boolean hasOutput = writeFieldsOutput(lastTupleAccessor, lastTupleIndex);
        if (hasOutput) {
            appenderWrapper.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize());
        }
    }

    protected boolean aggregatingWithBalanceGuaranteed(IFrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
            IFrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
        for (int i = 0; i < comparators.length; i++) {
            int fIdx = sampleFields[i];
            int s1 = prevTupleAccessor.getAbsoluteFieldStartOffset(prevTupleIndex, fIdx);
            int l1 = prevTupleAccessor.getFieldLength(prevTupleIndex, fIdx);
            int s2 = currTupleAccessor.getAbsoluteFieldStartOffset(currTupleIndex, fIdx);
            int l2 = currTupleAccessor.getFieldLength(currTupleIndex, fIdx);
            if (0 != comparators[i].compare(prevTupleAccessor.getBuffer().array(), s1, l1, currTupleAccessor
                    .getBuffer().array(), s2, l2)) {
                return false;
            }
        }
        return true;
    }

    protected void switchBinsIfRequired(IFrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
            IFrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
        if (!aggregatingWithBalanceGuaranteed(prevTupleAccessor, prevTupleIndex, currTupleAccessor, currTupleIndex)) {
            writeOutput(prevTupleAccessor, prevTupleIndex);
            tupleBuilder.reset();
            for (int i = 0; i < sampleFields.length; i++) {
                tupleBuilder.addField(currTupleAccessor, currTupleIndex, sampleFields[i]);
            }
            aggregator.init(currTupleAccessor, currTupleIndex, tupleBuilder.getDataOutput(), aggregateState);
        } else {
            try {
                aggregator.aggregate(currTupleAccessor, currTupleIndex, null, 0, aggregateState);
            } catch (Exception e) {
                LOGGER.info("Sampling error: " + tupleBuilder.getDataOutput().getClass().getName());
                throw new HyracksDataException("Failed to get the proper sampling bins");
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        isFailed = true;
        appenderWrapper.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (!isFailed && !isFirst) {
            appenderWrapper.flush();
        }
        aggregator.close();
        aggregateState.close();
        appenderWrapper.close();
    }
}
