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
package org.apache.hyracks.dataflow.std.group.preclustered;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppenderWrapper;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class PreclusteredGroupWriter implements IFrameWriter {
    private final int[] groupFields;
    private final IBinaryComparator[] comparators;
    private final IAggregatorDescriptor aggregator;
    private final AggregateState aggregateState;
    private final IFrame copyFrame;
    private final FrameTupleAccessor inFrameAccessor;
    private final FrameTupleAccessor copyFrameAccessor;

    private final FrameTupleAppenderWrapper appenderWrapper;
    private final ArrayTupleBuilder tupleBuilder;
    private boolean outputPartial = false;

    private boolean first;

    private boolean isFailed = false;

    public PreclusteredGroupWriter(IHyracksTaskContext ctx, int[] groupFields, IBinaryComparator[] comparators,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDesc, IFrameWriter writer, boolean outputPartial) throws HyracksDataException {
        this(ctx, groupFields, comparators, aggregatorFactory, inRecordDesc, outRecordDesc, writer);
        this.outputPartial = outputPartial;
    }

    public PreclusteredGroupWriter(IHyracksTaskContext ctx, int[] groupFields, IBinaryComparator[] comparators,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDesc, IFrameWriter writer) throws HyracksDataException {
        this.groupFields = groupFields;
        this.comparators = comparators;
        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDesc, outRecordDesc, groupFields,
                groupFields, writer);
        this.aggregateState = aggregator.createAggregateStates();
        copyFrame = new VSizeFrame(ctx);
        inFrameAccessor = new FrameTupleAccessor(inRecordDesc);
        copyFrameAccessor = new FrameTupleAccessor(inRecordDesc);
        copyFrameAccessor.reset(copyFrame.getBuffer());

        VSizeFrame outFrame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(outFrame, true);
        appenderWrapper = new FrameTupleAppenderWrapper(appender, writer);

        tupleBuilder = new ArrayTupleBuilder(outRecordDesc.getFields().length);
    }

    @Override
    public void open() throws HyracksDataException {
        appenderWrapper.open();
        first = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inFrameAccessor.reset(buffer);
        int nTuples = inFrameAccessor.getTupleCount();
        for (int i = 0; i < nTuples; ++i) {
            if (first) {

                tupleBuilder.reset();
                for (int j = 0; j < groupFields.length; j++) {
                    tupleBuilder.addField(inFrameAccessor, i, groupFields[j]);
                }
                aggregator.init(tupleBuilder, inFrameAccessor, i, aggregateState);

                first = false;

            } else {
                if (i == 0) {
                    switchGroupIfRequired(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1, inFrameAccessor, i);
                } else {
                    switchGroupIfRequired(inFrameAccessor, i - 1, inFrameAccessor, i);
                }

            }
        }
        copyFrame.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, copyFrame.getBuffer());
        copyFrameAccessor.reset(copyFrame.getBuffer());
    }

    private void switchGroupIfRequired(FrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
            FrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
        if (!sameGroup(prevTupleAccessor, prevTupleIndex, currTupleAccessor, currTupleIndex)) {
            writeOutput(prevTupleAccessor, prevTupleIndex);

            tupleBuilder.reset();
            for (int j = 0; j < groupFields.length; j++) {
                tupleBuilder.addField(currTupleAccessor, currTupleIndex, groupFields[j]);
            }
            aggregator.init(tupleBuilder, currTupleAccessor, currTupleIndex, aggregateState);
        } else {
            aggregator.aggregate(currTupleAccessor, currTupleIndex, null, 0, aggregateState);
        }
    }

    private void writeOutput(final FrameTupleAccessor lastTupleAccessor, int lastTupleIndex)
            throws HyracksDataException {

        tupleBuilder.reset();
        for (int j = 0; j < groupFields.length; j++) {
            tupleBuilder.addField(lastTupleAccessor, lastTupleIndex, groupFields[j]);
        }
        boolean hasOutput = outputPartial ? aggregator.outputPartialResult(tupleBuilder, lastTupleAccessor,
                lastTupleIndex, aggregateState) : aggregator.outputFinalResult(tupleBuilder, lastTupleAccessor,
                lastTupleIndex, aggregateState);

        if (hasOutput) {
            appenderWrapper.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize());
        }

    }

    private boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx)
            throws HyracksDataException {
        for (int i = 0; i < comparators.length; ++i) {
            int fIdx = groupFields[i];
            int s1 = a1.getAbsoluteFieldStartOffset(t1Idx, fIdx);
            int l1 = a1.getFieldLength(t1Idx, fIdx);
            int s2 = a2.getAbsoluteFieldStartOffset(t2Idx, fIdx);
            int l2 = a2.getFieldLength(t2Idx, fIdx);
            if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void fail() throws HyracksDataException {
        isFailed = true;
        appenderWrapper.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (!isFailed && !first) {
            assert(copyFrameAccessor.getTupleCount() > 0);
            writeOutput(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1);
            appenderWrapper.flush();
        }
        aggregator.close();
        aggregateState.close();
        appenderWrapper.close();
    }
}