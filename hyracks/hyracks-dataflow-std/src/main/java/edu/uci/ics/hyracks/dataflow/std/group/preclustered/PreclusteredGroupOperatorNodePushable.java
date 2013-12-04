/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.preclustered;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

class PreclusteredGroupOperatorNodePushable extends
        AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor inRecordDescriptor;
    private final RecordDescriptor outRecordDescriptor;
    private PreclusteredGroupWriter pgw;

    PreclusteredGroupOperatorNodePushable(IHyracksTaskContext ctx, int[] groupFields,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor) {
        this.ctx = ctx;
        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        this.inRecordDescriptor = inRecordDescriptor;
        this.outRecordDescriptor = outRecordDescriptor;
    }

    @Override
    public void open() throws HyracksDataException {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        final IAggregatorDescriptor aggregator = aggregatorFactory.createAggregator(ctx, inRecordDescriptor,
                outRecordDescriptor, groupFields, groupFields, writer);
        final ByteBuffer copyFrame = ctx.allocateFrame();
        final FrameTupleAccessor copyFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDescriptor);
        copyFrameAccessor.reset(copyFrame);
        ByteBuffer outFrame = ctx.allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(outFrame, true);
        pgw = new PreclusteredGroupWriter(ctx, groupFields, comparators, aggregator, inRecordDescriptor,
                outRecordDescriptor, writer);
        pgw.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        pgw.nextFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        pgw.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        pgw.close();
    }
}