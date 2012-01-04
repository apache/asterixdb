/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class PreclusteredGroupOperatorDescriptor extends
        AbstractSingleActivityOperatorDescriptor {
    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;

    private static final long serialVersionUID = 1L;

    public PreclusteredGroupOperatorDescriptor(JobSpecification spec,
            int[] groupFields, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition,
            int nPartitions) throws HyracksDataException {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        final RecordDescriptor inRecordDesc = recordDescProvider
                .getInputRecordDescriptor(getOperatorId(), 0);
        final IAggregatorDescriptor aggregator = aggregatorFactory
                .createAggregator(ctx, inRecordDesc, recordDescriptors[0],
                        groupFields, groupFields);
        final ByteBuffer copyFrame = ctx.allocateFrame();
        final FrameTupleAccessor copyFrameAccessor = new FrameTupleAccessor(
                ctx.getFrameSize(), inRecordDesc);
        copyFrameAccessor.reset(copyFrame);
        ByteBuffer outFrame = ctx.allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(
                ctx.getFrameSize());
        appender.reset(outFrame, true);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private PreclusteredGroupWriter pgw;

            @Override
            public void open() throws HyracksDataException {
                pgw = new PreclusteredGroupWriter(ctx, groupFields,
                        comparators, aggregator, inRecordDesc, writer);
                pgw.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
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
        };
    }
}