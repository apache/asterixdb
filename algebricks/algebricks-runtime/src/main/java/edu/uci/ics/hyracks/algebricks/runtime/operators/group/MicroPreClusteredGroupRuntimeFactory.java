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
package edu.uci.ics.hyracks.algebricks.runtime.operators.group;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.preclustered.PreclusteredGroupWriter;

public class MicroPreClusteredGroupRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final RecordDescriptor inRecordDesc;
    private final RecordDescriptor outRecordDesc;

    public MicroPreClusteredGroupRuntimeFactory(int[] groupFields, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDesc, int[] projectionList) {
        super(projectionList);
        // Obs: the projection list is currently ignored.
        if (projectionList != null) {
            throw new NotImplementedException("Cannot push projection into InMemorySortRuntime.");
        }
        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        this.inRecordDesc = inRecordDesc;
        this.outRecordDesc = outRecordDesc;
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws AlgebricksException {
        try {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            final ByteBuffer copyFrame = ctx.allocateFrame();
            final FrameTupleAccessor copyFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecordDesc);
            copyFrameAccessor.reset(copyFrame);
            ByteBuffer outFrame = ctx.allocateFrame();
            final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
            appender.reset(outFrame, true);

            return new AbstractOneInputOneOutputPushRuntime() {

                private PreclusteredGroupWriter pgw;

                @Override
                public void open() throws HyracksDataException {
                    IAggregatorDescriptor aggregator = aggregatorFactory.createAggregator(ctx, inRecordDesc,
                            outRecordDesc, groupFields, groupFields, writer);
                    pgw = new PreclusteredGroupWriter(ctx, groupFields, comparators, aggregator, inRecordDesc,
                            outRecordDesc, writer);
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
            };
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }

    }
}
