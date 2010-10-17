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

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class PreclusteredGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private final int[] groupFields;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAccumulatingAggregatorFactory aggregatorFactory;

    private static final long serialVersionUID = 1L;

    public PreclusteredGroupOperatorDescriptor(JobSpecification spec, int[] groupFields,
            IBinaryComparatorFactory[] comparatorFactories, IAccumulatingAggregatorFactory aggregatorFactory,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.groupFields = groupFields;
        this.comparatorFactories = comparatorFactories;
        this.aggregatorFactory = aggregatorFactory;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
        final IAccumulatingAggregator aggregator = aggregatorFactory.createAggregator(ctx, inRecordDesc,
                recordDescriptors[0]);
        final ByteBuffer copyFrame = ctx.getResourceManager().allocateFrame();
        final FrameTupleAccessor inFrameAccessor = new FrameTupleAccessor(ctx, inRecordDesc);
        final FrameTupleAccessor copyFrameAccessor = new FrameTupleAccessor(ctx, inRecordDesc);
        copyFrameAccessor.reset(copyFrame);
        ByteBuffer outFrame = ctx.getResourceManager().allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(ctx);
        appender.reset(outFrame, true);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private boolean first;

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                first = true;
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                inFrameAccessor.reset(buffer);
                int nTuples = inFrameAccessor.getTupleCount();
                for (int i = 0; i < nTuples; ++i) {
                    if (first) {
                        aggregator.init(inFrameAccessor, i);
                        first = false;
                    } else {
                        if (i == 0) {
                            switchGroupIfRequired(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1,
                                    inFrameAccessor, i);
                        } else {
                            switchGroupIfRequired(inFrameAccessor, i - 1, inFrameAccessor, i);
                        }
                    }
                    aggregator.accumulate(inFrameAccessor, i);
                }
                FrameUtils.copy(buffer, copyFrame);
            }

            private void switchGroupIfRequired(FrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
                    FrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
                if (!sameGroup(prevTupleAccessor, prevTupleIndex, currTupleAccessor, currTupleIndex)) {
                    writeOutput(prevTupleAccessor, prevTupleIndex);
                    aggregator.init(currTupleAccessor, currTupleIndex);
                }
            }

            private void writeOutput(final FrameTupleAccessor lastTupleAccessor, int lastTupleIndex)
                    throws HyracksDataException {
                if (!aggregator.output(appender, lastTupleAccessor, lastTupleIndex, groupFields)) {
                    FrameUtils.flushFrame(appender.getBuffer(), writer);
                    appender.reset(appender.getBuffer(), true);
                    if (!aggregator.output(appender, lastTupleAccessor, lastTupleIndex, groupFields)) {
                        throw new IllegalStateException();
                    }
                }
            }

            private boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx) {
                for (int i = 0; i < comparators.length; ++i) {
                    int fIdx = groupFields[i];
                    int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength()
                            + a1.getFieldStartOffset(t1Idx, fIdx);
                    int l1 = a1.getFieldLength(t1Idx, fIdx);
                    int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength()
                            + a2.getFieldStartOffset(t2Idx, fIdx);
                    int l2 = a2.getFieldLength(t2Idx, fIdx);
                    if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void flush() throws HyracksDataException {
                FrameUtils.flushFrame(appender.getBuffer(), writer);
                appender.reset(appender.getBuffer(), true);
            }

            @Override
            public void close() throws HyracksDataException {
                if (!first) {
                    writeOutput(copyFrameAccessor, copyFrameAccessor.getTupleCount() - 1);
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(appender.getBuffer(), writer);
                    }
                }
                writer.close();
            }
        };
    }
}