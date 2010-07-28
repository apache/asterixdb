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
package edu.uci.ics.hyracks.coreops;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IEndpointDataWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.comm.NonDeterministicFrameReader;
import edu.uci.ics.hyracks.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.context.HyracksContext;
import edu.uci.ics.hyracks.coreops.base.AbstractConnectorDescriptor;

public class MToNRangePartitioningConnectorDescriptor extends AbstractConnectorDescriptor {
    private static final long serialVersionUID = 1L;

    private class RangeDataWriter implements IFrameWriter {
        private final int consumerPartitionCount;
        private final IFrameWriter[] epWriters;
        private final FrameTupleAppender[] appenders;
        private final FrameTupleAccessor tupleAccessor;

        public RangeDataWriter(HyracksContext ctx, int consumerPartitionCount, IFrameWriter[] epWriters,
            FrameTupleAppender[] appenders, RecordDescriptor recordDescriptor) {
            this.consumerPartitionCount = consumerPartitionCount;
            this.epWriters = epWriters;
            this.appenders = appenders;
            tupleAccessor = new FrameTupleAccessor(ctx, recordDescriptor);
        }

        @Override
        public void close() throws HyracksDataException {
            for (int i = 0; i < epWriters.length; ++i) {
                if (appenders[i].getTupleCount() > 0) {
                    flushFrame(appenders[i].getBuffer(), epWriters[i]);
                }
                epWriters[i].close();
            }
        }

        private void flushFrame(ByteBuffer buffer, IFrameWriter frameWriter) throws HyracksDataException {
            buffer.position(0);
            buffer.limit(buffer.capacity());
            frameWriter.nextFrame(buffer);
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            if (true) {
                throw new UnsupportedOperationException();
            }
            tupleAccessor.reset(buffer);
            int slotLength = tupleAccessor.getFieldSlotsLength();
            int tupleCount = tupleAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; ++i) {
                int startOffset = tupleAccessor.getTupleStartOffset(i);
                int fStart = tupleAccessor.getFieldStartOffset(i, partitioningField);
                int fEnd = tupleAccessor.getFieldEndOffset(i, partitioningField);
                int h = 0;
                FrameTupleAppender appender = appenders[h];
                if (!appender.append(tupleAccessor, i)) {
                    ByteBuffer appenderBuffer = appender.getBuffer();
                    flushFrame(appenderBuffer, epWriters[h]);
                    appender.reset(appenderBuffer, true);
                }
            }
        }

        @Override
        public void open() throws HyracksDataException {
            for (int i = 0; i < epWriters.length; ++i) {
                epWriters[i].open();
                appenders[i].reset(appenders[i].getBuffer(), true);
            }
        }
    }

    private final int partitioningField;
    private final Object[] splitVector;

    public MToNRangePartitioningConnectorDescriptor(JobSpecification spec, int partitioningField, Object[] splitVector) {
        super(spec);
        this.partitioningField = partitioningField;
        this.splitVector = splitVector;
    }

    @Override
    public IFrameWriter createSendSideWriter(HyracksContext ctx, JobPlan plan, IEndpointDataWriterFactory edwFactory,
        int index, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        JobSpecification spec = plan.getJobSpecification();
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        final FrameTupleAppender[] appenders = new FrameTupleAppender[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; ++i) {
            try {
                epWriters[i] = edwFactory.createFrameWriter(i);
                appenders[i] = new FrameTupleAppender(ctx);
                appenders[i].reset(ctx.getResourceManager().allocateFrame(), true);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        final RangeDataWriter rangeWriter = new RangeDataWriter(ctx, nConsumerPartitions, epWriters, appenders, spec
            .getConnectorRecordDescriptor(this));
        return rangeWriter;
    }

    @Override
    public IFrameReader createReceiveSideReader(HyracksContext ctx, JobPlan plan, IConnectionDemultiplexer demux,
        int index, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        return new NonDeterministicFrameReader(ctx, demux);
    }
}