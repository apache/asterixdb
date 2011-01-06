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
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IEndpointDataWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.NonDeterministicFrameReader;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractConnectorDescriptor;

public class MToNRangePartitioningConnectorDescriptor extends AbstractConnectorDescriptor {
    private static final long serialVersionUID = 1L;

    private class RangeDataWriter implements IFrameWriter {
        private final IFrameWriter[] epWriters;
        private final FrameTupleAppender[] appenders;
        private final FrameTupleAccessor tupleAccessor;

        public RangeDataWriter(IHyracksStageletContext ctx, int consumerPartitionCount, IFrameWriter[] epWriters,
                FrameTupleAppender[] appenders, RecordDescriptor recordDescriptor) {
            this.epWriters = epWriters;
            this.appenders = appenders;
            tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
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

        @Override
        public void flush() throws HyracksDataException {
            for (int i = 0; i < appenders.length; ++i) {
                FrameTupleAppender appender = appenders[i];
                if (appender.getTupleCount() > 0) {
                    ByteBuffer buffer = appender.getBuffer();
                    IFrameWriter frameWriter = epWriters[i];
                    flushFrame(buffer, frameWriter);
                    epWriters[i].flush();
                    appender.reset(buffer, true);
                }
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
    public IFrameWriter createSendSideWriter(IHyracksStageletContext ctx, RecordDescriptor recordDesc,
            IEndpointDataWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final IFrameWriter[] epWriters = new IFrameWriter[nConsumerPartitions];
        final FrameTupleAppender[] appenders = new FrameTupleAppender[nConsumerPartitions];
        for (int i = 0; i < nConsumerPartitions; ++i) {
            try {
                epWriters[i] = edwFactory.createFrameWriter(i);
                appenders[i] = new FrameTupleAppender(ctx.getFrameSize());
                appenders[i].reset(ctx.allocateFrame(), true);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        final RangeDataWriter rangeWriter = new RangeDataWriter(ctx, nConsumerPartitions, epWriters, appenders,
                recordDesc);
        return rangeWriter;
    }

    @Override
    public IFrameReader createReceiveSideReader(IHyracksStageletContext ctx, RecordDescriptor recordDesc,
            IConnectionDemultiplexer demux, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        return new NonDeterministicFrameReader(ctx, demux);
    }
}