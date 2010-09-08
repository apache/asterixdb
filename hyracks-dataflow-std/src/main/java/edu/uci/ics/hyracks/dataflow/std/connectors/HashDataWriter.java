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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IEndpointDataWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class HashDataWriter implements IFrameWriter {
    private final int consumerPartitionCount;
    private final IFrameWriter[] epWriters;
    private final FrameTupleAppender[] appenders;
    private final FrameTupleAccessor tupleAccessor;
    private final ITuplePartitionComputer tpc;

    public HashDataWriter(IHyracksContext ctx, int consumerPartitionCount, IEndpointDataWriterFactory edwFactory,
            RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc) throws HyracksDataException {
        this.consumerPartitionCount = consumerPartitionCount;
        epWriters = new IFrameWriter[consumerPartitionCount];
        appenders = new FrameTupleAppender[consumerPartitionCount];
        for (int i = 0; i < consumerPartitionCount; ++i) {
            try {
                epWriters[i] = edwFactory.createFrameWriter(i);
                appenders[i] = new FrameTupleAppender(ctx);
                appenders[i].reset(ctx.getResourceManager().allocateFrame(), true);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        tupleAccessor = new FrameTupleAccessor(ctx, recordDescriptor);
        this.tpc = tpc;
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
    public void open() throws HyracksDataException {
        for (int i = 0; i < epWriters.length; ++i) {
            epWriters[i].open();
            appenders[i].reset(appenders[i].getBuffer(), true);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            int h = tpc.partition(tupleAccessor, i, consumerPartitionCount);
            FrameTupleAppender appender = appenders[h];
            if (!appender.append(tupleAccessor, i)) {
                ByteBuffer appenderBuffer = appender.getBuffer();
                flushFrame(appenderBuffer, epWriters[h]);
                appender.reset(appenderBuffer, true);
                if (!appender.append(tupleAccessor, i)) {
                    throw new IllegalStateException();
                }
            }
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