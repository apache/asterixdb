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
package edu.uci.ics.hyracks.dataflow.std.connectors;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class PartitionDataWriter implements IFrameWriter {
    private final int consumerPartitionCount;
    private final IFrameWriter[] pWriters;
    private final FrameTupleAppender[] appenders;
    private final FrameTupleAccessor tupleAccessor;
    private final ITuplePartitionComputer tpc;

    public PartitionDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount, IPartitionWriterFactory pwFactory,
            RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc) throws HyracksDataException {
        this.consumerPartitionCount = consumerPartitionCount;
        pWriters = new IFrameWriter[consumerPartitionCount];
        appenders = new FrameTupleAppender[consumerPartitionCount];
        for (int i = 0; i < consumerPartitionCount; ++i) {
            try {
                pWriters[i] = pwFactory.createFrameWriter(i);
                appenders[i] = new FrameTupleAppender(ctx.getFrameSize());
                appenders[i].reset(ctx.allocateFrame(), true);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        this.tpc = tpc;
    }

    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            if (appenders[i].getTupleCount() > 0) {
                flushFrame(appenders[i].getBuffer(), pWriters[i]);
            }
            pWriters[i].close();
        }
    }

    private void flushFrame(ByteBuffer buffer, IFrameWriter frameWriter) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        frameWriter.nextFrame(buffer);
    }

    @Override
    public void open() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            pWriters[i].open();
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
                flushFrame(appenderBuffer, pWriters[h]);
                appender.reset(appenderBuffer, true);
                if (!appender.append(tupleAccessor, i)) {
                    throw new HyracksDataException("Record size (" + (tupleAccessor.getTupleEndOffset(i) - tupleAccessor.getTupleStartOffset(i)) + ") larger than frame size (" + appender.getBuffer().capacity() + ")");
                }
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        for (int i = 0; i < appenders.length; ++i) {
            pWriters[i].fail();
        }
    }
}
