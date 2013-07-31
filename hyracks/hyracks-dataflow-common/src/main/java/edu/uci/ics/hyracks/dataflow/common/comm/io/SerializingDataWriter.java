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
package edu.uci.ics.hyracks.dataflow.common.comm.io;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class SerializingDataWriter implements IOpenableDataWriter<Object[]> {
    private static final Logger LOGGER = Logger.getLogger(SerializingDataWriter.class.getName());

    private final ByteBuffer buffer;

    private final ArrayTupleBuilder tb;

    private final RecordDescriptor recordDescriptor;

    private final IFrameWriter frameWriter;

    private final FrameTupleAppender tupleAppender;

    private boolean open;

    public SerializingDataWriter(IHyracksTaskContext ctx, RecordDescriptor recordDescriptor, IFrameWriter frameWriter)
            throws HyracksDataException {
        buffer = ctx.allocateFrame();
        tb = new ArrayTupleBuilder(recordDescriptor.getFieldCount());
        this.recordDescriptor = recordDescriptor;
        this.frameWriter = frameWriter;
        tupleAppender = new FrameTupleAppender(ctx.getFrameSize());
        open = false;
    }

    @Override
    public void open() throws HyracksDataException {
        frameWriter.open();
        buffer.clear();
        open = true;
        tupleAppender.reset(buffer, true);
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            throw new HyracksDataException("Closing SerializingDataWriter that has not been opened");
        }
        if (tupleAppender.getTupleCount() > 0) {
            flushFrame();
        }
        frameWriter.close();
        open = false;
    }

    @Override
    public void writeData(Object[] data) throws HyracksDataException {
        if (!open) {
            throw new HyracksDataException("Writing to SerializingDataWriter that has not been opened");
        }
        tb.reset();
        for (int i = 0; i < data.length; ++i) {
            Object instance = data[i];
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            tb.addField(recordDescriptor.getFields()[i], instance);
        }
        if (!tupleAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest("Flushing: position = " + buffer.position());
            }
            flushFrame();
            tupleAppender.reset(buffer, true);
            if (!tupleAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size (" + buffer.capacity() + ")");
            }
        }
    }

    private void flushFrame() throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        frameWriter.nextFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        frameWriter.fail();
    }
}
