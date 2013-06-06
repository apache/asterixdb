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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class FrameDeserializer {
    private static final Logger LOGGER = Logger.getLogger(FrameDeserializer.class.getName());

    private final ByteBufferInputStream bbis;

    private final DataInputStream di;

    private final RecordDescriptor recordDescriptor;

    private final FrameTupleAccessor frameTupleAccessor;

    private int tupleCount;

    private int tIndex;

    private ByteBuffer buffer;

    public FrameDeserializer(int frameSize, RecordDescriptor recordDescriptor) {
        this.bbis = new ByteBufferInputStream();
        this.di = new DataInputStream(bbis);
        this.recordDescriptor = recordDescriptor;
        frameTupleAccessor = new FrameTupleAccessor(frameSize, recordDescriptor);
    }

    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
        frameTupleAccessor.reset(buffer);
        tupleCount = frameTupleAccessor.getTupleCount();
        tIndex = 0;
    }

    public boolean done() {
        return tIndex >= tupleCount;
    }

    public Object[] deserializeRecord() throws HyracksDataException {
        int start = frameTupleAccessor.getTupleStartOffset(tIndex) + frameTupleAccessor.getFieldSlotsLength();
        bbis.setByteBuffer(buffer, start);
        Object[] record = new Object[recordDescriptor.getFieldCount()];
        for (int i = 0; i < record.length; ++i) {
            Object instance = recordDescriptor.getFields()[i].deserialize(di);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            record[i] = instance;
            if (FrameConstants.DEBUG_FRAME_IO) {
                try {
                    if (di.readInt() != FrameConstants.FRAME_FIELD_MAGIC) {
                        throw new HyracksDataException("Field magic mismatch");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Read Record tIndex = " + tIndex + ", tupleCount = " + tupleCount);
        }
        ++tIndex;
        return record;
    }

    public void close() {
        try {
            di.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}