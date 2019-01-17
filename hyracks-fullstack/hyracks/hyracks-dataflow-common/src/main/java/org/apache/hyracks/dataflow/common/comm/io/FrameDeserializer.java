/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.common.comm.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FrameDeserializer {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ByteBufferInputStream bbis;

    private final DataInputStream di;

    private final RecordDescriptor recordDescriptor;

    private final FrameTupleAccessor frameTupleAccessor;

    private int tupleCount;

    private int tIndex;

    private ByteBuffer buffer;

    public FrameDeserializer(RecordDescriptor recordDescriptor) {
        this.bbis = new ByteBufferInputStream();
        this.di = new DataInputStream(bbis);
        this.recordDescriptor = recordDescriptor;
        frameTupleAccessor = new FrameTupleAccessor(recordDescriptor);
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
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(i + " " + LogRedactionUtil.userData(instance.toString()));
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
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Read Record tIndex = " + tIndex + ", tupleCount = " + tupleCount);
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
