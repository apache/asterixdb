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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOpenableDataWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class SerializingDataWriter implements IOpenableDataWriter<Object[]> {
    private static final Logger LOGGER = Logger.getLogger(SerializingDataWriter.class.getName());

    private final ArrayTupleBuilder tb;

    private final RecordDescriptor recordDescriptor;

    private final IFrameWriter frameWriter;

    private final FrameTupleAppender tupleAppender;

    private boolean open;

    public SerializingDataWriter(IHyracksTaskContext ctx, RecordDescriptor recordDescriptor, IFrameWriter frameWriter)
            throws HyracksDataException {
        tb = new ArrayTupleBuilder(recordDescriptor.getFieldCount());
        this.recordDescriptor = recordDescriptor;
        this.frameWriter = frameWriter;
        tupleAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        open = false;
    }

    @Override
    public void open() throws HyracksDataException {
        frameWriter.open();
        open = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            throw new HyracksDataException("Closing SerializingDataWriter that has not been opened");
        }
        tupleAppender.flush(frameWriter, true);
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
        FrameUtils.appendToWriter(frameWriter, tupleAppender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                tb.getSize());
    }

    @Override
    public void fail() throws HyracksDataException {
        frameWriter.fail();
    }
}
