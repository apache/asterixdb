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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOpenableDataWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FrameDeserializingDataWriter implements IFrameWriter {
    private final IOpenableDataWriter<Object[]> writer;
    private final FrameDeserializer frameDeserializer;

    public FrameDeserializingDataWriter(IHyracksTaskContext ctx, IOpenableDataWriter<Object[]> writer,
            RecordDescriptor recordDescriptor) {
        this.writer = writer;
        this.frameDeserializer = new FrameDeserializer(recordDescriptor);
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        frameDeserializer.reset(buffer);
        while (!frameDeserializer.done()) {
            Object[] tuple = frameDeserializer.deserializeRecord();
            writer.writeData(tuple);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void fail() {
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }
}
