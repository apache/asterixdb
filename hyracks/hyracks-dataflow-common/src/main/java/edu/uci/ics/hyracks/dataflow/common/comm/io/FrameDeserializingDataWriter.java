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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FrameDeserializingDataWriter implements IFrameWriter {
    private final IOpenableDataWriter<Object[]> writer;
    private final FrameDeserializer frameDeserializer;

    public FrameDeserializingDataWriter(IHyracksTaskContext ctx, IOpenableDataWriter<Object[]> writer,
            RecordDescriptor recordDescriptor) {
        this.writer = writer;
        this.frameDeserializer = new FrameDeserializer(ctx.getFrameSize(), recordDescriptor);
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
}