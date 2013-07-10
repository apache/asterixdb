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

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataReader;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FrameDeserializingDataReader implements IOpenableDataReader<Object[]> {
    private final ByteBuffer buffer;

    private boolean eos;

    private boolean first;

    private final IFrameReader frameReader;

    private final FrameDeserializer frameDeserializer;

    public FrameDeserializingDataReader(IHyracksTaskContext ctx, IFrameReader frameReader,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        buffer = ctx.allocateFrame();
        this.frameReader = frameReader;
        this.frameDeserializer = new FrameDeserializer(ctx.getFrameSize(), recordDescriptor);
    }

    @Override
    public void open() throws HyracksDataException {
        frameReader.open();
        buffer.clear();
        buffer.flip();
        eos = false;
        first = true;
    }

    @Override
    public void close() throws HyracksDataException {
        frameReader.close();
        frameDeserializer.close();
    }

    @Override
    public Object[] readData() throws HyracksDataException {
        while (true) {
            if (eos) {
                return null;
            }
            if (!first && !frameDeserializer.done()) {
                return frameDeserializer.deserializeRecord();
            }
            buffer.clear();
            if (!frameReader.nextFrame(buffer)) {
                eos = true;
            } else {
                frameDeserializer.reset(buffer);
            }
            first = false;
        }
    }
}