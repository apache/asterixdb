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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOpenableDataReader;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FrameDeserializingDataReader implements IOpenableDataReader<Object[]> {
    private final IFrame frame;

    private boolean eos;

    private boolean first;

    private final IFrameReader frameReader;

    private final FrameDeserializer frameDeserializer;

    public FrameDeserializingDataReader(IHyracksTaskContext ctx, IFrameReader frameReader,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        this.frame = new VSizeFrame(ctx);
        this.frameReader = frameReader;
        this.frameDeserializer = new FrameDeserializer(recordDescriptor);
    }

    @Override
    public void open() throws HyracksDataException {
        frameReader.open();
        frame.reset();
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
            frame.reset();
            if (!frameReader.nextFrame(frame)) {
                eos = true;
            } else {
                frameDeserializer.reset(frame.getBuffer());
            }
            first = false;
        }
    }
}
