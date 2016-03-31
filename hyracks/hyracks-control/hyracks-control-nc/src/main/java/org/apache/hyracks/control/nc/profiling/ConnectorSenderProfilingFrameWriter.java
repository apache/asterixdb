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
package org.apache.hyracks.control.nc.profiling;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.counters.ICounter;

public class ConnectorSenderProfilingFrameWriter implements IFrameWriter {
    private final IFrameWriter writer;
    private final ICounter openCounter;
    private final ICounter closeCounter;
    private final ICounter frameCounter;

    public ConnectorSenderProfilingFrameWriter(IHyracksTaskContext ctx, IFrameWriter writer, ConnectorDescriptorId cdId,
            int senderIndex, int receiverIndex) {
        this.writer = writer;
        int attempt = ctx.getTaskAttemptId().getAttempt();
        this.openCounter = ctx.getCounterContext()
                .getCounter(cdId + ".sender." + attempt + "." + senderIndex + "." + receiverIndex + ".open", true);
        this.closeCounter = ctx.getCounterContext()
                .getCounter(cdId + ".sender." + attempt + "." + senderIndex + "." + receiverIndex + ".close", true);
        this.frameCounter = ctx.getCounterContext()
                .getCounter(cdId + ".sender." + attempt + "." + senderIndex + "." + receiverIndex + ".nextFrame", true);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        openCounter.update(1);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        frameCounter.update(1);
        writer.nextFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            closeCounter.update(1);
        } finally {
            writer.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }
}
