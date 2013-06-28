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
package edu.uci.ics.hyracks.control.nc.profiling;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;

public class ConnectorSenderProfilingFrameWriter implements IFrameWriter {
    private final IFrameWriter writer;
    private final ICounter openCounter;
    private final ICounter closeCounter;
    private final ICounter frameCounter;

    public ConnectorSenderProfilingFrameWriter(IHyracksTaskContext ctx, IFrameWriter writer,
            ConnectorDescriptorId cdId, int senderIndex, int receiverIndex) {
        this.writer = writer;
        int attempt = ctx.getTaskAttemptId().getAttempt();
        this.openCounter = ctx.getCounterContext().getCounter(
                cdId + ".sender." + attempt + "." + senderIndex + "." + receiverIndex + ".open", true);
        this.closeCounter = ctx.getCounterContext().getCounter(
                cdId + ".sender." + attempt + "." + senderIndex + "." + receiverIndex + ".close", true);
        this.frameCounter = ctx.getCounterContext().getCounter(
                cdId + ".sender." + attempt + "." + senderIndex + "." + receiverIndex + ".nextFrame", true);
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
        closeCounter.update(1);
        writer.close();
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}