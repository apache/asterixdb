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

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;

public class ConnectorReceiverProfilingFrameReader implements IFrameReader {
    private final IFrameReader reader;
    private final ICounter openCounter;
    private final ICounter closeCounter;
    private final ICounter frameCounter;

    public ConnectorReceiverProfilingFrameReader(IHyracksTaskContext ctx, IFrameReader reader,
            ConnectorDescriptorId cdId, int receiverIndex) {
        this.reader = reader;
        this.openCounter = ctx.getCounterContext().getCounter(cdId + ".receiver." + receiverIndex + ".open", true);
        this.closeCounter = ctx.getCounterContext().getCounter(cdId + ".receiver." + receiverIndex + ".close", true);
        this.frameCounter = ctx.getCounterContext()
                .getCounter(cdId + ".receiver." + receiverIndex + ".nextFrame", true);
    }

    @Override
    public void open() throws HyracksDataException {
        reader.open();
        openCounter.update(1);
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        boolean status = reader.nextFrame(buffer);
        if (status) {
            frameCounter.update(1);
        }
        return status;
    }

    @Override
    public void close() throws HyracksDataException {
        reader.close();
        closeCounter.update(1);
    }
}