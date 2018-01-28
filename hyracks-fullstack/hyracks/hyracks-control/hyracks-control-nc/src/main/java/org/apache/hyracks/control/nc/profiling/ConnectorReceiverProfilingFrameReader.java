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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.counters.ICounter;

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
        this.frameCounter =
                ctx.getCounterContext().getCounter(cdId + ".receiver." + receiverIndex + ".nextFrame", true);
    }

    @Override
    public void open() throws HyracksDataException {
        reader.open();
        openCounter.update(1);
    }

    @Override
    public boolean nextFrame(IFrame frame) throws HyracksDataException {
        boolean status = reader.nextFrame(frame);
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
