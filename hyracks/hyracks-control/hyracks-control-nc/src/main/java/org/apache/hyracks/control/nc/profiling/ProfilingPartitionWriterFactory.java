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
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.MultiResolutionEventProfiler;
import edu.uci.ics.hyracks.control.common.job.profiling.om.PartitionProfile;
import edu.uci.ics.hyracks.control.nc.Task;

public class ProfilingPartitionWriterFactory implements IPartitionWriterFactory {
    private static final int N_SAMPLES = 64;

    private final IHyracksTaskContext ctx;

    private final IConnectorDescriptor cd;

    private final int senderIndex;

    private final IPartitionWriterFactory delegate;

    public ProfilingPartitionWriterFactory(IHyracksTaskContext ctx, IConnectorDescriptor cd, int senderIndex,
            IPartitionWriterFactory delegate) {
        this.ctx = ctx;
        this.cd = cd;
        this.senderIndex = senderIndex;
        this.delegate = delegate;
    }

    @Override
    public IFrameWriter createFrameWriter(final int receiverIndex) throws HyracksDataException {
        final IFrameWriter writer = new ConnectorSenderProfilingFrameWriter(ctx,
                delegate.createFrameWriter(receiverIndex), cd.getConnectorId(), senderIndex, receiverIndex);
        return new IFrameWriter() {
            private long openTime;

            private long closeTime;

            MultiResolutionEventProfiler mrep = new MultiResolutionEventProfiler(N_SAMPLES);

            @Override
            public void open() throws HyracksDataException {
                openTime = System.currentTimeMillis();
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                mrep.reportEvent();
                writer.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                closeTime = System.currentTimeMillis();
                ((Task) ctx).setPartitionSendProfile(new PartitionProfile(new PartitionId(ctx.getJobletContext()
                        .getJobId(), cd.getConnectorId(), senderIndex, receiverIndex), openTime, closeTime, mrep));
                writer.close();
            }
        };
    }
}