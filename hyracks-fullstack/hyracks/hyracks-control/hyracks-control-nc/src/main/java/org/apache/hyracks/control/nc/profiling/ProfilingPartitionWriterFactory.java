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
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.profiling.counters.MultiResolutionEventProfiler;
import org.apache.hyracks.control.common.job.profiling.om.PartitionProfile;
import org.apache.hyracks.control.nc.Task;

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
                try {
                    ((Task) ctx).setPartitionSendProfile(
                            new PartitionProfile(new PartitionId(ctx.getJobletContext().getJobId(), cd.getConnectorId(),
                                    senderIndex, receiverIndex), openTime, closeTime, mrep));
                } finally {
                    writer.close();
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                writer.flush();
            }
        };
    }
}
