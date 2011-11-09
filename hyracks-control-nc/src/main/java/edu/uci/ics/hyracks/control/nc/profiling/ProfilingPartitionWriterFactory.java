/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.profiling.om.PartitionProfile;
import edu.uci.ics.hyracks.control.nc.Task;

public class ProfilingPartitionWriterFactory implements IPartitionWriterFactory {
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

            private long prevTime;

            private ByteArrayOutputStream baos = new ByteArrayOutputStream();

            @Override
            public void open() throws HyracksDataException {
                baos.reset();
                openTime = System.currentTimeMillis();
                prevTime = openTime;
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                long time = System.currentTimeMillis();
                long diff = time - prevTime;
                prevTime = time;
                do {
                    byte b = (byte) (diff & 0xef);
                    diff >>= 7;
                    if (diff != 0) {
                        b |= 0x80;
                    }
                    baos.write(b);
                } while (diff != 0);
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
                        .getJobId(), cd.getConnectorId(), senderIndex, receiverIndex), openTime, closeTime, baos
                        .toByteArray()));
                writer.close();
            }
        };
    }
}