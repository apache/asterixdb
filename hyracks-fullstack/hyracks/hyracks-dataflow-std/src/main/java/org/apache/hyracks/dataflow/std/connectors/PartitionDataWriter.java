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
package org.apache.hyracks.dataflow.std.connectors;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class PartitionDataWriter implements IFrameWriter {
    private final int consumerPartitionCount;
    private final IFrameWriter[] pWriters;
    private final boolean[] isOpen;
    private final FrameTupleAppender[] appenders;
    private final FrameTupleAccessor tupleAccessor;
    private final ITuplePartitionComputer tpc;
    private final IHyracksTaskContext ctx;
    private boolean[] allocatedFrames;

    public PartitionDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount, IPartitionWriterFactory pwFactory,
            RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc) throws HyracksDataException {
        this.consumerPartitionCount = consumerPartitionCount;
        pWriters = new IFrameWriter[consumerPartitionCount];
        isOpen = new boolean[consumerPartitionCount];
        allocatedFrames = new boolean[consumerPartitionCount];
        appenders = new FrameTupleAppender[consumerPartitionCount];
        for (int i = 0; i < consumerPartitionCount; ++i) {
            try {
                pWriters[i] = pwFactory.createFrameWriter(i);
                appenders[i] = createTupleAppender(ctx);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        this.tpc = tpc;
        this.ctx = ctx;
    }

    protected FrameTupleAppender createTupleAppender(IHyracksTaskContext ctx) {
        return new FrameTupleAppender();
    }

    @Override
    public void close() throws HyracksDataException {
        HyracksDataException closeException = null;
        for (int i = 0; i < pWriters.length; ++i) {
            if (isOpen[i]) {
                if (allocatedFrames[i] && appenders[i].getTupleCount() > 0) {
                    try {
                        appenders[i].write(pWriters[i], true);
                    } catch (Throwable th) {
                        if (closeException == null) {
                            closeException = new HyracksDataException(th);
                        } else {
                            closeException.addSuppressed(th);
                        }
                    }
                }
                try {
                    pWriters[i].close();
                } catch (Throwable th) {
                    if (closeException == null) {
                        closeException = new HyracksDataException(th);
                    } else {
                        closeException.addSuppressed(th);
                    }
                }
            }
        }
        if (closeException != null) {
            throw closeException;
        }
    }

    @Override
    public void open() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            isOpen[i] = true;
            pWriters[i].open();
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            int h = tpc.partition(tupleAccessor, i, consumerPartitionCount);
            if (!allocatedFrames[h]) {
                allocateFrames(h);
            }
            FrameUtils.appendToWriter(pWriters[h], appenders[h], tupleAccessor, i);
        }
    }

    protected void allocateFrames(int i) throws HyracksDataException {
        appenders[i].reset(new VSizeFrame(ctx), true);
        allocatedFrames[i] = true;
    }

    @Override
    public void fail() throws HyracksDataException {
        HyracksDataException failException = null;
        for (int i = 0; i < appenders.length; ++i) {
            if (isOpen[i]) {
                try {
                    pWriters[i].fail();
                } catch (Throwable th) {
                    if (failException == null) {
                        failException = new HyracksDataException(th);
                    } else {
                        failException.addSuppressed(th);
                    }
                }
            }
        }
        if (failException != null) {
            throw failException;
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        for (int i = 0; i < consumerPartitionCount; i++) {
            if (allocatedFrames[i]) {
                appenders[i].flush(pWriters[i]);
            }
        }
    }
}
