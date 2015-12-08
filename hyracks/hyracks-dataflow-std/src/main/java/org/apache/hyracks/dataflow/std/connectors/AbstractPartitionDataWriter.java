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
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public abstract class AbstractPartitionDataWriter implements IFrameWriter {
    protected final int consumerPartitionCount;
    protected final IFrameWriter[] pWriters;
    protected final FrameTupleAppender[] appenders;
    protected final FrameTupleAccessor tupleAccessor;
    protected final IHyracksTaskContext ctx;
    protected boolean allocatedFrame = false;

    public AbstractPartitionDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount,
            IPartitionWriterFactory pwFactory, RecordDescriptor recordDescriptor) throws HyracksDataException {
        this.consumerPartitionCount = consumerPartitionCount;
        pWriters = new IFrameWriter[consumerPartitionCount];
        appenders = new FrameTupleAppender[consumerPartitionCount];
        for (int i = 0; i < consumerPartitionCount; ++i) {
            try {
                pWriters[i] = pwFactory.createFrameWriter(i);
                appenders[i] = new FrameTupleAppender();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        this.ctx = ctx;
    }

    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            if (allocatedFrame) {
                appenders[i].flush(pWriters[i], true);
            }
            pWriters[i].close();
        }
    }

    @Override
    public void open() throws HyracksDataException {
        for (int i = 0; i < pWriters.length; ++i) {
            pWriters[i].open();
        }
    }

    @Override
    abstract public void nextFrame(ByteBuffer buffer) throws HyracksDataException;

    protected void allocateFrames() throws HyracksDataException {
        for (int i = 0; i < appenders.length; ++i) {
            appenders[i].reset(new VSizeFrame(ctx), true);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        for (int i = 0; i < appenders.length; ++i) {
            pWriters[i].fail();
        }
    }
}
