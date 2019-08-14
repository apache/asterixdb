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
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.util.trace.ITracer;

abstract class AbstractPartitionDataWriter implements IFrameWriter {

    protected final int consumerPartitionCount;
    private final IFrameWriter[] pWriters;
    private final boolean[] isOpen;
    private final FrameTupleAppender[] appenders;
    protected final FrameTupleAccessor tupleAccessor;
    protected final IHyracksTaskContext ctx;
    private boolean[] allocatedFrames;
    private boolean failed = false;

    public AbstractPartitionDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount,
            IPartitionWriterFactory pwFactory, RecordDescriptor recordDescriptor) throws HyracksDataException {
        this.ctx = ctx;
        this.consumerPartitionCount = consumerPartitionCount;
        pWriters = new IFrameWriter[consumerPartitionCount];
        isOpen = new boolean[consumerPartitionCount];
        allocatedFrames = new boolean[consumerPartitionCount];
        appenders = new FrameTupleAppender[consumerPartitionCount];
        tupleAccessor = new FrameTupleAccessor(recordDescriptor);
        initializeAppenders(pwFactory);
    }

    protected void initializeAppenders(IPartitionWriterFactory pwFactory) throws HyracksDataException {
        for (int i = 0; i < consumerPartitionCount; ++i) {
            try {
                pWriters[i] = pwFactory.createFrameWriter(i);
                appenders[i] = createTupleAppender(ctx);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    protected FrameTupleAppender createTupleAppender(IHyracksTaskContext ctx) {
        return new FrameTupleAppender();
    }

    @Override
    public void close() throws HyracksDataException {
        HyracksDataException closeException = null;
        if (!failed) {
            boolean newFailure = false;
            for (int i = 0; i < pWriters.length; ++i) {
                try {
                    if (isOpen[i] && allocatedFrames[i] && appenders[i].getTupleCount() > 0) {
                        appenders[i].write(pWriters[i], true);
                    }
                } catch (Exception e) {
                    newFailure = true;
                    closeException = wrapException(closeException, e);
                    break;
                }
            }
            if (newFailure) {
                try {
                    fail(); // Fail all writers if any new failure happens.
                } catch (Exception e) {
                    closeException = wrapException(closeException, e);
                }
            }
        }
        for (int i = 0; i < pWriters.length; ++i) {
            if (isOpen[i]) {
                // The try-block make sures that every writer is closed.
                try {
                    pWriters[i].close();
                } catch (Exception e) {
                    closeException = wrapException(closeException, e);
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
            processTuple(i);
        }
    }

    protected abstract void processTuple(int tupleIndex) throws HyracksDataException;

    protected void appendToPartitionWriter(int tupleIndex, int partition) throws HyracksDataException {
        if (!allocatedFrames[partition]) {
            allocateFrames(partition);
        }
        FrameUtils.appendToWriter(pWriters[partition], appenders[partition], tupleAccessor, tupleIndex);
    }

    protected void allocateFrames(int i) throws HyracksDataException {
        appenders[i].reset(new VSizeFrame(ctx), true);
        allocatedFrames[i] = true;
    }

    @Override
    public void fail() throws HyracksDataException {
        failed = true;
        HyracksDataException failException = null;
        for (int i = 0; i < appenders.length; ++i) {
            if (isOpen[i]) {
                try {
                    pWriters[i].fail();
                } catch (Exception e) {
                    failException = wrapException(failException, e);
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

    public void flush(ITracer tracer, String name, long cat, String args) throws HyracksDataException {
        for (int i = 0; i < consumerPartitionCount; i++) {
            if (allocatedFrames[i]) {
                appenders[i].flush(pWriters[i], tracer, name, cat, args);
            }
        }
    }

    // Wraps the current encountered exception into the final exception.
    private HyracksDataException wrapException(HyracksDataException finalException, Exception currentException) {
        if (finalException == null) {
            return HyracksDataException.create(currentException);
        }
        finalException.addSuppressed(currentException);
        return finalException;
    }
}
