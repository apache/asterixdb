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
package org.apache.hyracks.comm.channels;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.channels.IInputChannelMonitor;
import org.apache.hyracks.api.comm.IBufferAcceptor;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FileNetworkInputChannel implements IInputChannel {

    private static final int NUM_READ_BUFFERS = 1;
    public static final long FILE_CHANNEL_CODE = -1;

    private final IChannelConnectionFactory netManager;
    private final SocketAddress remoteAddress;
    private final long jobId;
    private final long fileId;
    private final Queue<ByteBuffer> fullQueue;
    private final int nBuffers;
    private IChannelControlBlock ccb;
    private IInputChannelMonitor monitor;
    private Object attachment;

    public FileNetworkInputChannel(IChannelConnectionFactory netManager, SocketAddress remoteAddress, long jobId,
            long fileId) {
        this.netManager = netManager;
        this.remoteAddress = remoteAddress;
        this.jobId = jobId;
        this.fileId = fileId;
        this.fullQueue = new ArrayDeque<>(NUM_READ_BUFFERS);
        this.nBuffers = NUM_READ_BUFFERS;
    }

    @Override
    public void registerMonitor(IInputChannelMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    @Override
    public Object getAttachment() {
        return attachment;
    }

    @Override
    public synchronized ByteBuffer getNextBuffer() {
        return fullQueue.poll();
    }

    @Override
    public void recycleBuffer(ByteBuffer buffer) {
        buffer.clear();
        ccb.getReadInterface().getEmptyBufferAcceptor().accept(buffer);
    }

    @Override
    public void open(IHyracksCommonContext ctx) throws HyracksDataException {
        try {
            ccb = netManager.connect(remoteAddress);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        ccb.getReadInterface().setFullBufferAcceptor(new ReadFullBufferAcceptor());
        ccb.getWriteInterface().setEmptyBufferAcceptor(WriteEmptyBufferAcceptor.INSTANCE);
        ccb.getReadInterface().setBufferFactory(new ReadBufferFactory(nBuffers, ctx), nBuffers,
                ctx.getInitialFrameSize());

        ByteBuffer writeBuffer = ByteBuffer.allocate(NetworkInputChannel.INITIAL_MESSAGE_SIZE);
        writeBuffer.putLong(FILE_CHANNEL_CODE);
        writeBuffer.putLong(jobId);
        writeBuffer.putLong(fileId);
        writeBuffer.flip();

        ccb.getWriteInterface().getFullBufferAcceptor().accept(writeBuffer);
        ccb.getWriteInterface().getFullBufferAcceptor().close();
    }

    @Override
    public void close() throws HyracksDataException {

    }

    @Override
    public void fail() {
        // do nothing (covered by job lifecycle)
    }

    private class ReadFullBufferAcceptor implements ICloseableBufferAcceptor {
        @Override
        public void accept(ByteBuffer buffer) {
            fullQueue.add(buffer);
            monitor.notifyDataAvailability(FileNetworkInputChannel.this, 1);
        }

        @Override
        public void close() {
            monitor.notifyEndOfStream(FileNetworkInputChannel.this);
        }

        @Override
        public void error(int ecode) {
            monitor.notifyFailure(FileNetworkInputChannel.this, ecode);
        }
    }

    private static class WriteEmptyBufferAcceptor implements IBufferAcceptor {

        static final WriteEmptyBufferAcceptor INSTANCE = new WriteEmptyBufferAcceptor();

        @Override
        public void accept(ByteBuffer buffer) {
            // do nothing
        }
    }
}
