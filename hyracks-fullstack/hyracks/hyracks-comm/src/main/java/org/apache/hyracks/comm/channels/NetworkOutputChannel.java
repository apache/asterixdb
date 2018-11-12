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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

import org.apache.hyracks.api.comm.IBufferAcceptor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.net.protocols.muxdemux.AbstractChannelWriteInterface;
import org.apache.hyracks.net.protocols.muxdemux.ChannelControlBlock;

public class NetworkOutputChannel implements IFrameWriter {
    private final ChannelControlBlock ccb;

    private final int nBuffers;

    private final Deque<ByteBuffer> emptyStack;

    private boolean aborted;

    private int frameSize = 32768;

    private int allocateCounter = 0;

    public NetworkOutputChannel(ChannelControlBlock ccb, int nBuffers) {
        this.ccb = ccb;
        this.nBuffers = nBuffers;
        emptyStack = new ArrayDeque<>(nBuffers);
        ccb.getWriteInterface().setEmptyBufferAcceptor(new WriteEmptyBufferAcceptor());
    }

    public void setFrameSize(int frameSize) {
        this.frameSize = frameSize;
    }

    @Override
    public void open() {
        // no op
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        ByteBuffer destBuffer;
        while (buffer.hasRemaining()) {
            synchronized (this) {
                while (true) {
                    if (aborted) {
                        throw new HyracksDataException("Connection has been aborted");
                    }
                    destBuffer = emptyStack.poll();
                    if (destBuffer == null && allocateCounter < nBuffers) {
                        destBuffer = ByteBuffer.allocateDirect(frameSize);
                        allocateCounter++;
                    }
                    if (destBuffer != null) {
                        break;
                    }
                    try {
                        InetSocketAddress remoteAddress = ccb.getRemoteAddress();
                        String nameBefore = Thread.currentThread().getName();
                        try {
                            Thread.currentThread().setName(
                                    nameBefore + ":SendingTo(" + Objects.toString(remoteAddress) + ") over " + ccb);
                            wait();
                        } finally {
                            Thread.currentThread().setName(nameBefore);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw HyracksDataException.create(e);
                    }
                }
            }
            destBuffer.clear();
            if (destBuffer.capacity() < buffer.remaining()) {
                destBuffer.put(buffer.array(), buffer.position(), destBuffer.capacity());
                buffer.position(buffer.position() + destBuffer.capacity());
            } else {
                destBuffer.put(buffer);
            }
            destBuffer.flip();
            ccb.getWriteInterface().getFullBufferAcceptor().accept(destBuffer);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        ccb.getWriteInterface().getFullBufferAcceptor().error(AbstractChannelWriteInterface.REMOTE_ERROR_CODE);
    }

    @Override
    public void close() throws HyracksDataException {
        ccb.getWriteInterface().getFullBufferAcceptor().close();
    }

    public void abort(int ecode) {
        ccb.getWriteInterface().getFullBufferAcceptor().error(ecode);
        synchronized (NetworkOutputChannel.this) {
            aborted = true;
            NetworkOutputChannel.this.notifyAll();
        }
    }

    private class WriteEmptyBufferAcceptor implements IBufferAcceptor {
        @Override
        public void accept(ByteBuffer buffer) {
            synchronized (NetworkOutputChannel.this) {
                emptyStack.push(buffer);
                NetworkOutputChannel.this.notifyAll();
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        // At the network boundary.
        // This frame writer always pushes its content
    }
}
