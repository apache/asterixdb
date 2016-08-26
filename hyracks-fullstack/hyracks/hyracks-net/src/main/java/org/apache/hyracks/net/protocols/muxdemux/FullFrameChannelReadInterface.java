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
package org.apache.hyracks.net.protocols.muxdemux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hyracks.api.comm.IBufferFactory;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.exceptions.NetException;

public class FullFrameChannelReadInterface extends AbstractChannelReadInterface {

    private final Deque<ByteBuffer> riEmptyStack;
    private final IChannelControlBlock ccb;

    FullFrameChannelReadInterface(IChannelControlBlock ccb) {
        this.ccb = ccb;
        riEmptyStack = new ArrayDeque<>();
        credits = 0;

        emptyBufferAcceptor = buffer -> {
            int delta = buffer.remaining();
            synchronized (ccb) {
                if (ccb.isRemotelyClosed()) {
                    return;
                }
                riEmptyStack.push(buffer);
            }
            ccb.addPendingCredits(delta);
        };
    }

    @Override
    public int read(SocketChannel sc, int size) throws IOException, NetException {
        while (true) {
            if (size <= 0) {
                return size;
            }
            if (currentReadBuffer == null) {
                currentReadBuffer = riEmptyStack.poll();
                //if current buffer == null and limit not reached
                // factory.createBuffer factory
                if (currentReadBuffer == null) {
                    currentReadBuffer = bufferFactory.createBuffer();
                }
            }
            int rSize = Math.min(size, currentReadBuffer.remaining());
            if (rSize > 0) {
                currentReadBuffer.limit(currentReadBuffer.position() + rSize);
                int len;
                try {
                    len = sc.read(currentReadBuffer);
                    if (len < 0) {
                        throw new NetException("Socket Closed");
                    }
                } finally {
                    currentReadBuffer.limit(currentReadBuffer.capacity());
                }
                size -= len;
                if (len < rSize) {
                    return size;
                }
            } else {
                return size;
            }
            if (currentReadBuffer.remaining() <= 0) {
                flush();
            }
        }
    }

    @Override
    public void setBufferFactory(IBufferFactory bufferFactory, int limit, int frameSize) {
        super.setBufferFactory(bufferFactory, limit, frameSize);
        ccb.addPendingCredits(limit * frameSize);
    }
}