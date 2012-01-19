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
package edu.uci.ics.hyracks.control.nc.net;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.net.buffers.IBufferAcceptor;
import edu.uci.ics.hyracks.net.protocols.muxdemux.ChannelControlBlock;

public class NetworkOutputChannel implements IFrameWriter {
    private final ChannelControlBlock ccb;

    private final Queue<ByteBuffer> emptyQueue;

    private boolean aborted;

    public NetworkOutputChannel(IHyracksRootContext ctx, ChannelControlBlock ccb, int nBuffers) {
        this.ccb = ccb;
        emptyQueue = new ArrayDeque<ByteBuffer>(nBuffers);
        for (int i = 0; i < nBuffers; ++i) {
            emptyQueue.add(ctx.allocateFrame());
        }
        ccb.getWriteInterface().setEmptyBufferAcceptor(new WriteEmptyBufferAcceptor());
    }

    @Override
    public void open() throws HyracksDataException {
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        ByteBuffer destBuffer = null;
        synchronized (this) {
            while (true) {
                if (aborted) {
                    throw new HyracksDataException("Connection has been aborted");
                }
                destBuffer = emptyQueue.poll();
                if (destBuffer != null) {
                    break;
                }
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
        buffer.position(0);
        buffer.limit(destBuffer.capacity());
        destBuffer.clear();
        destBuffer.put(buffer);
        destBuffer.flip();
        ccb.getWriteInterface().getFullBufferAcceptor().accept(destBuffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        ccb.getWriteInterface().getFullBufferAcceptor().error(1);
    }

    @Override
    public void close() throws HyracksDataException {
        ccb.getWriteInterface().getFullBufferAcceptor().close();
    }

    void abort() {
        ccb.getWriteInterface().getFullBufferAcceptor().error(1);
        synchronized (NetworkOutputChannel.this) {
            aborted = true;
            NetworkOutputChannel.this.notifyAll();
        }
    }

    private class WriteEmptyBufferAcceptor implements IBufferAcceptor {
        @Override
        public void accept(ByteBuffer buffer) {
            synchronized (NetworkOutputChannel.this) {
                emptyQueue.add(buffer);
                NetworkOutputChannel.this.notifyAll();
            }
        }
    }
}