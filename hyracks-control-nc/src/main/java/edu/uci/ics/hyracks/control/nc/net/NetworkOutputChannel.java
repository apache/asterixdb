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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class NetworkOutputChannel implements INetworkChannel, IFrameWriter {
    private final IHyracksRootContext ctx;

    private final Queue<ByteBuffer> emptyQueue;

    private final Queue<ByteBuffer> fullQueue;

    private SelectionKey key;

    private boolean aborted;

    private boolean eos;

    private boolean eosSent;

    private ByteBuffer currentBuffer;

    public NetworkOutputChannel(IHyracksRootContext ctx, int nBuffers) {
        this.ctx = ctx;
        emptyQueue = new ArrayDeque<ByteBuffer>(nBuffers);
        for (int i = 0; i < nBuffers; ++i) {
            emptyQueue.add(ctx.allocateFrame());
        }
        fullQueue = new ArrayDeque<ByteBuffer>(nBuffers);
    }

    @Override
    public synchronized boolean dispatchNetworkEvent() throws IOException {
        if (aborted) {
            eos = true;
            return true;
        } else if (key.isWritable()) {
            while (true) {
                if (currentBuffer == null) {
                    if (eosSent) {
                        return true;
                    }
                    currentBuffer = fullQueue.poll();
                    if (currentBuffer == null) {
                        if (eos) {
                            currentBuffer = emptyQueue.poll();
                            currentBuffer.clear();
                            currentBuffer.putInt(FrameHelper.getTupleCountOffset(ctx.getFrameSize()), 0);
                            eosSent = true;
                        } else {
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                            return false;
                        }
                    }
                }
                int bytesWritten = ((SocketChannel) key.channel()).write(currentBuffer);
                if (bytesWritten < 0) {
                    eos = true;
                    return true;
                }
                if (currentBuffer.remaining() == 0) {
                    emptyQueue.add(currentBuffer);
                    notifyAll();
                    currentBuffer = null;
                    if (eosSent) {
                        return true;
                    }
                } else {
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public void setSelectionKey(SelectionKey key) {
        this.key = key;
    }

    @Override
    public SelectionKey getSelectionKey() {
        return key;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return ((SocketChannel) key.channel()).socket().getRemoteSocketAddress();
    }

    @Override
    public synchronized void abort() {
        aborted = true;
    }

    @Override
    public void open() throws HyracksDataException {
        currentBuffer = null;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        ByteBuffer destBuffer = null;
        synchronized (this) {
            if (aborted) {
                throw new HyracksDataException("Connection has been aborted");
            }
            while (true) {
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
        synchronized (this) {
            fullQueue.add(destBuffer);
        }
        key.interestOps(SelectionKey.OP_WRITE);
        key.selector().wakeup();
    }

    @Override
    public void flush() throws HyracksDataException {

    }

    @Override
    public synchronized void close() throws HyracksDataException {
        eos = true;
        key.interestOps(SelectionKey.OP_WRITE);
        key.selector().wakeup();        
    }

    @Override
    public void notifyConnectionManagerRegistration() throws IOException {
    }
}