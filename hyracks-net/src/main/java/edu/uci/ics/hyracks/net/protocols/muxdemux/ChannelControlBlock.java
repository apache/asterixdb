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
package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.net.buffers.IBufferAcceptor;
import edu.uci.ics.hyracks.net.buffers.ICloseableBufferAcceptor;
import edu.uci.ics.hyracks.net.exceptions.NetException;

public class ChannelControlBlock {
    private static final Logger LOGGER = Logger.getLogger(ChannelControlBlock.class.getName());

    private final ChannelSet cSet;

    private final int channelId;

    private final ReadInterface ri;

    private final WriteInterface wi;

    private final AtomicBoolean localClose;

    private final AtomicBoolean localCloseAck;

    private final AtomicBoolean remoteClose;

    ChannelControlBlock(ChannelSet cSet, int channelId) {
        this.cSet = cSet;
        this.channelId = channelId;
        this.ri = new ReadInterface();
        this.wi = new WriteInterface();
        localClose = new AtomicBoolean();
        localCloseAck = new AtomicBoolean();
        remoteClose = new AtomicBoolean();
    }

    int getChannelId() {
        return channelId;
    }

    public IChannelReadInterface getReadInterface() {
        return ri;
    }

    public IChannelWriteInterface getWriteInterface() {
        return wi;
    }

    private final class ReadInterface implements IChannelReadInterface {
        private final Deque<ByteBuffer> riEmptyStack;

        private final IBufferAcceptor eba = new IBufferAcceptor() {
            @Override
            public void accept(ByteBuffer buffer) {
                int delta = buffer.remaining();
                synchronized (ChannelControlBlock.this) {
                    if (remoteClose.get()) {
                        return;
                    }
                    riEmptyStack.push(buffer);
                }
                cSet.addPendingCredits(channelId, delta);
            }
        };

        private ICloseableBufferAcceptor fba;

        private volatile int credits;

        private ByteBuffer currentReadBuffer;

        ReadInterface() {
            riEmptyStack = new ArrayDeque<ByteBuffer>();
            credits = 0;
        }

        @Override
        public void setFullBufferAcceptor(ICloseableBufferAcceptor fullBufferAcceptor) {
            fba = fullBufferAcceptor;
        }

        @Override
        public IBufferAcceptor getEmptyBufferAcceptor() {
            return eba;
        }

        int read(SocketChannel sc, int size) throws IOException, NetException {
            while (true) {
                if (size <= 0) {
                    return size;
                }
                if (ri.currentReadBuffer == null) {
                    ri.currentReadBuffer = ri.riEmptyStack.poll();
                    assert ri.currentReadBuffer != null;
                }
                int rSize = Math.min(size, ri.currentReadBuffer.remaining());
                if (rSize > 0) {
                    ri.currentReadBuffer.limit(ri.currentReadBuffer.position() + rSize);
                    int len;
                    try {
                        len = sc.read(ri.currentReadBuffer);
                        if (len < 0) {
                            throw new NetException("Socket Closed");
                        }
                    } finally {
                        ri.currentReadBuffer.limit(ri.currentReadBuffer.capacity());
                    }
                    size -= len;
                    if (len < rSize) {
                        return size;
                    }
                } else {
                    return size;
                }
                if (ri.currentReadBuffer.remaining() <= 0) {
                    flush();
                }
            }
        }

        void flush() {
            if (currentReadBuffer != null) {
                currentReadBuffer.flip();
                fba.accept(ri.currentReadBuffer);
                currentReadBuffer = null;
            }
        }
    }

    private final class WriteInterface implements IChannelWriteInterface {
        private final Queue<ByteBuffer> wiFullQueue;

        private int channelWriteEventCount;

        private final ICloseableBufferAcceptor fba = new ICloseableBufferAcceptor() {
            @Override
            public void accept(ByteBuffer buffer) {
                synchronized (ChannelControlBlock.this) {
                    wiFullQueue.add(buffer);
                    incrementLocalWriteEventCount();
                }
            }

            @Override
            public void close() {
                synchronized (ChannelControlBlock.this) {
                    if (eos) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Received duplicate close() on channel: " + channelId);
                        }
                        return;
                    }
                    eos = true;
                    incrementLocalWriteEventCount();
                }
            }

            @Override
            public void error(int ecode) {
                synchronized (ChannelControlBlock.this) {
                    WriteInterface.this.ecode = ecode;
                    incrementLocalWriteEventCount();
                }
            }
        };

        private IBufferAcceptor eba;

        private final AtomicInteger credits;

        private boolean eos;

        private boolean eosSent;

        private int ecode;

        private boolean ecodeSent;

        private ByteBuffer currentWriteBuffer;

        WriteInterface() {
            wiFullQueue = new ArrayDeque<ByteBuffer>();
            credits = new AtomicInteger();
            eos = false;
            eosSent = false;
            ecode = -1;
            ecodeSent = false;
        }

        @Override
        public void setEmptyBufferAcceptor(IBufferAcceptor emptyBufferAcceptor) {
            eba = emptyBufferAcceptor;
        }

        @Override
        public ICloseableBufferAcceptor getFullBufferAcceptor() {
            return fba;
        }

        void write(MultiplexedConnection.WriterState writerState) throws NetException {
            if (currentWriteBuffer == null) {
                currentWriteBuffer = wiFullQueue.poll();
            }
            if (currentWriteBuffer != null) {
                int size = Math.min(currentWriteBuffer.remaining(), credits.get());
                if (size > 0) {
                    credits.addAndGet(-size);
                    writerState.command.setChannelId(channelId);
                    writerState.command.setCommandType(MuxDemuxCommand.CommandType.DATA);
                    writerState.command.setData(size);
                    writerState.reset(currentWriteBuffer, size, ChannelControlBlock.this);
                }
            } else if (ecode >= 0 && !ecodeSent) {
                decrementLocalWriteEventCount();
                writerState.command.setChannelId(channelId);
                writerState.command.setCommandType(MuxDemuxCommand.CommandType.ERROR);
                writerState.command.setData(ecode);
                writerState.reset(null, 0, null);
                ecodeSent = true;
                localClose.set(true);
            } else if (wi.eos && !wi.eosSent) {
                decrementLocalWriteEventCount();
                writerState.command.setChannelId(channelId);
                writerState.command.setCommandType(MuxDemuxCommand.CommandType.CLOSE_CHANNEL);
                writerState.command.setData(0);
                writerState.reset(null, 0, null);
                eosSent = true;
                localClose.set(true);
            }
        }

        void writeComplete() {
            if (currentWriteBuffer.remaining() <= 0) {
                currentWriteBuffer.clear();
                eba.accept(currentWriteBuffer);
                decrementLocalWriteEventCount();
                currentWriteBuffer = null;
            }
        }

        void incrementLocalWriteEventCount() {
            ++channelWriteEventCount;
            if (channelWriteEventCount == 1) {
                cSet.markPendingWrite(channelId);
            }
        }

        void decrementLocalWriteEventCount() {
            --channelWriteEventCount;
            if (channelWriteEventCount == 0) {
                cSet.unmarkPendingWrite(channelId);
            }
        }
    }

    synchronized void write(MultiplexedConnection.WriterState writerState) throws NetException {
        wi.write(writerState);
    }

    synchronized void writeComplete() {
        wi.writeComplete();
    }

    synchronized int read(SocketChannel sc, int size) throws IOException, NetException {
        return ri.read(sc, size);
    }

    int getReadCredits() {
        return ri.credits;
    }

    void setReadCredits(int credits) {
        this.ri.credits = credits;
    }

    void addWriteCredits(int delta) {
        wi.credits.addAndGet(delta);
    }

    synchronized void reportRemoteEOS() {
        ri.flush();
        ri.fba.close();
        remoteClose.set(true);
    }

    boolean getRemoteEOS() {
        return remoteClose.get();
    }

    synchronized void reportLocalEOSAck() {
        localCloseAck.set(true);
    }

    synchronized void reportRemoteError(int ecode) {
        ri.flush();
        ri.fba.error(ecode);
        remoteClose.set(true);
    }

    boolean completelyClosed() {
        return localCloseAck.get() && remoteClose.get();
    }

    @Override
    public String toString() {
        return "Channel:" + channelId + "[localClose: " + localClose + " localCloseAck: " + localCloseAck
                + " remoteClose: " + remoteClose + " readCredits: " + ri.credits + " writeCredits: " + wi.credits + "]";
    }
}