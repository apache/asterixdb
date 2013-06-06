/*
 * Copyright 2009-2013 by The Regents of the University of California
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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.net.buffers.IBufferAcceptor;
import edu.uci.ics.hyracks.net.buffers.ICloseableBufferAcceptor;
import edu.uci.ics.hyracks.net.exceptions.NetException;

/**
 * Handle to a channel that represents a logical full-duplex communication end-point.
 * 
 * @author vinayakb
 */
public class ChannelControlBlock {
    private static final Logger LOGGER = Logger.getLogger(ChannelControlBlock.class.getName());

    private final ChannelSet cSet;

    private final int channelId;

    private final ReadInterface ri;

    private final WriteInterface wi;

    private final AtomicBoolean localClose;

    private final AtomicBoolean localCloseAck;

    private final AtomicBoolean remoteClose;

    private final AtomicBoolean remoteCloseAck;

    ChannelControlBlock(ChannelSet cSet, int channelId) {
        this.cSet = cSet;
        this.channelId = channelId;
        this.ri = new ReadInterface();
        this.wi = new WriteInterface();
        localClose = new AtomicBoolean();
        localCloseAck = new AtomicBoolean();
        remoteClose = new AtomicBoolean();
        remoteCloseAck = new AtomicBoolean();
    }

    int getChannelId() {
        return channelId;
    }

    /**
     * Get the read inderface of this channel.
     * 
     * @return the read interface.
     */
    public IChannelReadInterface getReadInterface() {
        return ri;
    }

    /**
     * Get the write interface of this channel.
     * 
     * @return the write interface.
     */
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
                if (currentReadBuffer == null) {
                    currentReadBuffer = riEmptyStack.poll();
                    assert currentReadBuffer != null;
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

        void flush() {
            if (currentReadBuffer != null) {
                currentReadBuffer.flip();
                fba.accept(currentReadBuffer);
                currentReadBuffer = null;
            }
        }
    }

    private final class WriteInterface implements IChannelWriteInterface {
        private final Queue<ByteBuffer> wiFullQueue;

        private boolean channelWritabilityState;

        private final ICloseableBufferAcceptor fba = new ICloseableBufferAcceptor() {
            @Override
            public void accept(ByteBuffer buffer) {
                synchronized (ChannelControlBlock.this) {
                    wiFullQueue.add(buffer);
                    adjustChannelWritability();
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
                    adjustChannelWritability();
                }
            }

            @Override
            public void error(int ecode) {
                synchronized (ChannelControlBlock.this) {
                    WriteInterface.this.ecode = ecode;
                    adjustChannelWritability();
                }
            }
        };

        private IBufferAcceptor eba;

        private int credits;

        private boolean eos;

        private boolean eosSent;

        private int ecode;

        private boolean ecodeSent;

        private ByteBuffer currentWriteBuffer;

        WriteInterface() {
            wiFullQueue = new ArrayDeque<ByteBuffer>();
            credits = 0;
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
                int size = Math.min(currentWriteBuffer.remaining(), credits);
                if (size > 0) {
                    credits -= size;
                    writerState.command.setChannelId(channelId);
                    writerState.command.setCommandType(MuxDemuxCommand.CommandType.DATA);
                    writerState.command.setData(size);
                    writerState.reset(currentWriteBuffer, size, ChannelControlBlock.this);
                } else {
                    adjustChannelWritability();
                }
            } else if (ecode >= 0 && !ecodeSent) {
                writerState.command.setChannelId(channelId);
                writerState.command.setCommandType(MuxDemuxCommand.CommandType.ERROR);
                writerState.command.setData(ecode);
                writerState.reset(null, 0, null);
                ecodeSent = true;
                localClose.set(true);
                adjustChannelWritability();
            } else if (eos && !eosSent) {
                writerState.command.setChannelId(channelId);
                writerState.command.setCommandType(MuxDemuxCommand.CommandType.CLOSE_CHANNEL);
                writerState.command.setData(0);
                writerState.reset(null, 0, null);
                eosSent = true;
                localClose.set(true);
                adjustChannelWritability();
            }
        }

        void writeComplete() {
            if (currentWriteBuffer.remaining() <= 0) {
                currentWriteBuffer.clear();
                eba.accept(currentWriteBuffer);
                currentWriteBuffer = null;
                adjustChannelWritability();
            }
        }

        private boolean computeWritability() {
            boolean writableDataPresent = currentWriteBuffer != null || !wiFullQueue.isEmpty();
            if (writableDataPresent) {
                return credits > 0;
            }
            if (eos && !eosSent) {
                return true;
            }
            if (ecode >= 0 && !ecodeSent) {
                return true;
            }
            return false;
        }

        void adjustChannelWritability() {
            boolean writable = computeWritability();
            if (writable) {
                if (!channelWritabilityState) {
                    cSet.markPendingWrite(channelId);
                }
            } else {
                if (channelWritabilityState) {
                    cSet.unmarkPendingWrite(channelId);
                }
            }
            channelWritabilityState = writable;
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

    synchronized void addWriteCredits(int delta) {
        wi.credits += delta;
        wi.adjustChannelWritability();
    }

    synchronized void reportRemoteEOS() {
        ri.flush();
        ri.fba.close();
        remoteClose.set(true);
    }

    void reportRemoteEOSAck() {
        remoteCloseAck.set(true);
    }

    boolean getRemoteEOS() {
        return remoteClose.get();
    }

    void reportLocalEOSAck() {
        localCloseAck.set(true);
    }

    synchronized void reportRemoteError(int ecode) {
        ri.flush();
        ri.fba.error(ecode);
        remoteClose.set(true);
    }

    boolean completelyClosed() {
        return localCloseAck.get() && remoteCloseAck.get();
    }

    @Override
    public String toString() {
        return "Channel:" + channelId + "[localClose: " + localClose + " localCloseAck: " + localCloseAck
                + " remoteClose: " + remoteClose + " remoteCloseAck:" + remoteCloseAck + " readCredits: " + ri.credits
                + " writeCredits: " + wi.credits + "]";
    }
}