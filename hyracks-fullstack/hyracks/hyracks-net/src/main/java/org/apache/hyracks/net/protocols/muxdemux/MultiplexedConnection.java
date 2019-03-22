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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.BitSet;
import java.util.Optional;

import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.comm.IConnectionWriterState;
import org.apache.hyracks.api.comm.MuxDemuxCommand;
import org.apache.hyracks.api.exceptions.NetException;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.net.protocols.tcp.ITCPConnectionEventListener;
import org.apache.hyracks.net.protocols.tcp.TCPConnection;
import org.apache.hyracks.util.JSONUtil;
import org.apache.hyracks.util.annotations.ThreadSafetyGuaranteedBy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A {@link MultiplexedConnection} can be used by clients to create multiple "channels"
 * that can have independent full-duplex conversations.
 *
 * @author vinayakb
 */
public class MultiplexedConnection implements ITCPConnectionEventListener {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MAX_CHUNKS_READ_PER_CYCLE = 4;

    private final MuxDemux muxDemux;

    private final IEventCounter pendingWriteEventsCounter;

    private final ChannelSet cSet;

    private final ReaderState readerState;

    private final WriterState writerState;

    private TCPConnection tcpConnection;

    private int lastChannelWritten;

    private int nConnectionAttempts;

    private boolean connectionFailure;

    private Exception error;

    MultiplexedConnection(MuxDemux muxDemux) {
        this.muxDemux = muxDemux;
        pendingWriteEventsCounter = new EventCounter();
        cSet = new ChannelSet(this, pendingWriteEventsCounter);
        readerState = new ReaderState();
        writerState = new WriterState();
        lastChannelWritten = -1;
        connectionFailure = false;
    }

    int getConnectionAttempts() {
        return nConnectionAttempts;
    }

    void setConnectionAttempts(int nConnectionAttempts) {
        this.nConnectionAttempts = nConnectionAttempts;
    }

    synchronized void setTCPConnection(TCPConnection tcpConnection) {
        this.tcpConnection = tcpConnection;
        tcpConnection.enable(SelectionKey.OP_READ);
        notifyAll();
    }

    synchronized void setConnectionFailure(Exception e) {
        this.connectionFailure = true;
        this.error = e;
        notifyAll();
    }

    synchronized void waitUntilConnected() throws InterruptedException, NetException {
        while (tcpConnection == null && !connectionFailure) {
            wait();
        }
        if (connectionFailure) {
            throw new NetException("Connection failure", error);
        }
    }

    @Override
    public void notifyIOReady(TCPConnection connection, boolean readable, boolean writable)
            throws IOException, NetException {
        if (readable) {
            driveReaderStateMachine();
        }
        if (writable) {
            driveWriterStateMachine();
        }
    }

    @Override
    public synchronized void notifyIOError(Exception e) {
        connectionFailure = true;
        error = e;
        cSet.notifyIOError();
    }

    /**
     * Open a channel to the other side.
     *
     * @return
     * @throws NetException
     *             - A network failure occurred.
     */
    public ChannelControlBlock openChannel() throws NetException {
        synchronized (this) {
            if (connectionFailure) {
                throw new NetException(error);
            }
        }
        ChannelControlBlock channel = cSet.allocateChannel();
        int channelId = channel.getChannelId();
        cSet.initiateChannelSyn(channelId);
        return channel;
    }

    class WriterState implements IConnectionWriterState {
        private final ByteBuffer cmdWriteBuffer;

        final MuxDemuxCommand command;

        private ByteBuffer pendingBuffer;

        private int pendingWriteSize;

        private IChannelControlBlock ccb;

        private boolean pendingWriteCompletion = false;

        public WriterState() {
            cmdWriteBuffer = ByteBuffer.allocateDirect(MuxDemuxCommand.COMMAND_SIZE);
            cmdWriteBuffer.flip();
            command = new MuxDemuxCommand();
            ccb = null;
        }

        boolean writePending() {
            return cmdWriteBuffer.remaining() > 0 || (pendingBuffer != null && pendingWriteSize > 0)
                    || pendingWriteCompletion;
        }

        @Override
        public void reset(ByteBuffer pendingBuffer, int pendingWriteSize, IChannelControlBlock ccb) {
            cmdWriteBuffer.clear();
            command.write(cmdWriteBuffer);
            cmdWriteBuffer.flip();
            this.pendingBuffer = pendingBuffer;
            this.pendingWriteSize = pendingWriteSize;
            this.ccb = ccb;
        }

        boolean performPendingWrite(ISocketChannel sc) throws IOException {
            if (pendingWriteCompletion && !sc.completeWrite()) {
                return false;
            }
            int len = cmdWriteBuffer.remaining();
            if (len > 0) {
                int written = sc.write(cmdWriteBuffer);
                muxDemux.getPerformanceCounters().addSignalingBytesWritten(written);
                if (written < len) {
                    return false;
                }
            }
            if (pendingBuffer != null) {
                if (pendingWriteSize > 0) {
                    assert pendingWriteSize <= pendingBuffer.remaining();
                    int oldLimit = pendingBuffer.limit();
                    try {
                        pendingBuffer.limit(pendingWriteSize + pendingBuffer.position());
                        int written = sc.write(pendingBuffer);
                        muxDemux.getPerformanceCounters().addPayloadBytesWritten(written);
                        pendingWriteSize -= written;
                    } finally {
                        pendingBuffer.limit(oldLimit);
                    }
                }
                if (pendingWriteSize > 0) {
                    return false;
                }
                pendingBuffer = null;
                pendingWriteSize = 0;
            }
            // must ensure all pending writes are performed before calling ccb.writeComplete()
            if (sc.isPendingWrite()) {
                pendingWriteCompletion = true;
                return false;
            }
            if (ccb != null) {
                ccb.writeComplete();
                ccb = null;
            }
            pendingWriteCompletion = false;
            return true;
        }

        @Override
        public MuxDemuxCommand getCommand() {
            return command;
        }
    }

    void driveWriterStateMachine() throws IOException, NetException {
        ISocketChannel sc = tcpConnection.getSocketChannel();
        if (writerState.writePending()) {
            if (!writerState.performPendingWrite(sc)) {
                return;
            }
            pendingWriteEventsCounter.decrement();
        }
        int numCycles;

        synchronized (MultiplexedConnection.this) {
            numCycles = cSet.getOpenChannelCount();
        }

        for (int i = 0; i < numCycles; ++i) {
            ChannelControlBlock writeCCB = null;
            synchronized (MultiplexedConnection.this) {
                BitSet pendingChannelSynBitmap = cSet.getPendingChannelSynBitmap();
                for (int j = pendingChannelSynBitmap.nextSetBit(0); j >= 0; j = pendingChannelSynBitmap.nextSetBit(j)) {
                    pendingChannelSynBitmap.clear(j);
                    writerState.command.setChannelId(j);
                    writerState.command.setCommandType(MuxDemuxCommand.CommandType.OPEN_CHANNEL);
                    writerState.command.setData(0);
                    writerState.reset(null, 0, null);
                    if (!writerState.performPendingWrite(sc)) {
                        return;
                    }
                    pendingWriteEventsCounter.decrement();
                }
                BitSet pendingChannelCreditsBitmap = cSet.getPendingChannelCreditsBitmap();
                for (int j = pendingChannelCreditsBitmap.nextSetBit(0); j >= 0; j =
                        pendingChannelCreditsBitmap.nextSetBit(j)) {
                    writerState.command.setChannelId(j);
                    writerState.command.setCommandType(MuxDemuxCommand.CommandType.ADD_CREDITS);
                    ChannelControlBlock ccb = cSet.getCCB(j);
                    int credits = ccb.getReadCredits();
                    int effectiveCredits;
                    if (credits <= MuxDemuxCommand.MAX_DATA_VALUE) {
                        effectiveCredits = credits;
                        ccb.setReadCredits(0);
                        pendingChannelCreditsBitmap.clear(j);
                    } else {
                        effectiveCredits = MuxDemuxCommand.MAX_DATA_VALUE;
                        ccb.setReadCredits(credits - effectiveCredits);
                    }
                    writerState.command.setData(effectiveCredits);
                    writerState.reset(null, 0, null);
                    if (!writerState.performPendingWrite(sc)) {
                        return;
                    }
                    if (credits == effectiveCredits) {
                        pendingWriteEventsCounter.decrement();
                    }
                }
                BitSet pendingEOSAckBitmap = cSet.getPendingEOSAckBitmap();
                for (int j = pendingEOSAckBitmap.nextSetBit(0); j >= 0; j = pendingEOSAckBitmap.nextSetBit(j)) {
                    pendingEOSAckBitmap.clear(j);
                    ChannelControlBlock ccb = cSet.getCCB(j);
                    ccb.reportRemoteEOSAck();
                    writerState.command.setChannelId(j);
                    writerState.command.setCommandType(MuxDemuxCommand.CommandType.CLOSE_CHANNEL_ACK);
                    writerState.command.setData(0);
                    writerState.reset(null, 0, null);
                    if (!writerState.performPendingWrite(sc)) {
                        return;
                    }
                    pendingWriteEventsCounter.decrement();
                }
                BitSet pendingChannelWriteBitmap = cSet.getPendingChannelWriteBitmap();
                lastChannelWritten = pendingChannelWriteBitmap.nextSetBit(lastChannelWritten + 1);
                if (lastChannelWritten == -1) {
                    lastChannelWritten = pendingChannelWriteBitmap.nextSetBit(0);
                    if (lastChannelWritten == -1) {
                        return;
                    }
                }
                writeCCB = cSet.getCCB(lastChannelWritten);
            }
            writeCCB.write(writerState);
            if (writerState.writePending()) {
                pendingWriteEventsCounter.increment();
                if (!writerState.performPendingWrite(sc)) {
                    return;
                }
                pendingWriteEventsCounter.decrement();
            }
        }
    }

    class ReaderState {
        private final ByteBuffer readBuffer;

        final MuxDemuxCommand command;

        private int pendingReadSize;

        private ChannelControlBlock ccb;

        ReaderState() {
            readBuffer = ByteBuffer.allocateDirect(MuxDemuxCommand.COMMAND_SIZE);
            command = new MuxDemuxCommand();
        }

        void reset() {
            readBuffer.clear();
            pendingReadSize = 0;
            ccb = null;
        }

        private ChannelControlBlock getCCBInCommand() {
            synchronized (MultiplexedConnection.this) {
                return cSet.getCCB(command.getChannelId());
            }
        }
    }

    void driveReaderStateMachine() throws IOException, NetException {
        ISocketChannel sc = tcpConnection.getSocketChannel();
        int chunksRead = 0;
        while (chunksRead < MAX_CHUNKS_READ_PER_CYCLE || sc.isPendingRead()) {
            if (readerState.readBuffer.remaining() > 0) {
                int read = sc.read(readerState.readBuffer);
                if (read < 0) {
                    throw new NetException("Socket Closed");
                }
                muxDemux.getPerformanceCounters().addSignalingBytesRead(read);
                if (readerState.readBuffer.remaining() > 0) {
                    return;
                }
                readerState.readBuffer.flip();
                readerState.command.read(readerState.readBuffer);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Received command: " + readerState.command);
                }
                ChannelControlBlock ccb = null;
                switch (readerState.command.getCommandType()) {
                    case ADD_CREDITS: {
                        ccb = readerState.getCCBInCommand();
                        ccb.addWriteCredits(readerState.command.getData());
                        break;
                    }
                    case CLOSE_CHANNEL: {
                        ccb = readerState.getCCBInCommand();
                        ccb.reportRemoteEOS();
                        int channelId = ccb.getChannelId();
                        cSet.markEOSAck(channelId);
                        cSet.unmarkPendingCredits(channelId);
                        break;
                    }
                    case CLOSE_CHANNEL_ACK: {
                        ccb = readerState.getCCBInCommand();
                        ccb.reportLocalEOSAck();
                        break;
                    }
                    case DATA: {
                        ccb = readerState.getCCBInCommand();
                        readerState.pendingReadSize = readerState.command.getData();
                        readerState.ccb = ccb;
                        break;
                    }
                    case ERROR: {
                        ccb = readerState.getCCBInCommand();
                        ccb.reportRemoteError(readerState.command.getData());
                        int channelId = ccb.getChannelId();
                        cSet.markEOSAck(channelId);
                        cSet.unmarkPendingCredits(channelId);
                        break;
                    }
                    case OPEN_CHANNEL: {
                        int channelId = readerState.command.getChannelId();
                        ccb = cSet.registerChannel(channelId);
                        muxDemux.getChannelOpenListener().channelOpened(ccb);
                    }
                }
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Applied command: " + readerState.command + " on " + ccb);
                }
            }
            if (readerState.pendingReadSize > 0) {
                ++chunksRead;
                int newPendingReadSize = readerState.ccb.read(sc, readerState.pendingReadSize);
                muxDemux.getPerformanceCounters().addPayloadBytesRead(readerState.pendingReadSize - newPendingReadSize);
                readerState.pendingReadSize = newPendingReadSize;
                if (readerState.pendingReadSize > 0) {
                    return;
                }
            }
            readerState.reset();
        }
    }

    public IChannelInterfaceFactory getChannelInterfaceFactory() {
        return muxDemux.getChannelInterfaceFactory();
    }

    @ThreadSafetyGuaranteedBy("MultiplexedConnection.this")
    private class EventCounter implements IEventCounter {
        private int counter;

        @Override
        public synchronized void increment() {
            if (!connectionFailure) {
                ++counter;
                if (counter == 1) {
                    tcpConnection.enable(SelectionKey.OP_WRITE);
                }
            }
        }

        @Override
        public synchronized void decrement() {
            if (!connectionFailure) {
                --counter;
                if (counter == 0) {
                    tcpConnection.disable(SelectionKey.OP_WRITE);
                }
                if (counter < 0) {
                    throw new IllegalStateException();
                }
            }
        }
    }

    public InetSocketAddress getRemoteAddress() {
        return tcpConnection == null ? null : tcpConnection.getRemoteAddress();
    }

    public synchronized Optional<JsonNode> getState() {
        if (tcpConnection == null) {
            return Optional.empty();
        }
        final ObjectNode state = JSONUtil.createObject();
        state.put("remoteAddress", getRemoteAddress().toString());
        final ArrayNode channels = cSet.getState();
        state.set("channels", channels);
        return Optional.of(state);
    }
}
