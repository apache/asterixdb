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
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.net.exceptions.NetException;
import edu.uci.ics.hyracks.net.protocols.tcp.ITCPConnectionEventListener;
import edu.uci.ics.hyracks.net.protocols.tcp.TCPConnection;

public class MultiplexedConnection implements ITCPConnectionEventListener {
    private static final Logger LOGGER = Logger.getLogger(MultiplexedConnection.class.getName());

    private final MuxDemux muxDemux;

    private final IEventCounter pendingWriteEventsCounter;

    private final ChannelSet cSet;

    private final ReaderState readerState;

    private final WriterState writerState;

    private TCPConnection tcpConnection;

    private int lastChannelWritten;

    public MultiplexedConnection(MuxDemux muxDemux) {
        this.muxDemux = muxDemux;
        pendingWriteEventsCounter = new IEventCounter() {
            private int counter;

            @Override
            public synchronized void increment() {
                ++counter;
                if (counter == 1) {
                    tcpConnection.enable(SelectionKey.OP_WRITE);
                }
            }

            @Override
            public synchronized void decrement() {
                --counter;
                if (counter == 0) {
                    tcpConnection.disable(SelectionKey.OP_WRITE);
                }
                if (counter < 0) {
                    throw new IllegalStateException();
                }
            }
        };
        cSet = new ChannelSet(this, pendingWriteEventsCounter);
        readerState = new ReaderState();
        writerState = new WriterState();
        lastChannelWritten = -1;
    }

    synchronized void setTCPConnection(TCPConnection tcpConnection) {
        this.tcpConnection = tcpConnection;
        tcpConnection.enable(SelectionKey.OP_READ);
        notifyAll();
    }

    synchronized void waitUntilConnected() throws InterruptedException {
        while (tcpConnection == null) {
            wait();
        }
    }

    @Override
    public void notifyIOReady(TCPConnection connection, boolean readable, boolean writable) throws IOException,
            NetException {
        if (readable) {
            driveReaderStateMachine();
        }
        if (writable) {
            driveWriterStateMachine();
        }
    }

    public ChannelControlBlock openChannel() throws NetException, InterruptedException {
        ChannelControlBlock channel = cSet.allocateChannel();
        int channelId = channel.getChannelId();
        cSet.initiateChannelSyn(channelId);
        return channel;
    }

    class WriterState {
        private final ByteBuffer writeBuffer;

        final MuxDemuxCommand command;

        private ByteBuffer pendingBuffer;

        private int pendingWriteSize;

        private ChannelControlBlock ccb;

        public WriterState() {
            writeBuffer = ByteBuffer.allocate(MuxDemuxCommand.COMMAND_SIZE);
            writeBuffer.flip();
            command = new MuxDemuxCommand();
            ccb = null;
        }

        boolean writePending() {
            return writeBuffer.remaining() > 0 || (pendingBuffer != null && pendingWriteSize > 0);
        }

        void reset(ByteBuffer pendingBuffer, int pendingWriteSize, ChannelControlBlock ccb) {
            writeBuffer.clear();
            command.write(writeBuffer);
            writeBuffer.flip();
            this.pendingBuffer = pendingBuffer;
            this.pendingWriteSize = pendingWriteSize;
            this.ccb = ccb;
        }

        boolean performPendingWrite(SocketChannel sc) throws IOException {
            int len = writeBuffer.remaining();
            if (len > 0) {
                int written = sc.write(writeBuffer);
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
            if (ccb != null) {
                ccb.writeComplete();
                ccb = null;
            }
            return true;
        }
    }

    void driveWriterStateMachine() throws IOException, NetException {
        SocketChannel sc = tcpConnection.getSocketChannel();
        if (writerState.writePending()) {
            if (!writerState.performPendingWrite(sc)) {
                return;
            }
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
                    pendingWriteEventsCounter.decrement();
                    writerState.command.setChannelId(j);
                    writerState.command.setCommandType(MuxDemuxCommand.CommandType.OPEN_CHANNEL);
                    writerState.command.setData(0);
                    writerState.reset(null, 0, null);
                    if (!writerState.performPendingWrite(sc)) {
                        return;
                    }
                }
                BitSet pendingChannelCreditsBitmap = cSet.getPendingChannelCreditsBitmap();
                for (int j = pendingChannelCreditsBitmap.nextSetBit(0); j >= 0; j = pendingChannelCreditsBitmap
                        .nextSetBit(j)) {
                    pendingChannelCreditsBitmap.clear(j);
                    pendingWriteEventsCounter.decrement();
                    writerState.command.setChannelId(j);
                    writerState.command.setCommandType(MuxDemuxCommand.CommandType.ADD_CREDITS);
                    ChannelControlBlock ccb = cSet.getCCB(j);
                    int credits = ccb.getAndResetReadCredits();
                    writerState.command.setData(credits);
                    writerState.reset(null, 0, null);
                    if (!writerState.performPendingWrite(sc)) {
                        return;
                    }
                }
                BitSet pendingEOSAckBitmap = cSet.getPendingEOSAckBitmap();
                for (int j = pendingEOSAckBitmap.nextSetBit(0); j >= 0; j = pendingEOSAckBitmap.nextSetBit(j)) {
                    pendingEOSAckBitmap.clear(j);
                    pendingWriteEventsCounter.decrement();
                    writerState.command.setChannelId(j);
                    writerState.command.setCommandType(MuxDemuxCommand.CommandType.CLOSE_CHANNEL_ACK);
                    writerState.command.setData(0);
                    writerState.reset(null, 0, null);
                    if (!writerState.performPendingWrite(sc)) {
                        return;
                    }
                }
                BitSet pendingChannelWriteBitmap = cSet.getPendingChannelWriteBitmap();
                lastChannelWritten = pendingChannelWriteBitmap.nextSetBit(lastChannelWritten + 1);
                if (lastChannelWritten < 0) {
                    lastChannelWritten = pendingChannelWriteBitmap.nextSetBit(0);
                    if (lastChannelWritten < 0) {
                        return;
                    }
                }
                writeCCB = cSet.getCCB(lastChannelWritten);
            }
            writeCCB.write(writerState);
            if (writerState.writePending()) {
                if (!writerState.performPendingWrite(sc)) {
                    return;
                }
            }
        }
    }

    class ReaderState {
        private final ByteBuffer readBuffer;

        final MuxDemuxCommand command;

        private int pendingReadSize;

        private ChannelControlBlock ccb;

        ReaderState() {
            readBuffer = ByteBuffer.allocate(MuxDemuxCommand.COMMAND_SIZE);
            command = new MuxDemuxCommand();
        }

        void reset() {
            readBuffer.clear();
            pendingReadSize = 0;
            ccb = null;
        }
    }

    void driveReaderStateMachine() throws IOException, NetException {
        SocketChannel sc = tcpConnection.getSocketChannel();
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
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Received command: " + readerState.command);
            }
            ChannelControlBlock ccb = null;
            switch (readerState.command.getCommandType()) {
                case ADD_CREDITS: {
                    synchronized (MultiplexedConnection.this) {
                        ccb = cSet.getCCB(readerState.command.getChannelId());
                    }
                    ccb.addWriteCredits(readerState.command.getData());
                    break;
                }
                case CLOSE_CHANNEL: {
                    synchronized (MultiplexedConnection.this) {
                        ccb = cSet.getCCB(readerState.command.getChannelId());
                    }
                    ccb.reportRemoteEOS();
                    int channelId = ccb.getChannelId();
                    cSet.markEOSAck(channelId);
                    cSet.unmarkPendingCredits(channelId);
                    break;
                }
                case CLOSE_CHANNEL_ACK: {
                    synchronized (MultiplexedConnection.this) {
                        ccb = cSet.getCCB(readerState.command.getChannelId());
                    }
                    ccb.reportLocalEOSAck();
                    break;
                }
                case DATA: {
                    synchronized (MultiplexedConnection.this) {
                        ccb = cSet.getCCB(readerState.command.getChannelId());
                    }
                    readerState.pendingReadSize = readerState.command.getData();
                    readerState.ccb = ccb;
                    break;
                }
                case ERROR: {
                    synchronized (MultiplexedConnection.this) {
                        ccb = cSet.getCCB(readerState.command.getChannelId());
                    }
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
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Applied command: " + readerState.command + " on " + ccb);
            }
        }
        if (readerState.pendingReadSize > 0) {
            int newPendingReadSize = readerState.ccb.read(sc, readerState.pendingReadSize);
            muxDemux.getPerformanceCounters().addPayloadBytesRead(newPendingReadSize - readerState.pendingReadSize);
            readerState.pendingReadSize = newPendingReadSize;
            if (readerState.pendingReadSize > 0) {
                return;
            }
        }
        readerState.reset();
    }
}