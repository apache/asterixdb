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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.channels.IInputChannelMonitor;
import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class NetworkInputChannel implements IInputChannel, INetworkChannel {
    private static final Logger LOGGER = Logger.getLogger(NetworkInputChannel.class.getName());

    private final ConnectionManager connectionManager;

    private final SocketAddress remoteAddress;

    private final PartitionId partitionId;

    private final Queue<ByteBuffer> emptyQueue;

    private final Queue<ByteBuffer> fullQueue;

    private SocketChannel socketChannel;

    private SelectionKey key;

    private ByteBuffer currentBuffer;

    private boolean eos;

    private boolean aborted;

    private IInputChannelMonitor monitor;

    private Object attachment;

    private ByteBuffer writeBuffer;

    public NetworkInputChannel(IHyracksRootContext ctx, ConnectionManager connectionManager,
            SocketAddress remoteAddress, PartitionId partitionId, int nBuffers) {
        this.connectionManager = connectionManager;
        this.remoteAddress = remoteAddress;
        this.partitionId = partitionId;
        this.emptyQueue = new ArrayDeque<ByteBuffer>(nBuffers);
        for (int i = 0; i < nBuffers; ++i) {
            emptyQueue.add(ctx.allocateFrame());
        }
        fullQueue = new ArrayDeque<ByteBuffer>(nBuffers);
        aborted = false;
        eos = false;
    }

    @Override
    public void registerMonitor(IInputChannelMonitor monitor) throws HyracksException {
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
    public synchronized void recycleBuffer(ByteBuffer buffer) {
        buffer.clear();
        emptyQueue.add(buffer);
        if (!eos && !aborted) {
            int ops = key.interestOps();
            if ((ops & SelectionKey.OP_READ) == 0) {
                key.interestOps(ops | SelectionKey.OP_READ);
                key.selector().wakeup();
                if (currentBuffer == null) {
                    currentBuffer = emptyQueue.poll();
                }
            }
        }
    }

    @Override
    public void open() throws HyracksDataException {
        currentBuffer = emptyQueue.poll();
        try {
            connectionManager.connect(this);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {

    }

    @Override
    public synchronized boolean dispatchNetworkEvent() throws IOException {
        if (aborted) {
            eos = true;
            monitor.notifyEndOfStream(this);
            return true;
        }
        if (key.isConnectable()) {
            if (socketChannel.finishConnect()) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
                prepareForWrite();
            }
        } else if (key.isWritable()) {
            socketChannel.write(writeBuffer);
            if (writeBuffer.remaining() == 0) {
                key.interestOps(SelectionKey.OP_READ);
            }
        } else if (key.isReadable()) {
            if (LOGGER.isLoggable(Level.FINER)) {
                LOGGER.finer("Before read: " + currentBuffer.position() + " " + currentBuffer.limit());
            }
            int bytesRead = socketChannel.read(currentBuffer);
            if (bytesRead < 0) {
                eos = true;
                monitor.notifyEndOfStream(this);
                return true;
            }
            if (LOGGER.isLoggable(Level.FINER)) {
                LOGGER.finer("After read: " + currentBuffer.position() + " " + currentBuffer.limit());
            }
            currentBuffer.flip();
            int dataLen = currentBuffer.remaining();
            if (dataLen >= currentBuffer.capacity() || aborted()) {
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.finest("NetworkInputChannel: frame received: sender = " + partitionId.getSenderIndex());
                }
                if (currentBuffer.getInt(FrameHelper.getTupleCountOffset(currentBuffer.capacity())) == 0) {
                    eos = true;
                    monitor.notifyEndOfStream(this);
                    return true;
                }
                fullQueue.add(currentBuffer);
                currentBuffer = emptyQueue.poll();
                if (currentBuffer == null && key.isValid()) {
                    int ops = key.interestOps();
                    key.interestOps(ops & ~SelectionKey.OP_READ);
                }
                monitor.notifyDataAvailability(this, 1);
                return false;
            }
            currentBuffer.compact();
        }
        return false;
    }

    private void prepareForConnect() {
        key.interestOps(SelectionKey.OP_CONNECT);
    }

    private void prepareForWrite() {
        writeBuffer = ByteBuffer.allocate(ConnectionManager.INITIAL_MESSAGE_SIZE);
        writeBuffer.putLong(partitionId.getJobId().getId());
        writeBuffer.putInt(partitionId.getConnectorDescriptorId().getId());
        writeBuffer.putInt(partitionId.getSenderIndex());
        writeBuffer.putInt(partitionId.getReceiverIndex());
        writeBuffer.flip();

        key.interestOps(SelectionKey.OP_WRITE);
    }

    @Override
    public void setSelectionKey(SelectionKey key) {
        this.key = key;
        socketChannel = (SocketChannel) key.channel();
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public SelectionKey getSelectionKey() {
        return key;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }

    public void abort() {
        aborted = true;
    }

    public boolean aborted() {
        return aborted;
    }

    @Override
    public void notifyConnectionManagerRegistration() throws IOException {
        if (socketChannel.connect(remoteAddress)) {
            prepareForWrite();
        } else {
            prepareForConnect();
        }
    }
}