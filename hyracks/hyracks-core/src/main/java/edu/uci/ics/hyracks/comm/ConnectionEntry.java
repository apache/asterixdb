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
package edu.uci.ics.hyracks.comm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListener;
import edu.uci.ics.hyracks.context.HyracksContext;

public class ConnectionEntry implements IConnectionEntry {
    private static final Logger LOGGER = Logger.getLogger(ConnectionEntry.class.getName());

    private SocketChannel socketChannel;

    private final ByteBuffer readBuffer;

    private final ByteBuffer writeBuffer;

    private IDataReceiveListener recvListener;

    private Object attachment;

    private final SelectionKey key;

    private UUID jobId;

    private UUID stageId;

    private boolean aborted;

    public ConnectionEntry(HyracksContext ctx, SocketChannel socketChannel, SelectionKey key) {
        this.socketChannel = socketChannel;
        readBuffer = ctx.getResourceManager().allocateFrame();
        readBuffer.clear();
        writeBuffer = ctx.getResourceManager().allocateFrame();
        writeBuffer.clear();
        this.key = key;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public boolean dispatch(SelectionKey key) throws IOException {
        if (aborted) {
            recvListener.dataReceived(this);
        } else {
            if (key.isReadable()) {
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.finer("Before read: " + readBuffer.position() + " " + readBuffer.limit());
                }
                int bytesRead = socketChannel.read(readBuffer);
                if (bytesRead < 0) {
                    recvListener.eos(this);
                    return true;
                }
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.finer("After read: " + readBuffer.position() + " " + readBuffer.limit());
                }
                recvListener.dataReceived(this);
            } else if (key.isWritable()) {
                synchronized (this) {
                    writeBuffer.flip();
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.finer("Before write: " + writeBuffer.position() + " " + writeBuffer.limit());
                    }
                    int bytesWritten = socketChannel.write(writeBuffer);
                    if (bytesWritten < 0) {
                        return true;
                    }
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.finer("After write: " + writeBuffer.position() + " " + writeBuffer.limit());
                    }
                    if (writeBuffer.remaining() <= 0) {
                        int ops = key.interestOps();
                        key.interestOps(ops & ~SelectionKey.OP_WRITE);
                    }
                    writeBuffer.compact();
                    notifyAll();
                }
            } else {
                LOGGER.warning("Spurious event triggered: " + key.readyOps());
                return true;
            }
        }
        return false;
    }

    @Override
    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    @Override
    public synchronized void write(ByteBuffer buffer) {
        while (buffer.remaining() > 0) {
            while (writeBuffer.remaining() <= 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                }
            }
            int oldLimit = buffer.limit();
            buffer.limit(Math.min(oldLimit, writeBuffer.remaining()));
            writeBuffer.put(buffer);
            buffer.limit(oldLimit);
            int ops = key.interestOps();
            key.interestOps(ops | SelectionKey.OP_WRITE);
            key.selector().wakeup();
        }
    }

    @Override
    public void setDataReceiveListener(IDataReceiveListener listener) {
        this.recvListener = listener;
    }

    @Override
    public void attach(Object attachment) {
        this.attachment = attachment;
    }

    @Override
    public Object getAttachment() {
        return attachment;
    }

    @Override
    public void close() {
        try {
            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public SelectionKey getSelectionKey() {
        return key;
    }

    @Override
    public UUID getJobId() {
        return jobId;
    }

    @Override
    public void setJobId(UUID jobId) {
        this.jobId = jobId;
    }

    @Override
    public UUID getStageId() {
        return stageId;
    }

    @Override
    public void setStageId(UUID stageId) {
        this.stageId = stageId;
    }

    @Override
    public void abort() {
        aborted = true;
    }

    @Override
    public boolean aborted() {
        return aborted;
    }
}