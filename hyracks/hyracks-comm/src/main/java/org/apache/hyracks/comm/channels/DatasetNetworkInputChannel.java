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
package edu.uci.ics.hyracks.comm.channels;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.channels.IInputChannelMonitor;
import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.net.buffers.IBufferAcceptor;
import edu.uci.ics.hyracks.net.buffers.ICloseableBufferAcceptor;
import edu.uci.ics.hyracks.net.protocols.muxdemux.ChannelControlBlock;

public class DatasetNetworkInputChannel implements IInputChannel {
    private static final Logger LOGGER = Logger.getLogger(DatasetNetworkInputChannel.class.getName());

    static final int INITIAL_MESSAGE_SIZE = 20;

    private final IChannelConnectionFactory netManager;

    private final SocketAddress remoteAddress;

    private final JobId jobId;

    private final ResultSetId resultSetId;

    private final int partition;

    private final Queue<ByteBuffer> fullQueue;

    private final int nBuffers;

    private ChannelControlBlock ccb;

    private IInputChannelMonitor monitor;

    private Object attachment;

    public DatasetNetworkInputChannel(IChannelConnectionFactory netManager, SocketAddress remoteAddress, JobId jobId,
            ResultSetId resultSetId, int partition, int nBuffers) {
        this.netManager = netManager;
        this.remoteAddress = remoteAddress;
        this.jobId = jobId;
        this.resultSetId = resultSetId;
        this.partition = partition;
        fullQueue = new ArrayDeque<ByteBuffer>(nBuffers);
        this.nBuffers = nBuffers;
    }

    @Override
    public void registerMonitor(IInputChannelMonitor monitor) {
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
    public void recycleBuffer(ByteBuffer buffer) {
        buffer.clear();
        ccb.getReadInterface().getEmptyBufferAcceptor().accept(buffer);
    }

    @Override
    public void open(IHyracksCommonContext ctx) throws HyracksDataException {
        try {
            ccb = netManager.connect(remoteAddress);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        ccb.getReadInterface().setFullBufferAcceptor(new ReadFullBufferAcceptor());
        ccb.getWriteInterface().setEmptyBufferAcceptor(new WriteEmptyBufferAcceptor());
        for (int i = 0; i < nBuffers; ++i) {
            ccb.getReadInterface().getEmptyBufferAcceptor().accept(ctx.allocateFrame());
        }
        ByteBuffer writeBuffer = ByteBuffer.allocate(INITIAL_MESSAGE_SIZE);
        writeBuffer.putLong(jobId.getId());
        writeBuffer.putLong(resultSetId.getId());
        writeBuffer.putInt(partition);
        writeBuffer.flip();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Sending partition request for JobId: " + jobId + " partition: " + partition + " on channel: "
                    + ccb);
        }
        ccb.getWriteInterface().getFullBufferAcceptor().accept(writeBuffer);
        ccb.getWriteInterface().getFullBufferAcceptor().close();
    }

    @Override
    public void close() throws HyracksDataException {

    }

    private class ReadFullBufferAcceptor implements ICloseableBufferAcceptor {
        @Override
        public void accept(ByteBuffer buffer) {
            fullQueue.add(buffer);
            monitor.notifyDataAvailability(DatasetNetworkInputChannel.this, 1);
        }

        @Override
        public void close() {
            monitor.notifyEndOfStream(DatasetNetworkInputChannel.this);
        }

        @Override
        public void error(int ecode) {
            monitor.notifyFailure(DatasetNetworkInputChannel.this);
        }
    }

    private class WriteEmptyBufferAcceptor implements IBufferAcceptor {
        @Override
        public void accept(ByteBuffer buffer) {
            // do nothing
        }
    }
}