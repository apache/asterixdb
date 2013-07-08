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
package edu.uci.ics.hyracks.control.nc.partitions;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.channels.IInputChannelMonitor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.partitions.IPartition;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class MaterializedPartitionInputChannel implements IInputChannel {
    private final int nBuffers;

    private final Queue<ByteBuffer> emptyQueue;

    private final Queue<ByteBuffer> fullQueue;

    private final PartitionId pid;

    private final PartitionManager manager;

    private final FrameWriter writer;

    private IInputChannelMonitor monitor;

    private Object attachment;

    public MaterializedPartitionInputChannel(int nBuffers, PartitionId pid, PartitionManager manager) {
        this.nBuffers = nBuffers;
        this.emptyQueue = new ArrayDeque<ByteBuffer>(nBuffers);
        fullQueue = new ArrayDeque<ByteBuffer>(nBuffers);
        this.pid = pid;
        this.manager = manager;
        writer = new FrameWriter();
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
    public ByteBuffer getNextBuffer() {
        return fullQueue.poll();
    }

    @Override
    public void recycleBuffer(ByteBuffer buffer) {
        buffer.clear();
        synchronized (this) {
            emptyQueue.add(buffer);
            notifyAll();
        }
    }

    @Override
    public void open(IHyracksCommonContext ctx) throws HyracksDataException {
        for (int i = 0; i < nBuffers; ++i) {
            emptyQueue.add(ctx.allocateFrame());
        }
        IPartition partition = manager.getPartition(pid);
        partition.writeTo(writer);
    }

    @Override
    public void close() throws HyracksDataException {

    }

    private class FrameWriter implements IFrameWriter {
        @Override
        public void open() throws HyracksDataException {

        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            synchronized (MaterializedPartitionInputChannel.this) {
                while (emptyQueue.isEmpty()) {
                    try {
                        MaterializedPartitionInputChannel.this.wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }
                ByteBuffer destFrame = emptyQueue.poll();
                buffer.position(0);
                buffer.limit(buffer.capacity());
                destFrame.clear();
                destFrame.put(buffer);
                fullQueue.add(destFrame);
                monitor.notifyDataAvailability(MaterializedPartitionInputChannel.this, 1);
            }
        }

        @Override
        public void fail() throws HyracksDataException {

        }

        @Override
        public void close() throws HyracksDataException {
            monitor.notifyEndOfStream(MaterializedPartitionInputChannel.this);
        }
    }
}