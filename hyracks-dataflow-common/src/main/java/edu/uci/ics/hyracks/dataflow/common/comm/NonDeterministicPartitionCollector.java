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
package edu.uci.ics.hyracks.dataflow.common.comm;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.channels.IInputChannelMonitor;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.PartitionChannel;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class NonDeterministicPartitionCollector extends AbstractPartitionCollector {
    private final FrameReader reader;

    private final BitSet expectedPartitions;

    private IInputChannel[] channels;

    private BitSet frameAvailability;

    private int[] availableFrameCounts;

    private BitSet eosSenders;

    private BitSet closedSenders;

    private int lastReadSender;

    public NonDeterministicPartitionCollector(IHyracksTaskContext ctx, ConnectorDescriptorId connectorId,
            int receiverIndex, BitSet expectedPartitions) {
        super(ctx, connectorId, receiverIndex);
        this.expectedPartitions = expectedPartitions;
        int nSenders = expectedPartitions.size();
        reader = new FrameReader();
        channels = new IInputChannel[nSenders];
        eosSenders = new BitSet(nSenders);
        closedSenders = new BitSet(nSenders);
        closedSenders.or(expectedPartitions);
        closedSenders.flip(0, nSenders);
        frameAvailability = new BitSet(nSenders);
        availableFrameCounts = new int[nSenders];
    }

    @Override
    public void open() throws HyracksException {
        lastReadSender = 0;
    }

    @Override
    public synchronized void addPartitions(Collection<PartitionChannel> partitions) throws HyracksException {
        for (PartitionChannel pc : partitions) {
            PartitionId pid = pc.getPartitionId();
            IInputChannel channel = pc.getInputChannel();
            channel.setAttachment(pid);
            channel.registerMonitor(reader);
            channels[pid.getSenderIndex()] = channel;
            channel.open();
        }
    }

    @Override
    public IFrameReader getReader() throws HyracksException {
        return reader;
    }

    @Override
    public void close() throws HyracksException {
    }

    private final class FrameReader implements IFrameReader, IInputChannelMonitor {
        @Override
        public void open() throws HyracksDataException {
        }

        @Override
        public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
            synchronized (NonDeterministicPartitionCollector.this) {
                while (true) {
                    switch (lastReadSender) {
                        default:
                            lastReadSender = frameAvailability.nextSetBit(lastReadSender);
                            if (lastReadSender >= 0) {
                                break;
                            }
                        case 0:
                            lastReadSender = frameAvailability.nextSetBit(0);
                    }
                    if (lastReadSender >= 0) {
                        assert availableFrameCounts[lastReadSender] > 0;
                        if (--availableFrameCounts[lastReadSender] == 0) {
                            frameAvailability.clear(lastReadSender);
                        }
                        ByteBuffer srcFrame = channels[lastReadSender].getNextBuffer();
                        FrameUtils.copy(srcFrame, buffer);
                        channels[lastReadSender].recycleBuffer(srcFrame);
                        return true;
                    }
                    for (int i = eosSenders.nextSetBit(0); i >= 0; i = eosSenders.nextSetBit(i)) {
                        channels[i].close();
                        eosSenders.clear(i);
                        closedSenders.set(i);
                    }
                    if (closedSenders.nextClearBit(0) < 0) {
                        return false;
                    }
                    try {
                        NonDeterministicPartitionCollector.this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void close() throws HyracksDataException {
            synchronized (NonDeterministicPartitionCollector.this) {
                for (int i = closedSenders.nextClearBit(0); i >= 0; i = closedSenders.nextClearBit(i)) {
                    if (channels[i] != null) {
                        channels[i].close();
                    }
                }
            }
        }

        @Override
        public void notifyDataAvailability(IInputChannel channel, int nFrames) {
            synchronized (NonDeterministicPartitionCollector.this) {
                PartitionId pid = (PartitionId) channel.getAttachment();
                int senderIndex = pid.getSenderIndex();
                availableFrameCounts[senderIndex] += nFrames;
                frameAvailability.set(senderIndex);
                NonDeterministicPartitionCollector.this.notifyAll();
            }
        }

        @Override
        public void notifyEndOfStream(IInputChannel channel) {
            synchronized (NonDeterministicPartitionCollector.this) {
                PartitionId pid = (PartitionId) channel.getAttachment();
                int senderIndex = pid.getSenderIndex();
                eosSenders.set(senderIndex);
                NonDeterministicPartitionCollector.this.notifyAll();
            }
        }
    }

    @Override
    public Collection<PartitionId> getRequiredPartitionIds() throws HyracksException {
        Collection<PartitionId> c = new ArrayList<PartitionId>(expectedPartitions.cardinality());
        for (int i = expectedPartitions.nextSetBit(0); i >= 0; i = expectedPartitions.nextSetBit(i)) {
            c.add(new PartitionId(getJobId(), getConnectorId(), i, getReceiverIndex()));
        }
        return c;
    }

    @Override
    public void abort() {
    }
}