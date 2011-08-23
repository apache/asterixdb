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
package edu.uci.ics.hyracks.dataflow.std.collectors;

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

    private final int nSenderPartitions;

    private final IInputChannel[] channels;

    private final BitSet frameAvailability;

    private final int[] availableFrameCounts;

    private final BitSet eosSenders;

    private final BitSet failSenders;

    private BitSet closedSenders;

    private int lastReadSender;

    public NonDeterministicPartitionCollector(IHyracksTaskContext ctx, ConnectorDescriptorId connectorId,
            int receiverIndex, int nSenderPartitions, BitSet expectedPartitions) {
        super(ctx, connectorId, receiverIndex);
        this.expectedPartitions = expectedPartitions;
        this.nSenderPartitions = nSenderPartitions;
        reader = new FrameReader();
        channels = new IInputChannel[nSenderPartitions];
        eosSenders = new BitSet(nSenderPartitions);
        failSenders = new BitSet(nSenderPartitions);
        closedSenders = new BitSet(nSenderPartitions);
        closedSenders.or(expectedPartitions);
        closedSenders.flip(0, nSenderPartitions);
        frameAvailability = new BitSet(nSenderPartitions);
        availableFrameCounts = new int[nSenderPartitions];
    }

    @Override
    public void open() throws HyracksException {
        lastReadSender = 0;
    }

    @Override
    public void addPartitions(Collection<PartitionChannel> partitions) throws HyracksException {
        for (PartitionChannel pc : partitions) {
            PartitionId pid = pc.getPartitionId();
            IInputChannel channel = pc.getInputChannel();
            channel.setAttachment(pid);
            channel.registerMonitor(reader);
            synchronized (this) {
                channels[pid.getSenderIndex()] = channel;
            }
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
            findNextSender();
            if (lastReadSender >= 0) {
                ByteBuffer srcFrame = channels[lastReadSender].getNextBuffer();
                FrameUtils.copy(srcFrame, buffer);
                channels[lastReadSender].recycleBuffer(srcFrame);
                return true;
            }
            return false;
        }

        private void findNextSender() throws HyracksDataException {
            synchronized (NonDeterministicPartitionCollector.this) {
                while (true) {
                    switch (lastReadSender) {
                        default:
                            lastReadSender = frameAvailability.nextSetBit(lastReadSender + 1);
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
                        return;
                    }
                    if (!failSenders.isEmpty()) {
                        throw new HyracksDataException("Failure occurred on input");
                    }
                    for (int i = eosSenders.nextSetBit(0); i >= 0; i = eosSenders.nextSetBit(i)) {
                        channels[i].close();
                        eosSenders.clear(i);
                        closedSenders.set(i);
                    }
                    int nextClosedBitIndex = closedSenders.nextClearBit(0);
                    if (nextClosedBitIndex < 0 || nextClosedBitIndex >= nSenderPartitions) {
                        lastReadSender = -1;
                        return;
                    }
                    try {
                        NonDeterministicPartitionCollector.this.wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }
        }

        @Override
        public void close() throws HyracksDataException {
            synchronized (NonDeterministicPartitionCollector.this) {
                for (int i = closedSenders.nextClearBit(0); i >= 0 && i < nSenderPartitions; i = closedSenders
                        .nextClearBit(i + 1)) {
                    if (channels[i] != null) {
                        channels[i].close();
                        channels[i] = null;
                    }
                }
            }
        }

        @Override
        public void notifyFailure(IInputChannel channel) {
            synchronized (NonDeterministicPartitionCollector.this) {
                PartitionId pid = (PartitionId) channel.getAttachment();
                int senderIndex = pid.getSenderIndex();
                failSenders.set(senderIndex);
                NonDeterministicPartitionCollector.this.notifyAll();
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
        for (int i = expectedPartitions.nextSetBit(0); i >= 0; i = expectedPartitions.nextSetBit(i + 1)) {
            c.add(new PartitionId(getJobId(), getConnectorId(), i, getReceiverIndex()));
        }
        return c;
    }

    @Override
    public void abort() {
    }
}