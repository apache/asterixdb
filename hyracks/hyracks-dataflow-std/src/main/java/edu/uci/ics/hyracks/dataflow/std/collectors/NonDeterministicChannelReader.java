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
package edu.uci.ics.hyracks.dataflow.std.collectors;

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.channels.IInputChannel;
import edu.uci.ics.hyracks.api.channels.IInputChannelMonitor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class NonDeterministicChannelReader implements IInputChannelMonitor, IPartitionAcceptor {
    private static final Logger LOGGER = Logger.getLogger(NonDeterministicChannelReader.class.getName());

    private final int nSenderPartitions;

    private final IInputChannel[] channels;

    private final BitSet frameAvailability;

    private final int[] availableFrameCounts;

    private final BitSet eosSenders;

    private final BitSet failSenders;

    private final BitSet closedSenders;

    private int lastReadSender;

    public NonDeterministicChannelReader(int nSenderPartitions, BitSet expectedPartitions) {
        this.nSenderPartitions = nSenderPartitions;
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
    public void addPartition(PartitionId pid, IInputChannel channel) {
        channel.registerMonitor(this);
        channel.setAttachment(pid);
        synchronized (this) {
            channels[pid.getSenderIndex()] = channel;
        }
    }

    public int getSenderPartitionCount() {
        return nSenderPartitions;
    }

    public void open() throws HyracksDataException {
        lastReadSender = -1;
    }

    public IInputChannel[] getChannels() {
        return channels;
    }

    public synchronized int findNextSender() throws HyracksDataException {
        while (true) {
            lastReadSender = frameAvailability.nextSetBit(lastReadSender + 1);
            if (lastReadSender < 0) {
                lastReadSender = frameAvailability.nextSetBit(0);
            }
            if (lastReadSender >= 0) {
                assert availableFrameCounts[lastReadSender] > 0;
                if (--availableFrameCounts[lastReadSender] == 0) {
                    frameAvailability.clear(lastReadSender);
                }
                return lastReadSender;
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
                return lastReadSender;
            }
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    public synchronized void close() throws HyracksDataException {
        for (int i = closedSenders.nextClearBit(0); i >= 0 && i < nSenderPartitions; i = closedSenders
                .nextClearBit(i + 1)) {
            if (channels[i] != null) {
                channels[i].close();
                channels[i] = null;
            }
        }
    }

    @Override
    public synchronized void notifyFailure(IInputChannel channel) {
        PartitionId pid = (PartitionId) channel.getAttachment();
        int senderIndex = pid.getSenderIndex();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Failure: " + pid.getConnectorDescriptorId() + " sender: " + senderIndex + " receiver: "
                    + pid.getReceiverIndex());
        }
        failSenders.set(senderIndex);
        eosSenders.set(senderIndex);
        notifyAll();
    }

    @Override
    public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
        PartitionId pid = (PartitionId) channel.getAttachment();
        int senderIndex = pid.getSenderIndex();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Data available: " + pid.getConnectorDescriptorId() + " sender: " + senderIndex + " receiver: "
                    + pid.getReceiverIndex());
        }
        availableFrameCounts[senderIndex] += nFrames;
        frameAvailability.set(senderIndex);
        notifyAll();
    }

    @Override
    public synchronized void notifyEndOfStream(IInputChannel channel) {
        PartitionId pid = (PartitionId) channel.getAttachment();
        int senderIndex = pid.getSenderIndex();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("EOS: " + pid);
        }
        eosSenders.set(senderIndex);
        notifyAll();
    }
}