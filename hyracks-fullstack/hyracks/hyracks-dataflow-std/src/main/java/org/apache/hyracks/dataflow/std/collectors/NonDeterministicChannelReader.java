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
package org.apache.hyracks.dataflow.std.collectors;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.channels.IInputChannelMonitor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.net.protocols.muxdemux.AbstractChannelWriteInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NonDeterministicChannelReader implements IInputChannelMonitor, IPartitionAcceptor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final int nSenderPartitions;

    private final IInputChannel[] channels;

    private final BitSet frameAvailability;

    private final int[] availableFrameCounts;

    private final BitSet eosSenders;

    private final BitSet failSenders;

    private final BitSet closedSenders;

    private int lastReadSender;

    private boolean localFailure;

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

    public synchronized ByteBuffer getNextBuffer(int index) throws HyracksDataException {
        while ((availableFrameCounts[index] <= 0)) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw HyracksDataException.create(e);
            }
        }
        if (--availableFrameCounts[index] == 0) {
            frameAvailability.clear(index);
        }
        return channels[index].getNextBuffer();

    }

    public void recycleBuffer(int index, ByteBuffer frame) {
        channels[index].recycleBuffer(frame);
    }

    public void open() throws HyracksDataException {
        lastReadSender = -1;
    }

    public synchronized int findNextSender() throws HyracksDataException {
        while (true) {
            lastReadSender = frameAvailability.nextSetBit(lastReadSender + 1);
            if (lastReadSender < 0) {
                lastReadSender = frameAvailability.nextSetBit(0);
            }
            if (lastReadSender >= 0) {
                assert availableFrameCounts[lastReadSender] > 0;
                return lastReadSender;
            }
            if (!failSenders.isEmpty()) {
                LOGGER.warn("Sender failed.. returning silently");
                if (localFailure) {
                    throw HyracksDataException.create(ErrorCode.LOCAL_NETWORK_ERROR);
                }
                // Do not throw exception here to allow the root cause exception gets propagated to the master first.
                // Return a negative value to allow the nextFrame(...) call to be a non-op.
                return -1;
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
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
        }
    }

    public synchronized void close() throws HyracksDataException {
        for (int i = closedSenders.nextClearBit(0); i >= 0 && i < nSenderPartitions; i =
                closedSenders.nextClearBit(i + 1)) {
            if (channels[i] != null) {
                channels[i].close();
                channels[i] = null;
            }
        }
    }

    @Override
    public synchronized void notifyFailure(IInputChannel channel, int errorCode) {
        PartitionId pid = (PartitionId) channel.getAttachment();
        int senderIndex = pid.getSenderIndex();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Failure: " + pid.getConnectorDescriptorId() + " sender: " + senderIndex + " receiver: "
                    + pid.getReceiverIndex());
        }
        // Note: if a remote failure overwrites the value of localFailure, then we rely on
        // the fact that the remote task will notify the cc of the failure.
        // Otherwise, the local task must fail
        localFailure = errorCode == AbstractChannelWriteInterface.CONNECTION_LOST_ERROR_CODE;
        failSenders.set(senderIndex);
        eosSenders.set(senderIndex);
        notifyAll();
    }

    @Override
    public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
        PartitionId pid = (PartitionId) channel.getAttachment();
        int senderIndex = pid.getSenderIndex();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Data available: " + pid.getConnectorDescriptorId() + " sender: " + senderIndex + " receiver: "
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
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("EOS: " + pid);
        }
        eosSenders.set(senderIndex);
        notifyAll();
    }
}
