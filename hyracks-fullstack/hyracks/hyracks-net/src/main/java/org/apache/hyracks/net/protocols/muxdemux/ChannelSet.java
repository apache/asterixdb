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

import java.util.Arrays;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.comm.MuxDemuxCommand;
import org.apache.hyracks.api.exceptions.NetException;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;

public class ChannelSet {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final int INITIAL_SIZE = 16;

    private final MultiplexedConnection mConn;

    private ChannelControlBlock[] ccbArray;

    private final BitSet allocationBitmap;

    private final BitSet pendingChannelWriteBitmap;

    private final BitSet pendingChannelCreditsBitmap;

    private final BitSet pendingChannelSynBitmap;

    private final BitSet pendingEOSAckBitmap;

    /**
     * Channels that have become completely closed (both close acks seen) but still occupy their slot. A slot is only
     * recycled when a channel is next created, so this is what lets that reclamation visit just the closed channels
     * rather than every slot.
     */
    private final BitSet reclaimableBitmap;

    private int openChannelCount;

    private final IEventCounter pendingWriteEventsCounter;

    ChannelSet(MultiplexedConnection mConn, IEventCounter pendingWriteEventsCounter) {
        this.mConn = mConn;
        ccbArray = new ChannelControlBlock[INITIAL_SIZE];
        allocationBitmap = new BitSet();
        pendingChannelWriteBitmap = new BitSet();
        pendingChannelCreditsBitmap = new BitSet();
        pendingChannelSynBitmap = new BitSet();
        pendingEOSAckBitmap = new BitSet();
        reclaimableBitmap = new BitSet();
        this.pendingWriteEventsCounter = pendingWriteEventsCounter;
        openChannelCount = 0;
    }

    ChannelControlBlock allocateChannel() throws NetException {
        synchronized (mConn) {
            cleanupClosedChannels();
            return createChannel(allocationBitmap.nextClearBit(0));
        }
    }

    private void cleanupClosedChannels() {
        for (int i = reclaimableBitmap.nextSetBit(0); i >= 0; i = reclaimableBitmap.nextSetBit(i + 1)) {
            ChannelControlBlock ccb = ccbArray[i];
            // a slot is only marked while it holds its block, and freeChannel() clears the mark when it empties one
            assert ccb != null : i;
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Cleaning free channel: " + ccb);
            }
            freeChannel(ccb);
        }
    }

    /**
     * Records the ack of our close of the given channel, looking the channel up and acking it in one critical section.
     *
     * @return the channel acked
     */
    ChannelControlBlock reportLocalEOSAck(int channelId) {
        synchronized (mConn) {
            ChannelControlBlock ccb = ccbArray[channelId];
            ccb.reportLocalEOSAck();
            return ccb;
        }
    }

    /**
     * Called by a control block each time it records one of its two close acks, so that the flags and the mark cannot
     * be separated. The acks arrive on different threads; each sets its flag before taking the lock to check, so
     * whichever check runs second sees both and marks the slot. The identity check keeps a block that has already
     * been superseded in its slot from marking its successor.
     */
    void markReclaimableIfClosed(ChannelControlBlock ccb) {
        synchronized (mConn) {
            int idx = ccb.getChannelId();
            if (ccb.completelyClosed() && ccbArray[idx] == ccb) {
                reclaimableBitmap.set(idx);
            }
        }
    }

    ChannelControlBlock registerChannel(int channelId) throws NetException {
        synchronized (mConn) {
            return createChannel(channelId);
        }
    }

    private void freeChannel(ChannelControlBlock channel) {
        int idx = channel.getChannelId();
        ccbArray[idx] = null;
        allocationBitmap.clear(idx);
        pendingChannelWriteBitmap.clear(idx);
        pendingChannelCreditsBitmap.clear(idx);
        pendingChannelSynBitmap.clear(idx);
        pendingEOSAckBitmap.clear(idx);
        reclaimableBitmap.clear(idx);
        --openChannelCount;
    }

    ChannelControlBlock getCCB(int channelId) {
        return ccbArray[channelId];
    }

    BitSet getPendingChannelWriteBitmap() {
        return pendingChannelWriteBitmap;
    }

    BitSet getPendingChannelCreditsBitmap() {
        return pendingChannelCreditsBitmap;
    }

    BitSet getPendingChannelSynBitmap() {
        return pendingChannelSynBitmap;
    }

    BitSet getPendingEOSAckBitmap() {
        return pendingEOSAckBitmap;
    }

    int getOpenChannelCount() {
        return openChannelCount;
    }

    void initiateChannelSyn(int channelId) {
        synchronized (mConn) {
            assert !pendingChannelSynBitmap.get(channelId);
            pendingChannelSynBitmap.set(channelId);
            pendingWriteEventsCounter.increment();
        }
    }

    void addPendingCredits(ChannelControlBlock targetCcb, int delta) {
        if (delta <= 0) {
            return;
        }
        synchronized (mConn) {
            final int channelId = targetCcb.getChannelId();
            ChannelControlBlock ccb = ccbArray[channelId];
            // ensure the channel slot id was not recycled and used for a diffierent channel
            if (ccb == targetCcb) {
                if (ccb.getRemoteEOS()) {
                    return;
                }
                int oldCredits = ccb.getReadCredits();
                ccb.setReadCredits(oldCredits + delta);
                if (oldCredits == 0) {
                    assert !pendingChannelCreditsBitmap.get(channelId);
                    pendingChannelCreditsBitmap.set(channelId);
                    pendingWriteEventsCounter.increment();
                }
            }
        }
    }

    void unmarkPendingCredits(int channelId) {
        synchronized (mConn) {
            if (pendingChannelCreditsBitmap.get(channelId)) {
                pendingChannelCreditsBitmap.clear(channelId);
                pendingWriteEventsCounter.decrement();
            }
        }
    }

    void markPendingWrite(int channelId) {
        synchronized (mConn) {
            assert !pendingChannelWriteBitmap.get(channelId);
            pendingChannelWriteBitmap.set(channelId);
            pendingWriteEventsCounter.increment();
        }
    }

    void unmarkPendingWrite(int channelId) {
        synchronized (mConn) {
            assert pendingChannelWriteBitmap.get(channelId);
            pendingChannelWriteBitmap.clear(channelId);
            pendingWriteEventsCounter.decrement();
        }
    }

    void markEOSAck(int channelId) {
        synchronized (mConn) {
            if (!pendingEOSAckBitmap.get(channelId)) {
                pendingEOSAckBitmap.set(channelId);
                pendingWriteEventsCounter.increment();
            }
        }
    }

    void notifyIOError() {
        synchronized (mConn) {
            for (int i = 0; i < ccbArray.length; ++i) {
                ChannelControlBlock ccb = ccbArray[i];
                if (ccb != null) {
                    ccb.reportRemoteError(AbstractChannelWriteInterface.CONNECTION_LOST_ERROR_CODE);
                    markEOSAck(i);
                    unmarkPendingCredits(i);
                }
            }
        }
    }

    private ChannelControlBlock createChannel(int idx) throws NetException {
        if (idx > MuxDemuxCommand.MAX_CHANNEL_ID) {
            throw new NetException("Channel Id > " + MuxDemuxCommand.MAX_CHANNEL_ID + " being opened");
        }
        if (idx >= ccbArray.length) {
            expand(idx);
        }
        if (ccbArray[idx] != null) {
            assert ccbArray[idx].completelyClosed() : ccbArray[idx].toString();
            if (ccbArray[idx].completelyClosed()) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Cleaning free channel: " + ccbArray[idx]);
                }
                freeChannel(ccbArray[idx]);
            }
        }
        assert idx < ccbArray.length;
        assert !allocationBitmap.get(idx);
        IChannelInterfaceFactory channelInterfaceFactory = mConn.getChannelInterfaceFactory();
        ChannelControlBlock channel = new ChannelControlBlock(this, idx, channelInterfaceFactory);
        ccbArray[idx] = channel;
        allocationBitmap.set(idx);
        ++openChannelCount;
        return channel;
    }

    private void expand(int idx) {
        ccbArray = Arrays.copyOf(ccbArray, Math.max(idx + 1, ccbArray.length * 2));
    }

    public MultiplexedConnection getMultiplexedConnection() {
        return mConn;
    }

    public synchronized ArrayNode getState() {
        final ArrayNode state = JSONUtil.createArray();
        for (ChannelControlBlock ccb : ccbArray) {
            if (ccb != null) {
                state.add(ccb.getState());
            }
        }
        return state;
    }
}
