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
package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.util.Arrays;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.net.exceptions.NetException;

public class ChannelSet {
    private static final Logger LOGGER = Logger.getLogger(ChannelSet.class.getName());

    private static final int INITIAL_SIZE = 16;

    private final MultiplexedConnection mConn;

    private ChannelControlBlock[] ccbArray;

    private final BitSet allocationBitmap;

    private final BitSet pendingChannelWriteBitmap;

    private final BitSet pendingChannelCreditsBitmap;

    private final BitSet pendingChannelSynBitmap;

    private final BitSet pendingEOSAckBitmap;

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
        this.pendingWriteEventsCounter = pendingWriteEventsCounter;
        openChannelCount = 0;
    }

    ChannelControlBlock allocateChannel() throws NetException {
        synchronized (mConn) {
       	    cleanupClosedChannels();
            int idx = allocationBitmap.nextClearBit(0);
            if (idx < 0 || idx >= ccbArray.length) {
                cleanupClosedChannels();
                idx = allocationBitmap.nextClearBit(0);
                if (idx < 0 || idx == ccbArray.length) {
                    idx = ccbArray.length;
                }
            }
            return createChannel(idx);
        }
    }

    private void cleanupClosedChannels() {
        for (int i = 0; i < ccbArray.length; ++i) {
            ChannelControlBlock ccb = ccbArray[i];
            if (ccb != null) {
                if (ccb.completelyClosed()) {
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("Cleaning free channel: " + ccb);
                    }
                    freeChannel(ccb);
                }
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

    void addPendingCredits(int channelId, int delta) {
        if (delta <= 0) {
            return;
        }
        synchronized (mConn) {
            ChannelControlBlock ccb = ccbArray[channelId];
            if (ccb != null) {
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
                if (ccb != null && !ccb.getRemoteEOS()) {
                    ccb.reportRemoteError(-1);
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
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("Cleaning free channel: " + ccbArray[idx]);
                }
                freeChannel(ccbArray[idx]);
            }
        }
        assert idx < ccbArray.length;
        assert !allocationBitmap.get(idx);
        ChannelControlBlock channel = new ChannelControlBlock(this, idx);
        ccbArray[idx] = channel;
        allocationBitmap.set(idx);
        ++openChannelCount;
        return channel;
    }

    private void expand(int idx) {
        while (idx >= ccbArray.length) {
            ccbArray = Arrays.copyOf(ccbArray, ccbArray.length * 2);
        }
    }
}
