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
package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.util.Arrays;
import java.util.BitSet;

public class ChannelSet {
    private static final int INITIAL_SIZE = 16;

    private final MultiplexedConnection mConn;

    private ChannelControlBlock[] ccbArray;

    private final BitSet allocationBitmap;

    private final BitSet pendingChannelWriteBitmap;

    private final BitSet pendingChannelCreditsBitmap;

    private final BitSet pendingChannelSynBitmap;

    private int openChannelCount;

    private final IEventCounter pendingWriteEventsCounter;

    ChannelSet(MultiplexedConnection mConn, IEventCounter pendingWriteEventsCounter) {
        this.mConn = mConn;
        ccbArray = new ChannelControlBlock[INITIAL_SIZE];
        allocationBitmap = new BitSet();
        pendingChannelWriteBitmap = new BitSet();
        pendingChannelCreditsBitmap = new BitSet();
        pendingChannelSynBitmap = new BitSet();
        this.pendingWriteEventsCounter = pendingWriteEventsCounter;
        openChannelCount = 0;
    }

    ChannelControlBlock allocateChannel() {
        synchronized (mConn) {
            int idx = allocationBitmap.nextClearBit(0);
            if (idx < 0) {
                cleanupClosedChannels();
                idx = allocationBitmap.nextClearBit(0);
                if (idx < 0) {
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
                    freeChannel(ccb);
                }
            }
        }
    }

    ChannelControlBlock registerChannel(int channelId) {
        return createChannel(channelId);
    }

    private void freeChannel(ChannelControlBlock channel) {
        int idx = channel.getChannelId();
        ccbArray[idx] = null;
        allocationBitmap.clear(idx);
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

    void markPendingCredits(int channelId) {
        synchronized (mConn) {
            if (!pendingChannelCreditsBitmap.get(channelId)) {
                pendingChannelCreditsBitmap.set(channelId);
                pendingWriteEventsCounter.increment();
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

    public void unmarkPendingWrite(int channelId) {
        synchronized (mConn) {
            assert pendingChannelWriteBitmap.get(channelId);
            pendingChannelWriteBitmap.clear(channelId);
            pendingWriteEventsCounter.decrement();
        }
    }

    private ChannelControlBlock createChannel(int idx) {
        if (idx >= ccbArray.length) {
            expand(idx);
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