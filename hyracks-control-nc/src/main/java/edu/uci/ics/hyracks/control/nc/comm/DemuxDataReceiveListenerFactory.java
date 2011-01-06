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
package edu.uci.ics.hyracks.control.nc.comm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.BitSet;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListener;
import edu.uci.ics.hyracks.api.comm.IDataReceiveListenerFactory;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DemuxDataReceiveListenerFactory implements IDataReceiveListenerFactory, IConnectionDemultiplexer,
        IDataReceiveListener {
    private static final Logger LOGGER = Logger.getLogger(DemuxDataReceiveListenerFactory.class.getName());

    private final BitSet readyBits;
    private final int frameSize;
    private IConnectionEntry senders[];
    private int openSenderCount;
    private UUID jobId;
    private UUID stageId;

    public DemuxDataReceiveListenerFactory(IHyracksStageletContext ctx, UUID jobId, UUID stageId) {
        frameSize = ctx.getFrameSize();
        this.jobId = jobId;
        this.stageId = stageId;
        readyBits = new BitSet();
        senders = null;
        openSenderCount = 0;
    }

    @Override
    public IDataReceiveListener getDataReceiveListener(UUID endpointUUID, IConnectionEntry entry, int senderIndex) {
        entry.attach(senderIndex);
        addSender(senderIndex, entry);
        return this;
    }

    @Override
    public synchronized void dataReceived(IConnectionEntry entry) throws IOException {
        int senderIndex = (Integer) entry.getAttachment();
        ByteBuffer buffer = entry.getReadBuffer();
        buffer.flip();
        int dataLen = buffer.remaining();
        if (dataLen >= frameSize || entry.aborted()) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest("NonDeterministicDataReceiveListener: frame received: sender = " + senderIndex);
            }
            SelectionKey key = entry.getSelectionKey();
            if (key.isValid()) {
                int ops = key.interestOps();
                key.interestOps(ops & ~SelectionKey.OP_READ);
            }
            readyBits.set(senderIndex);
            notifyAll();
            return;
        }
        buffer.compact();
    }

    @Override
    public void eos(IConnectionEntry entry) {
    }

    private synchronized void addSender(int senderIndex, IConnectionEntry entry) {
        readyBits.clear(senderIndex);
        if (senders == null) {
            senders = new IConnectionEntry[senderIndex + 1];
        } else if (senders.length <= senderIndex) {
            senders = Arrays.copyOf(senders, senderIndex + 1);
        }
        senders[senderIndex] = entry;
        ++openSenderCount;
    }

    @Override
    public synchronized IConnectionEntry findNextReadyEntry(int lastReadSender) {
        while (openSenderCount > 0 && readyBits.isEmpty()) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
        lastReadSender = readyBits.nextSetBit(lastReadSender);
        if (lastReadSender < 0) {
            lastReadSender = readyBits.nextSetBit(0);
        }
        return senders[lastReadSender];
    }

    @Override
    public synchronized void unreadyEntry(int index) {
        readyBits.clear(index);
        IConnectionEntry entry = senders[index];
        SelectionKey key = entry.getSelectionKey();
        if (key.isValid()) {
            int ops = key.interestOps();
            key.interestOps(ops | SelectionKey.OP_READ);
            key.selector().wakeup();
        }
    }

    @Override
    public synchronized int closeEntry(int index) throws HyracksDataException {
        IConnectionEntry entry = senders[index];
        SelectionKey key = entry.getSelectionKey();
        key.cancel();
        try {
            entry.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        return --openSenderCount;
    }

    @Override
    public synchronized int getSenderCount() {
        return senders.length;
    }

    @Override
    public UUID getJobId() {
        return jobId;
    }

    @Override
    public UUID getStageId() {
        return stageId;
    }
}