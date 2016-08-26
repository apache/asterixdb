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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IBufferAcceptor;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.IChannelWriteInterface;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;

public abstract class AbstractChannelWriteInterface implements IChannelWriteInterface {

    private static final Logger LOGGER = Logger.getLogger(AbstractChannelWriteInterface.class.getName());
    protected final IChannelControlBlock ccb;
    protected final Queue<ByteBuffer> wiFullQueue;
    protected boolean channelWritabilityState;
    protected final int channelId;
    protected IBufferAcceptor eba;
    protected int credits;
    protected boolean eos;
    protected boolean eosSent;
    protected int ecode;
    protected boolean ecodeSent;
    protected ByteBuffer currentWriteBuffer;
    private final ICloseableBufferAcceptor fba;

    public AbstractChannelWriteInterface(IChannelControlBlock ccb) {
        this.ccb = ccb;
        this.channelId = ccb.getChannelId();
        wiFullQueue = new ArrayDeque<>();
        fba = new CloseableBufferAcceptor();
        credits = 0;
        eos = false;
        eosSent = false;
        ecode = -1;
        ecodeSent = false;
    }

    @Override
    public void writeComplete() {
        if (currentWriteBuffer.remaining() <= 0) {
            currentWriteBuffer.clear();
            eba.accept(currentWriteBuffer);
            currentWriteBuffer = null;
            adjustChannelWritability();
        }
    }

    private boolean computeWritability() {
        boolean writableDataPresent = currentWriteBuffer != null || !wiFullQueue.isEmpty();
        if (writableDataPresent) {
            return credits > 0;
        }
        if (eos && !eosSent) {
            return true;
        }
        if (ecode >= 0 && !ecodeSent) {
            return true;
        }
        return false;
    }

    @Override
    public void adjustChannelWritability() {
        boolean writable = computeWritability();
        if (writable) {
            if (!channelWritabilityState) {
                ccb.markPendingWrite();
            }
        } else {
            if (channelWritabilityState) {
                ccb.unmarkPendingWrite();
            }
        }
        channelWritabilityState = writable;
    }

    @Override
    public void addCredits(int credit) {
        credits += credit;
    }

    @Override
    public void setEmptyBufferAcceptor(IBufferAcceptor emptyBufferAcceptor) {
        eba = emptyBufferAcceptor;
    }

    @Override
    public ICloseableBufferAcceptor getFullBufferAcceptor() {
        return fba;
    }

    @Override
    public int getCredits() {
        return credits;
    }

    private class CloseableBufferAcceptor implements ICloseableBufferAcceptor {
        @Override
        public void accept(ByteBuffer buffer) {
            synchronized (ccb) {
                wiFullQueue.add(buffer);
                adjustChannelWritability();
            }
        }

        @Override
        public void close() {
            synchronized (ccb) {
                if (eos) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Received duplicate close() on channel: " + ccb.getChannelId());
                    }
                    return;
                }
                eos = true;
                adjustChannelWritability();
            }
        }

        @Override
        public void error(int ecode) {
            synchronized (ccb) {
                AbstractChannelWriteInterface.this.ecode = ecode;
                adjustChannelWritability();
            }
        }
    }
}
