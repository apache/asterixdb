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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.comm.IBufferAcceptor;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.IChannelWriteInterface;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractChannelWriteInterface implements IChannelWriteInterface {

    public static final int NO_ERROR_CODE = 0;
    public static final int CONNECTION_LOST_ERROR_CODE = -1;
    public static final int REMOTE_ERROR_CODE = 1;
    private static final Logger LOGGER = LogManager.getLogger();
    protected final IChannelControlBlock ccb;
    protected final Queue<ByteBuffer> wiFullQueue;
    protected final AtomicInteger ecode = new AtomicInteger(NO_ERROR_CODE);
    protected boolean channelWritabilityState;
    protected final int channelId;
    protected IBufferAcceptor eba;
    protected int credits;
    protected boolean eos;
    protected boolean eosSent;
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
        if (isPendingCloseWrite()) {
            return true;
        }
        return ecode.get() == REMOTE_ERROR_CODE && !ecodeSent;
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

    protected boolean isPendingCloseWrite() {
        return eos && !eosSent && !ecodeSent;
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
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Received duplicate close() on channel: " + ccb.getChannelId());
                    }
                    return;
                }
                eos = true;
                if (ecode.get() != REMOTE_ERROR_CODE) {
                    adjustChannelWritability();
                }
            }
        }

        @Override
        public void error(int ecode) {
            AbstractChannelWriteInterface.this.ecode.set(ecode);
            if (ecode == CONNECTION_LOST_ERROR_CODE) {
                return;
            }
            synchronized (ccb) {
                adjustChannelWritability();
            }
        }
    }
}
