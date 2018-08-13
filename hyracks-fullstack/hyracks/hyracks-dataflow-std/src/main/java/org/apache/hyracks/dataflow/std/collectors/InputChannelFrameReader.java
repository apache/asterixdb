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

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.channels.IInputChannelMonitor;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.net.protocols.muxdemux.AbstractChannelWriteInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InputChannelFrameReader implements IFrameReader, IInputChannelMonitor {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IInputChannel channel;

    private int availableFrames;

    private boolean eos;

    private int errorCode;

    public InputChannelFrameReader(IInputChannel channel) {
        this.channel = channel;
        availableFrames = 0;
        errorCode = AbstractChannelWriteInterface.NO_ERROR_CODE;
        eos = false;
    }

    @Override
    public void open() throws HyracksDataException {
    }

    private boolean hasFailed() {
        return errorCode != AbstractChannelWriteInterface.NO_ERROR_CODE;
    }

    private synchronized boolean canGetNextBuffer() throws HyracksDataException {
        while (!hasFailed() && !eos && availableFrames <= 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw HyracksDataException.create(e);
            }
        }
        if (hasFailed()) {
            if (errorCode == AbstractChannelWriteInterface.CONNECTION_LOST_ERROR_CODE) {
                throw HyracksDataException.create(ErrorCode.LOCAL_NETWORK_ERROR);
            }
            // Do not throw exception here to allow the root cause exception gets propagated to the master first.
            // Return false to allow the nextFrame(...) call to be a non-op.
            LOGGER.warn("Sender failed.. returning silently");
            return false;
        }
        if (availableFrames <= 0 && eos) {
            return false;
        }
        --availableFrames;
        return true;
    }

    /**
     * This implementation works under the truth that one Channel is neverNonDeterministicChannelReader shared by two readers.
     * More precisely, one channel only has exact one reader and one writer side.
     *
     * @param frame
     *            outputFrame
     * @return {@code true} if succeed to read the data from the channel to the {@code frame}.
     *         Otherwise return {@code false} if the end of stream is reached.
     * @throws HyracksDataException
     */
    @Override
    public boolean nextFrame(IFrame frame) throws HyracksDataException {
        if (!canGetNextBuffer()) {
            return false;
        }
        frame.reset();
        ByteBuffer srcFrame = channel.getNextBuffer();
        int nBlocks = FrameHelper.deserializeNumOfMinFrame(srcFrame);
        frame.ensureFrameSize(frame.getMinSize() * nBlocks);
        FrameUtils.copyWholeFrame(srcFrame, frame.getBuffer());
        channel.recycleBuffer(srcFrame);

        for (int i = 1; i < nBlocks; ++i) {
            if (!canGetNextBuffer()) {
                return false;
            }
            srcFrame = channel.getNextBuffer();
            frame.getBuffer().put(srcFrame);
            channel.recycleBuffer(srcFrame);
        }
        if (frame.getBuffer().hasRemaining()) { // bigger frame
            FrameHelper.clearRemainingFrame(frame.getBuffer(), frame.getBuffer().position());
        }
        frame.getBuffer().flip();
        return true;
    }

    @Override
    public void close() throws HyracksDataException {

    }

    @Override
    public synchronized void notifyFailure(IInputChannel channel, int errorCode) {
        // Note: if a remote failure overwrites the value of localFailure, then we rely on
        // the fact that the remote task will notify the cc of the failure.
        // Otherwise, the local task must fail
        this.errorCode = errorCode;
        notifyAll();
    }

    @Override
    public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
        availableFrames += nFrames;
        notifyAll();
    }

    @Override
    public synchronized void notifyEndOfStream(IInputChannel channel) {
        eos = true;
        notifyAll();
    }
}
