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

import org.apache.hyracks.api.comm.IBufferFactory;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.IConnectionWriterState;
import org.apache.hyracks.api.comm.MuxDemuxCommand;
import org.apache.hyracks.api.exceptions.NetException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FullFrameChannelWriteInterface extends AbstractChannelWriteInterface {

    private static final Logger LOGGER = LogManager.getLogger();

    FullFrameChannelWriteInterface(IChannelControlBlock ccb) {
        super(ccb);
    }

    @Override
    public void write(IConnectionWriterState writerState) throws NetException {
        if (currentWriteBuffer == null) {
            currentWriteBuffer = wiFullQueue.poll();
        }
        if (currentWriteBuffer != null) {
            int size = Math.min(currentWriteBuffer.remaining(), credits);
            if (size > 0) {
                credits -= size;
                writerState.getCommand().setChannelId(channelId);
                writerState.getCommand().setCommandType(MuxDemuxCommand.CommandType.DATA);
                writerState.getCommand().setData(size);
                writerState.reset(currentWriteBuffer, size, ccb);
            } else {
                adjustChannelWritability();
            }
        } else if (ecode.get() == REMOTE_ERROR_CODE && !ecodeSent) {
            writerState.getCommand().setChannelId(channelId);
            writerState.getCommand().setCommandType(MuxDemuxCommand.CommandType.ERROR);
            writerState.getCommand().setData(REMOTE_ERROR_CODE);
            writerState.reset(null, 0, null);
            ecodeSent = true;
            ccb.reportLocalEOS();
            adjustChannelWritability();
        } else if (isPendingCloseWrite()) {
            writerState.getCommand().setChannelId(channelId);
            writerState.getCommand().setCommandType(MuxDemuxCommand.CommandType.CLOSE_CHANNEL);
            writerState.getCommand().setData(0);
            writerState.reset(null, 0, null);
            eosSent = true;
            ccb.reportLocalEOS();
            adjustChannelWritability();
        }
    }

    @Override
    public void setBufferFactory(IBufferFactory bufferFactory, int limit, int frameSize) {
        if (!channelWritabilityState) {
            ccb.markPendingWrite();
        }
        channelWritabilityState = true;
        if (eos) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Received duplicate close() on channel: " + channelId);
            }
            return;
        }
        eos = true;
    }
}