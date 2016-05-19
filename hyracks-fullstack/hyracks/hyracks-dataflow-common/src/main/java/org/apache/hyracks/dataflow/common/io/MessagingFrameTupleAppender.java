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
package org.apache.hyracks.dataflow.common.io;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.util.IntSerDeUtils;

public class MessagingFrameTupleAppender extends FrameTupleAppender {

    public static final int MAX_MESSAGE_SIZE = 100;
    private final IHyracksTaskContext ctx;

    public MessagingFrameTupleAppender(IHyracksTaskContext ctx) {
        this.ctx = ctx;
    }

    @Override
    protected boolean canHoldNewTuple(int fieldCount, int dataLength) throws HyracksDataException {
        if (hasEnoughSpace(fieldCount, dataLength + MAX_MESSAGE_SIZE)) {
            return true;
        }
        if (tupleCount == 0) {
            frame.ensureFrameSize(FrameHelper.calcAlignedFrameSizeToStore(fieldCount, dataLength + MAX_MESSAGE_SIZE,
                    frame.getMinSize()));
            reset(frame.getBuffer(), true);
            return true;
        }
        return false;
    }

    @Override
    public void write(IFrameWriter outWriter, boolean clearFrame) throws HyracksDataException {
        appendMessage((ByteBuffer) ctx.getSharedObject());
        getBuffer().clear();
        outWriter.nextFrame(getBuffer());
        if (clearFrame) {
            frame.reset();
            reset(getBuffer(), true);
        }
    }

    private void appendMessage(ByteBuffer message) {
        System.arraycopy(message.array(), message.position(), array, tupleDataEndOffset, message.limit());
        tupleDataEndOffset += message.limit();
        IntSerDeUtils.putInt(getBuffer().array(),
                FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1), tupleDataEndOffset);
        ++tupleCount;
        IntSerDeUtils.putInt(getBuffer().array(), FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
    }
}
