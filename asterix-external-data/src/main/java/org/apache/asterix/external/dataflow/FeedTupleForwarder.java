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
package org.apache.asterix.external.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.api.ITupleForwarder;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.asterix.external.util.FeedMessageUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class FeedTupleForwarder implements ITupleForwarder {

    private final FeedLogManager feedLogManager;
    private int maxRecordSize; // temporary until the big object in storage is solved
    private FrameTupleAppender appender;
    private IFrame frame;
    private IFrameWriter writer;
    private boolean paused = false;
    private boolean initialized;

    public FeedTupleForwarder(@Nonnull FeedLogManager feedLogManager) {
        this.feedLogManager = feedLogManager;
    }

    public FeedLogManager getFeedLogManager() {
        return feedLogManager;
    }

    @Override
    public void initialize(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
        if (!initialized) {
            this.maxRecordSize = ((IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                    .getApplicationObject()).getBufferCache().getPageSize() / 2;
            this.frame = new VSizeFrame(ctx);
            this.writer = writer;
            this.appender = new FrameTupleAppender(frame);
            // Set null feed message
            ByteBuffer message = (ByteBuffer) ctx.getSharedObject();
            // a null message
            message.put(FeedMessageUtils.NULL_FEED_MESSAGE);
            message.flip();
            initialized = true;
        }
    }

    @Override
    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        if (tb.getSize() > maxRecordSize) {
            try {
                feedLogManager.logRecord(tb.toString(), ExternalDataConstants.ERROR_LARGE_RECORD);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        if (paused) {
            synchronized (this) {
                while (paused) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }
        }
        DataflowUtils.addTupleToFrame(appender, tb, writer);
    }

    public void pause() {
        paused = true;
    }

    public synchronized void resume() {
        paused = false;
        notifyAll();
    }

    @Override
    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(frame.getBuffer(), writer);
        }
        try {
            feedLogManager.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }

    public int getMaxRecordSize() {
        return maxRecordSize;
    }
}
