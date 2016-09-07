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

import org.apache.asterix.external.api.ITupleForwarder;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.common.util.TaskUtils;

public class FeedTupleForwarder implements ITupleForwarder {

    private final FeedLogManager feedLogManager;
    private FrameTupleAppender appender;
    private IFrame frame;
    private IFrameWriter writer;
    private boolean paused = false;
    private boolean initialized;

    public FeedTupleForwarder(FeedLogManager feedLogManager) {
        this.feedLogManager = feedLogManager;
    }

    public FeedLogManager getFeedLogManager() {
        return feedLogManager;
    }

    @Override
    public void initialize(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
        if (!initialized) {
            this.frame = new VSizeFrame(ctx);
            this.writer = writer;
            this.appender = new FrameTupleAppender(frame);
            // Set null feed message
            VSizeFrame message = TaskUtils.<VSizeFrame> get(HyracksConstants.KEY_MESSAGE, ctx);
            // a null message
            message.getBuffer().put(MessagingFrameTupleAppender.NULL_FEED_MESSAGE);
            message.getBuffer().flip();
            initialized = true;
        }
    }

    @Override
    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
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
        Throwable throwable = null;
        try {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame.getBuffer(), writer);
            }
        } catch (Throwable th) {
            throwable = th;
            throw th;
        } finally {
            try {
                feedLogManager.close();
            } catch (IOException e) {
                if (throwable != null) {
                    throwable.addSuppressed(e);
                } else {
                    throw new HyracksDataException(e);
                }
            } catch (Throwable th) {
                if (throwable != null) {
                    throwable.addSuppressed(th);
                } else {
                    throw th;
                }
            }
        }
    }

    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }
}
