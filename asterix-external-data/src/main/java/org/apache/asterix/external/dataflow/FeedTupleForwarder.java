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

import java.util.Map;

import org.apache.asterix.common.parse.ITupleForwarder;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class FeedTupleForwarder implements ITupleForwarder {

    private FrameTupleAppender appender;
    private IFrame frame;
    private IFrameWriter writer;
    private boolean paused = false;

    @Override
    public void configure(Map<String, String> configuration) {
    }

    @Override
    public void initialize(IHyracksCommonContext ctx, IFrameWriter writer) throws HyracksDataException {
        this.frame = new VSizeFrame(ctx);
        this.writer = writer;
        this.appender = new FrameTupleAppender(frame);
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
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(frame.getBuffer(), writer);
        }
    }
}
