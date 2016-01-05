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
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class RateControlledTupleForwarder implements ITupleForwarder {

    private FrameTupleAppender appender;
    private IFrame frame;
    private IFrameWriter writer;
    private long interTupleInterval;
    private boolean delayConfigured;

    public static final String INTER_TUPLE_INTERVAL = "tuple-interval";

    @Override
    public void configure(Map<String, String> configuration) {
        String propValue = configuration.get(INTER_TUPLE_INTERVAL);
        if (propValue != null) {
            interTupleInterval = Long.parseLong(propValue);
        }
        delayConfigured = interTupleInterval != 0;
    }

    @Override
    public void initialize(IHyracksCommonContext ctx, IFrameWriter writer) throws HyracksDataException {
        this.appender = new FrameTupleAppender();
        this.frame = new VSizeFrame(ctx);
        this.writer = writer;
        appender.reset(frame, true);
    }

    @Override
    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        if (delayConfigured) {
            try {
                Thread.sleep(interTupleInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        boolean success = appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        if (!success) {
            FrameUtils.flushFrame(frame.getBuffer(), writer);
            appender.reset(frame, true);
            success = appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            if (!success) {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(frame.getBuffer(), writer);
        }

    }
}