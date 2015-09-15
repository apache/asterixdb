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
package org.apache.asterix.runtime.operators.file;

import java.util.Map;

import org.apache.asterix.common.parse.ITupleParserPolicy;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class RateContolledParserPolicy implements ITupleParserPolicy {

    protected FrameTupleAppender appender;
    protected IFrame  frame;
    private IFrameWriter writer;
    private long interTupleInterval;
    private boolean delayConfigured;

    public static final String INTER_TUPLE_INTERVAL = "tuple-interval";

    public RateContolledParserPolicy() {

    }

    public TupleParserPolicy getType() {
        return ITupleParserPolicy.TupleParserPolicy.FRAME_FULL;
    }

 
    @Override
    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        if (delayConfigured) {
            try {
                Thread.sleep(interTupleInterval);
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
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
        appender.reset(frame, true);
    }

    @Override
    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(frame.getBuffer(), writer);
        }
    }

    @Override
    public void configure(Map<String, String> configuration) throws HyracksDataException {
        String propValue = configuration.get(INTER_TUPLE_INTERVAL);
        if (propValue != null) {
            interTupleInterval = Long.parseLong(propValue);
        } else {
            interTupleInterval = 0;
        }
        delayConfigured = interTupleInterval != 0;
        
    }

    @Override
    public void initialize(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
        this.appender = new FrameTupleAppender();
        this.frame = new VSizeFrame(ctx);
    }
}
