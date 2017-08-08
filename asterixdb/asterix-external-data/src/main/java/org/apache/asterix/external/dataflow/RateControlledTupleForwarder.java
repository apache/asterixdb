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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.ITupleForwarder;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
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

    private RateControlledTupleForwarder(long interTupleInterval) {
        this.interTupleInterval = interTupleInterval;
        delayConfigured = interTupleInterval != 0L;
    }

    // Factory method
    public static RateControlledTupleForwarder create(Map<String, String> configuration) {
        long interTupleInterval = 0L;
        String propValue = configuration.get(INTER_TUPLE_INTERVAL);
        if (propValue != null) {
            interTupleInterval = Long.parseLong(propValue);
        }
        return new RateControlledTupleForwarder(interTupleInterval);
    }

    @Override
    public void initialize(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
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
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
        }
        boolean success = appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        if (!success) {
            FrameUtils.flushFrame(frame.getBuffer(), writer);
            appender.reset(frame, true);
            success = appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            if (!success) {
                throw new RuntimeDataException(ErrorCode.DATAFLOW_ILLEGAL_STATE);
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
