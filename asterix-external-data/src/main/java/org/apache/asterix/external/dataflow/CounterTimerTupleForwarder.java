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
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.parse.ITupleForwarder;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

public class CounterTimerTupleForwarder implements ITupleForwarder {

    public static final String BATCH_SIZE = "batch-size";
    public static final String BATCH_INTERVAL = "batch-interval";

    private static final Logger LOGGER = Logger.getLogger(CounterTimerTupleForwarder.class.getName());

    private FrameTupleAppender appender;
    private IFrame frame;
    private IFrameWriter writer;
    private int batchSize;
    private long batchInterval;
    private int tuplesInFrame = 0;
    private TimeBasedFlushTask flushTask;
    private Timer timer;
    private Object lock = new Object();
    private boolean activeTimer = false;

    @Override
    public void configure(Map<String, String> configuration) {
        String propValue = configuration.get(BATCH_SIZE);
        if (propValue != null) {
            batchSize = Integer.parseInt(propValue);
        } else {
            batchSize = -1;
        }

        propValue = configuration.get(BATCH_INTERVAL);
        if (propValue != null) {
            batchInterval = Long.parseLong(propValue);
            activeTimer = true;
        }
    }

    @Override
    public void initialize(IHyracksCommonContext ctx, IFrameWriter writer) throws HyracksDataException {
        this.appender = new FrameTupleAppender();
        this.frame = new VSizeFrame(ctx);
        appender.reset(frame, true);
        this.writer = writer;
        if (activeTimer) {
            this.timer = new Timer();
            this.flushTask = new TimeBasedFlushTask(writer, lock);
            timer.scheduleAtFixedRate(flushTask, 0, batchInterval);
        }
    }

    @Override
    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        if (activeTimer) {
            synchronized (lock) {
                addTupleToFrame(tb);
            }
        } else {
            addTupleToFrame(tb);
        }
        tuplesInFrame++;
    }

    private void addTupleToFrame(ArrayTupleBuilder tb) throws HyracksDataException {
        if (tuplesInFrame == batchSize
                || !appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("flushing frame containg (" + tuplesInFrame + ") tuples");
            }
            FrameUtils.flushFrame(frame.getBuffer(), writer);
            tuplesInFrame = 0;
            appender.reset(frame, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            if (activeTimer) {
                synchronized (lock) {
                    FrameUtils.flushFrame(frame.getBuffer(), writer);
                }
            } else {
                FrameUtils.flushFrame(frame.getBuffer(), writer);
            }
        }

        if (timer != null) {
            timer.cancel();
        }
    }

    private class TimeBasedFlushTask extends TimerTask {

        private IFrameWriter writer;
        private final Object lock;

        public TimeBasedFlushTask(IFrameWriter writer, Object lock) {
            this.writer = writer;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                if (tuplesInFrame > 0) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("TTL expired flushing frame (" + tuplesInFrame + ")");
                    }
                    synchronized (lock) {
                        FrameUtils.flushFrame(frame.getBuffer(), writer);
                        appender.reset(frame, true);
                        tuplesInFrame = 0;
                    }
                }
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

    }
}