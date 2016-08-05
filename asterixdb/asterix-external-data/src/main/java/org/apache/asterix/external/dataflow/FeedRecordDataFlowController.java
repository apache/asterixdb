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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.asterix.external.api.IFeedMarker;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataExceptionUtils;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.common.util.TaskUtils;
import org.apache.log4j.Logger;

public class FeedRecordDataFlowController<T> extends AbstractFeedDataFlowController {
    private static final Logger LOGGER = Logger.getLogger(FeedRecordDataFlowController.class.getName());
    protected final IRecordDataParser<T> dataParser;
    protected final IRecordReader<? extends T> recordReader;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected static final long INTERVAL = 1000;
    protected final Object mutex = new Object();
    protected final boolean sendMarker;
    protected boolean failed = false;
    private FeedRecordDataFlowController<T>.DataflowMarker dataflowMarker;
    private Future<?> dataflowMarkerResult;

    public FeedRecordDataFlowController(IHyracksTaskContext ctx, FeedTupleForwarder tupleForwarder,
            @Nonnull FeedLogManager feedLogManager, int numOfOutputFields, @Nonnull IRecordDataParser<T> dataParser,
            @Nonnull IRecordReader<T> recordReader, boolean sendMarker) throws HyracksDataException {
        super(ctx, tupleForwarder, feedLogManager, numOfOutputFields);
        this.dataParser = dataParser;
        this.recordReader = recordReader;
        this.sendMarker = sendMarker;
        recordReader.setFeedLogManager(feedLogManager);
        recordReader.setController(this);
    }

    @Override
    public void start(IFrameWriter writer) throws HyracksDataException {
        startDataflowMarker();
        HyracksDataException hde = null;
        try {
            failed = false;
            tupleForwarder.initialize(ctx, writer);
            while (recordReader.hasNext()) {
                // synchronized on mutex before we call next() so we don't a marker before its record
                synchronized (mutex) {
                    IRawRecord<? extends T> record = recordReader.next();
                    if (record == null) {
                        flush();
                        mutex.wait(INTERVAL);
                        continue;
                    }
                    tb.reset();
                    parseAndForward(record);
                }
            }
        } catch (InterruptedException e) {
            //TODO: Find out what could cause an interrupted exception beside termination of a job/feed
            LOGGER.warn("Feed has been interrupted. Closing the feed", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            failed = true;
            tupleForwarder.flush();
            LOGGER.warn("Failure while operating a feed source", e);
            throw new HyracksDataException(e);
        }
        stopDataflowMarker();
        try {
            tupleForwarder.close();
        } catch (Throwable th) {
            hde = ExternalDataExceptionUtils.suppressIntoHyracksDataException(hde, th);
        }
        try {
            recordReader.close();
        } catch (Throwable th) {
            LOGGER.warn("Failure during while operating a feed sourcec", th);
            hde = ExternalDataExceptionUtils.suppressIntoHyracksDataException(hde, th);
        } finally {
            closeSignal();
            if (sendMarker && dataflowMarkerResult != null) {
                dataflowMarkerResult.cancel(true);
            }
        }
        if (hde != null) {
            throw hde;
        }
    }

    private void parseAndForward(IRawRecord<? extends T> record) throws IOException {
        synchronized (dataParser) {
            try {
                dataParser.parse(record, tb.getDataOutput());
            } catch (Exception e) {
                LOGGER.warn(ExternalDataConstants.ERROR_PARSE_RECORD, e);
                feedLogManager.logRecord(record.toString(), ExternalDataConstants.ERROR_PARSE_RECORD);
                // continue the outer loop
                return;
            }
            tb.addFieldEndOffset();
            addMetaPart(tb, record);
            addPrimaryKeys(tb, record);
            tupleForwarder.addTuple(tb);
        }
    }

    protected void addMetaPart(ArrayTupleBuilder tb, IRawRecord<? extends T> record) throws IOException {
    }

    protected void addPrimaryKeys(ArrayTupleBuilder tb, IRawRecord<? extends T> record) throws IOException {
    }

    private void startDataflowMarker() {
        ExecutorService executorService = sendMarker ? Executors.newSingleThreadExecutor() : null;
        if (sendMarker && dataflowMarker == null) {
            dataflowMarker = new DataflowMarker(recordReader.getProgressReporter(),
                    TaskUtils.<VSizeFrame> get(HyracksConstants.KEY_MESSAGE, ctx));
            dataflowMarkerResult = executorService.submit(dataflowMarker);
        }
    }

    private void stopDataflowMarker() {
        if (dataflowMarker != null) {
            dataflowMarker.stop();
        }
    }

    private void closeSignal() {
        synchronized (closed) {
            closed.set(true);
            closed.notifyAll();
        }
    }

    private void waitForSignal() throws InterruptedException {
        synchronized (closed) {
            while (!closed.get()) {
                closed.wait();
            }
        }
    }

    @Override
    public boolean stop() throws HyracksDataException {
        stopDataflowMarker();
        HyracksDataException hde = null;
        if (recordReader.stop()) {
            if (failed) {
                // failed, close here
                try {
                    tupleForwarder.close();
                } catch (Throwable th) {
                    hde = ExternalDataExceptionUtils.suppressIntoHyracksDataException(hde, th);
                }
                try {
                    recordReader.close();
                } catch (Throwable th) {
                    hde = ExternalDataExceptionUtils.suppressIntoHyracksDataException(hde, th);
                }
                if (hde != null) {
                    throw hde;
                }
            } else {
                try {
                    waitForSignal();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean handleException(Throwable th) {
        // This is not a parser record. most likely, this error happened in the record reader.
        return recordReader.handleException(th);
    }

    private class DataflowMarker implements Runnable {
        private final IFeedMarker marker;
        private final VSizeFrame mark;
        private volatile boolean stopped = false;

        public DataflowMarker(IFeedMarker marker, VSizeFrame mark) {
            this.marker = marker;
            this.mark = mark;
        }

        public synchronized void stop() {
            stopped = true;
            notify();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    synchronized (this) {
                        if (!stopped) {
                            // TODO (amoudi): find a better reactive way to do this
                            // sleep for two seconds
                            wait(TimeUnit.SECONDS.toMillis(2));
                        } else {
                            break;
                        }
                    }
                    synchronized (mutex) {
                        if (marker.mark(mark)) {
                            // broadcast
                            tupleForwarder.flush();
                            // clear
                            mark.getBuffer().clear();
                            mark.getBuffer().put(MessagingFrameTupleAppender.NULL_FEED_MESSAGE);
                            mark.getBuffer().flip();
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Marker stopped", e);
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOGGER.warn("Marker stopped", e);
            }
        }
    }
}
