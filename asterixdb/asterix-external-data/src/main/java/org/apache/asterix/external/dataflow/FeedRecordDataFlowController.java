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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class FeedRecordDataFlowController<T> extends AbstractFeedDataFlowController {
    public static final String INCOMING_RECORDS_COUNT_FIELD_NAME = "incoming-records-count";
    public static final String FAILED_AT_PARSER_RECORDS_COUNT_FIELD_NAME = "failed-at-parser-records-count";

    public enum State {
        CREATED,
        STARTED,
        STOPPED
    }

    private static final Logger LOGGER = Logger.getLogger(FeedRecordDataFlowController.class.getName());
    private final IRecordDataParser<T> dataParser;
    private final IRecordReader<T> recordReader;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected static final long INTERVAL = 1000;
    protected State state = State.CREATED;
    protected long incomingRecordsCount = 0;
    protected long failedRecordsCount = 0;

    public FeedRecordDataFlowController(IHyracksTaskContext ctx, FeedTupleForwarder tupleForwarder,
            FeedLogManager feedLogManager, int numOfOutputFields, IRecordDataParser<T> dataParser,
            IRecordReader<T> recordReader) throws HyracksDataException {
        super(ctx, tupleForwarder, feedLogManager, numOfOutputFields);
        this.dataParser = dataParser;
        this.recordReader = recordReader;
        recordReader.setFeedLogManager(feedLogManager);
        recordReader.setController(this);
    }

    @Override
    public void start(IFrameWriter writer) throws HyracksDataException, InterruptedException {
        synchronized (this) {
            if (state == State.STOPPED) {
                return;
            } else {
                setState(State.STARTED);
            }
        }
        Exception failure = null;
        try {
            tupleForwarder.initialize(ctx, writer);
            while (hasNext()) {
                IRawRecord<? extends T> record = next();
                if (record == null) {
                    flush();
                    Thread.sleep(INTERVAL); // NOSONAR: No one notifies the sleeping thread
                    continue;
                }
                tb.reset();
                incomingRecordsCount++;
                if (!parseAndForward(record)) {
                    failedRecordsCount++;
                }
            }
        } catch (HyracksDataException e) {
            LOGGER.log(Level.WARNING, "Exception during ingestion", e);
            //if interrupted while waiting for a new record, then it is safe to not fail forward
            if (e.getComponent() == ErrorCode.ASTERIX
                    && (e.getErrorCode() == ErrorCode.FEED_STOPPED_WHILE_WAITING_FOR_A_NEW_RECORD)) {
                // Do nothing. interrupted by the active manager
            } else if (e.getComponent() == ErrorCode.ASTERIX
                    && (e.getErrorCode() == ErrorCode.FEED_FAILED_WHILE_GETTING_A_NEW_RECORD)) {
                // Failure but we know we can for sure push the previously parsed records safely
                failure = e;
                try {
                    flush();
                } catch (Exception flushException) {
                    tupleForwarder.fail();
                    flushException.addSuppressed(e);
                    failure = flushException;
                }
            } else {
                failure = e;
                tupleForwarder.fail();
            }
        } catch (Exception e) {
            failure = e;
            tupleForwarder.fail();
            LOGGER.log(Level.WARNING, "Failure while operating a feed source", e);
        } finally {
            failure = finish(failure);
        }
        if (failure != null) {
            if (failure instanceof InterruptedException) {
                throw (InterruptedException) failure;
            }
            throw HyracksDataException.create(failure);
        }
    }

    private synchronized void setState(State newState) {
        LOGGER.log(Level.INFO, "State is being set from " + state + " to " + newState);
        state = newState;
    }

    public synchronized State getState() {
        return state;
    }

    private IRawRecord<? extends T> next() throws Exception {
        try {
            return recordReader.next();
        } catch (InterruptedException e) { // NOSONAR Gracefully handling interrupt to push records in the pipeline
            if (flushing) {
                throw e;
            }
            throw new RuntimeDataException(ErrorCode.FEED_STOPPED_WHILE_WAITING_FOR_A_NEW_RECORD, e);
        } catch (Exception e) {
            if (flushing) {
                throw e;
            }
            if (!recordReader.handleException(e)) {
                throw new RuntimeDataException(ErrorCode.FEED_FAILED_WHILE_GETTING_A_NEW_RECORD, e);
            }
            return null;
        }
    }

    private boolean hasNext() throws Exception {
        while (true) {
            try {
                return recordReader.hasNext();
            } catch (InterruptedException e) { // NOSONAR Gracefully handling interrupt to push records in the pipeline
                if (flushing) {
                    throw e;
                }
                throw new RuntimeDataException(ErrorCode.FEED_STOPPED_WHILE_WAITING_FOR_A_NEW_RECORD, e);
            } catch (Exception e) {
                if (flushing) {
                    throw e;
                }
                if (!recordReader.handleException(e)) {
                    throw new RuntimeDataException(ErrorCode.FEED_FAILED_WHILE_GETTING_A_NEW_RECORD, e);
                }
            }
        }
    }

    private Exception finish(Exception failure) {
        HyracksDataException hde = null;
        try {
            recordReader.close();
        } catch (Exception th) {
            LOGGER.log(Level.WARNING, "Failure during while operating a feed source", th);
            hde = HyracksDataException.suppress(hde, th);
        }
        try {
            tupleForwarder.close();
        } catch (Exception th) {
            hde = HyracksDataException.suppress(hde, th);
        } finally {
            closeSignal();
        }
        setState(State.STOPPED);
        if (hde != null) {
            if (failure != null) {
                failure.addSuppressed(hde);
            } else {
                return hde;
            }
        }
        return failure;
    }

    private boolean parseAndForward(IRawRecord<? extends T> record) throws IOException {
        try {
            dataParser.parse(record, tb.getDataOutput());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, ExternalDataConstants.ERROR_PARSE_RECORD, e);
            feedLogManager.logRecord(record.toString(), ExternalDataConstants.ERROR_PARSE_RECORD);
            // continue the outer loop
            return false;
        }
        tb.addFieldEndOffset();
        addMetaPart(tb, record);
        addPrimaryKeys(tb, record);
        tupleForwarder.addTuple(tb);
        return true;
    }

    protected void addMetaPart(ArrayTupleBuilder tb, IRawRecord<? extends T> record) throws IOException {
    }

    protected void addPrimaryKeys(ArrayTupleBuilder tb, IRawRecord<? extends T> record) throws IOException {
    }

    private void closeSignal() {
        synchronized (closed) {
            closed.set(true);
            closed.notifyAll();
        }
    }

    private void waitForSignal(long timeout) throws InterruptedException, HyracksDataException {
        if (timeout <= 0) {
            throw new IllegalArgumentException("timeout must be greater than 0");
        }
        synchronized (closed) {
            while (!closed.get()) {
                long before = System.currentTimeMillis();
                closed.wait(timeout);
                timeout -= System.currentTimeMillis() - before;
                if (!closed.get() && timeout <= 0) {
                    throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.TIMEOUT);
                }
            }
        }
    }

    @Override
    public boolean stop(long timeout) throws HyracksDataException {
        synchronized (this) {
            switch (state) {
                case CREATED:
                case STOPPED:
                    setState(State.STOPPED);
                    return true;
                case STARTED:
                    break;
                default:
                    throw new HyracksDataException("unknown state " + state);

            }
        }
        if (recordReader.stop()) {
            try {
                waitForSignal(timeout);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
            return true;
        }
        return false;
    }

    public IRecordReader<T> getReader() {
        return recordReader;
    }

    public IRecordDataParser<T> getParser() {
        return dataParser;
    }

    @Override
    public String getStats() {
        return "{\"" + INCOMING_RECORDS_COUNT_FIELD_NAME + "\": " + incomingRecordsCount + ", \"" +
                FAILED_AT_PARSER_RECORDS_COUNT_FIELD_NAME + "\": " + failedRecordsCount + "}";
    }
}
