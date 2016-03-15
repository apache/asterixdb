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

import javax.annotation.Nonnull;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataExceptionUtils;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.log4j.Logger;

public class FeedRecordDataFlowController<T> extends AbstractFeedDataFlowController {
    private static final Logger LOGGER = Logger.getLogger(FeedRecordDataFlowController.class.getName());
    protected final IRecordDataParser<T> dataParser;
    protected final IRecordReader<? extends T> recordReader;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected final long interval = 1000;
    protected boolean failed = false;

    public FeedRecordDataFlowController(IHyracksTaskContext ctx, FeedTupleForwarder tupleForwarder,
            @Nonnull FeedLogManager feedLogManager, int numOfOutputFields, @Nonnull IRecordDataParser<T> dataParser,
            @Nonnull IRecordReader<T> recordReader) {
        super(ctx, tupleForwarder, feedLogManager, numOfOutputFields);
        this.dataParser = dataParser;
        this.recordReader = recordReader;
        recordReader.setFeedLogManager(feedLogManager);
        recordReader.setController(this);
    }

    @Override
    public void start(IFrameWriter writer) throws HyracksDataException {
        HyracksDataException hde = null;
        try {
            failed = false;
            tupleForwarder.initialize(ctx, writer);
            while (recordReader.hasNext()) {
                IRawRecord<? extends T> record = recordReader.next();
                if (record == null) {
                    flush();
                    Thread.sleep(interval);
                    continue;
                }
                tb.reset();
                dataParser.parse(record, tb.getDataOutput());
                tb.addFieldEndOffset();
                addMetaPart(tb, record);
                addPrimaryKeys(tb, record);
                if (tb.getSize() > tupleForwarder.getMaxRecordSize()) {
                    // log
                    feedLogManager.logRecord(record.toString(), ExternalDataConstants.LARGE_RECORD_ERROR_MESSAGE);
                    continue;
                }
                tupleForwarder.addTuple(tb);
            }
        } catch (Throwable th) {
            failed = true;
            tupleForwarder.flush();
            LOGGER.warn("Failure during while operating a feed source", th);
            throw new HyracksDataException(th);
        }
        try {
            tupleForwarder.close();
        } catch (Throwable th) {
            hde = ExternalDataExceptionUtils.suppress(hde, th);
        }
        try {
            recordReader.close();
        } catch (Throwable th) {
            LOGGER.warn("Failure during while operating a feed sourcec", th);
            hde = ExternalDataExceptionUtils.suppress(hde, th);
        } finally {
            closeSignal();
            if (hde != null) {
                throw hde;
            }
        }
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

    private void waitForSignal() throws InterruptedException {
        synchronized (closed) {
            while (!closed.get()) {
                closed.wait();
            }
        }
    }

    @Override
    public boolean stop() throws HyracksDataException {
        HyracksDataException hde = null;
        if (recordReader.stop()) {
            if (failed) {
                // failed, close here
                try {
                    tupleForwarder.close();
                } catch (Throwable th) {
                    hde = ExternalDataExceptionUtils.suppress(hde, th);
                }
                try {
                    recordReader.close();
                } catch (Throwable th) {
                    hde = ExternalDataExceptionUtils.suppress(hde, th);
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
        return true;
    }
}
