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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordFlowController;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.util.ExternalDataExceptionUtils;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FeedRecordDataFlowController<T> extends AbstractFeedDataFlowController
        implements IRecordFlowController<T> {
    protected IRecordDataParser<T> dataParser;
    protected IRecordReader<? extends T> recordReader;
    protected long interval;
    protected AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void start(IFrameWriter writer) throws HyracksDataException {
        HyracksDataException hde = null;
        try {
            initializeTupleForwarder(writer);
            while (recordReader.hasNext()) {
                IRawRecord<? extends T> record = recordReader.next();
                if (record == null) {
                    Thread.sleep(interval);
                    continue;
                }
                tb.reset();
                dataParser.parse(record, tb.getDataOutput());
                tb.addFieldEndOffset();
                tupleForwarder.addTuple(tb);
            }
        } catch (Throwable th) {
            hde = new HyracksDataException(th);
        }
        try {
            tupleForwarder.close();
        } catch (Throwable th) {
            hde = ExternalDataExceptionUtils.suppress(hde, th);
        }
        try {
            recordReader.close();
        } catch (Throwable th) {
            hde = ExternalDataExceptionUtils.suppress(hde, th);
            throw hde;
        } finally {
            closeSignal();
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
        if (recordReader.stop()) {
            try {
                waitForSignal();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean handleException(Throwable th) {
        return true;
    }

    @Override
    public void setRecordParser(IRecordDataParser<T> dataParser) {
        this.dataParser = dataParser;
    }

    @Override
    public void setRecordReader(IRecordReader<T> recordReader) {
        this.recordReader = recordReader;
    }
}
