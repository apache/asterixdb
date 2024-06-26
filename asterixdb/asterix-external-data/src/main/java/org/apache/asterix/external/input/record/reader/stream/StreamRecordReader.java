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
package org.apache.asterix.external.input.record.reader.stream;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IStreamNotificationHandler;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.stream.AsterixInputStreamReader;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.IFeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class StreamRecordReader extends AbstractStreamRecordReader<char[]>
        implements IStreamNotificationHandler {
    protected AsterixInputStreamReader reader;
    protected CharArrayRecord record;
    protected char[] inputBuffer;
    protected int bufferLength = 0;
    protected int bufferPosn = 0;
    protected boolean done = false;
    protected IFeedLogManager feedLogManager;

    protected void configure(AsterixInputStream inputStream, Map<String, String> config) {
        int bufferSize = ExternalDataUtils.getOrDefaultBufferSize(config);
        this.reader = new AsterixInputStreamReader(inputStream, bufferSize);
        record = new CharArrayRecord();
        inputBuffer = new char[bufferSize];
        setSuppliers(config, reader::getStreamName, reader::getPreviousStreamName);
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        return record;
    }

    @Override
    public void close() throws IOException {
        try {
            if (!done) {
                reader.close();
            }
        } finally {
            done = true;
        }
    }

    @Override
    public boolean stop() {
        try {
            reader.stop();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void setFeedLogManager(IFeedLogManager feedLogManager) throws HyracksDataException {
        this.feedLogManager = feedLogManager;
        reader.setFeedLogManager(feedLogManager);
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        reader.setController(controller);
    }

    @Override
    public boolean handleException(Throwable th) {
        return reader.handleException(th);
    }

    @Override
    public void notifyNewSource() {
        throw new UnsupportedOperationException();
    }

    protected void resetForNewSource() {
        record.reset();
    }

    public abstract String getRequiredConfigs();
}