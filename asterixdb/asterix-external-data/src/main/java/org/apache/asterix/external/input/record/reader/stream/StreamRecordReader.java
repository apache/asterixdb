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
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IStreamNotificationHandler;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.stream.AsterixInputStreamReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class StreamRecordReader implements IRecordReader<char[]>, IStreamNotificationHandler {
    protected AsterixInputStreamReader reader;
    protected CharArrayRecord record;
    protected char[] inputBuffer;
    protected int bufferLength = 0;
    protected int bufferPosn = 0;
    protected boolean done = false;
    protected FeedLogManager feedLogManager;

    public void configure(AsterixInputStream inputStream) {
        this.reader = new AsterixInputStreamReader(inputStream);
        record = new CharArrayRecord();
        inputBuffer = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        return record;
    }

    @Override
    public void close() throws IOException {
        if (!done) {
            reader.close();
        }
        done = true;
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
    public abstract boolean hasNext() throws IOException;

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) throws HyracksDataException {
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

    public abstract List<String> getRecordReaderFormats();

    public abstract String getRequiredConfigs();

    public abstract void configure(AsterixInputStream inputStream, Map<String, String> config)
            throws HyracksDataException;
}
