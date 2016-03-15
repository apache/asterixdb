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

import org.apache.asterix.external.api.IDataFlowController;
import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.stream.AInputStream;
import org.apache.asterix.external.input.stream.AInputStreamReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;

public abstract class AbstractStreamRecordReader implements IRecordReader<char[]>, IIndexingDatasource {
    protected final AInputStreamReader reader;
    protected CharArrayRecord record;
    protected char[] inputBuffer;
    protected int bufferLength = 0;
    protected int bufferPosn = 0;
    protected final IExternalIndexer indexer;
    protected boolean done = false;
    protected FeedLogManager feedLogManager;

    public AbstractStreamRecordReader(AInputStream inputStream, IExternalIndexer indexer) {
        this.reader = new AInputStreamReader(inputStream);
        this.indexer = indexer;
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
    public IExternalIndexer getIndexer() {
        return indexer;
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
    public void setController(IDataFlowController controller) {
        reader.setController((AbstractFeedDataFlowController) controller);
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
        this.feedLogManager = feedLogManager;
        reader.setFeedLogManager(feedLogManager);
    }
}
