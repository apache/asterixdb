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

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.stream.AsterixInputStreamReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

public abstract class AbstractStreamRecordReader implements IRecordReader<char[]>, IIndexingDatasource {
    protected final AsterixInputStreamReader reader;
    protected CharArrayRecord record;
    protected char[] inputBuffer;
    protected int bufferLength = 0;
    protected int bufferPosn = 0;
    protected final IExternalIndexer indexer;
    protected boolean done = false;
    protected FeedLogManager feedLogManager;

    public AbstractStreamRecordReader(AsterixInputStream inputStream, IExternalIndexer indexer) {
        this.reader = new AsterixInputStreamReader(inputStream);
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
    public void setController(AbstractFeedDataFlowController controller) {
        reader.setController(controller);
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
        this.feedLogManager = feedLogManager;
        reader.setFeedLogManager(feedLogManager);
    }

    @Override
    public boolean handleException(Throwable th) {
        return reader.handleException(th);
    }

    //TODO: Fix the following method since they don't fit
    //Already the fix is in another local branch
    @Override
    public List<ExternalFile> getSnapshot() {
        return null;
    }

    @Override
    public int getCurrentSplitIndex() {
        return -1;
    }

    @Override
    public RecordReader<?, Writable> getReader() {
        return null;
    }
}
