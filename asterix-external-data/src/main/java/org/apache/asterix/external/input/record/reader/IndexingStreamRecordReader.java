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
package org.apache.asterix.external.input.record.reader;

import java.io.IOException;
import java.util.List;

import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.input.record.reader.stream.StreamRecordReader;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class IndexingStreamRecordReader implements IRecordReader<char[]>, IIndexingDatasource {

    private StreamRecordReader reader;
    private IExternalIndexer indexer;

    public IndexingStreamRecordReader(StreamRecordReader reader, IExternalIndexer indexer) {
        this.reader = reader;
        this.indexer = indexer;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public IExternalIndexer getIndexer() {
        return indexer;
    }

    @Override
    public boolean hasNext() throws Exception {
        return reader.hasNext();
    }

    @Override
    public IRawRecord<char[]> next() throws IOException, InterruptedException {
        return reader.next();
    }

    @Override
    public boolean stop() {
        return reader.stop();
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        reader.setController(controller);
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) throws HyracksDataException {
        reader.setFeedLogManager(feedLogManager);
    }

    @Override
    public List<ExternalFile> getSnapshot() {
        return null;
    }

    @Override
    public int getCurrentSplitIndex() {
        return -1;
    }

    @Override
    public RecordReader<?, ? extends Writable> getReader() {
        return null;
    }

    @Override
    public boolean handleException(Throwable th) {
        return reader.handleException(th);
    }

}
