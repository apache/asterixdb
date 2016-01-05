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
import java.util.Map;

import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.stream.AInputStream;
import org.apache.asterix.external.input.stream.AInputStreamReader;
import org.apache.asterix.external.util.ExternalDataConstants;

public abstract class AbstractStreamRecordReader implements IRecordReader<char[]>, IIndexingDatasource {
    protected AInputStreamReader reader;
    protected CharArrayRecord record;
    protected char[] inputBuffer;
    protected int bufferLength = 0;
    protected int bufferPosn = 0;
    protected IExternalIndexer indexer;

    @Override
    public IRawRecord<char[]> next() throws IOException {
        return record;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public void setInputStream(AInputStream inputStream) throws IOException {
        this.reader = new AInputStreamReader(inputStream);
    }

    @Override
    public Class<char[]> getRecordClass() {
        return char[].class;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        record = new CharArrayRecord();
        inputBuffer = new char[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
    }

    @Override
    public IExternalIndexer getIndexer() {
        return indexer;
    }

    @Override
    public void setIndexer(IExternalIndexer indexer) {
        this.indexer = indexer;
    }
}
