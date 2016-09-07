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

import java.util.Map;

import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.provider.StreamRecordReaderProvider;
import org.apache.asterix.external.provider.StreamRecordReaderProvider.Format;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class StreamRecordReaderFactory implements IRecordReaderFactory<char[]> {

    private static final long serialVersionUID = 1L;
    protected final IInputStreamFactory streamFactory;
    protected Map<String, String> configuration;
    protected Format format;

    public StreamRecordReaderFactory(IInputStreamFactory inputStreamFactory) {
        this.streamFactory = inputStreamFactory;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public Class<?> getRecordClass() {
        return char[].class;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint()
            throws HyracksDataException, AlgebricksException {
        return streamFactory.getPartitionConstraint();
    }

    @Override
    public void configure(Map<String, String> configuration) throws HyracksDataException, AlgebricksException {
        this.configuration = configuration;
        streamFactory.configure(configuration);
        format = StreamRecordReaderProvider.getReaderFormat(configuration);
    }

    @Override
    public IRecordReader<? extends char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        return StreamRecordReaderProvider.createRecordReader(format, streamFactory.createInputStream(ctx, partition),
                configuration);
    }
}
