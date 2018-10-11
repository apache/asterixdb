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

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.stream.factory.LocalFSInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketClientInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketServerInputStreamFactory;
import org.apache.asterix.external.provider.StreamRecordReaderProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class StreamRecordReaderFactory implements IRecordReaderFactory<char[]> {

    private static final long serialVersionUID = 1L;
    protected IInputStreamFactory streamFactory;
    protected Map<String, String> configuration;
    protected Class recordReaderClazz;
    private static final List<String> recordReaderNames =
            Collections.unmodifiableList(Arrays.asList(ExternalDataConstants.ALIAS_LOCALFS_ADAPTER,
                    ExternalDataConstants.KEY_ALIAS_ADAPTER_NAME_SOCKET, ExternalDataConstants.KEY_ADAPTER_NAME_SOCKET,
                    ExternalDataConstants.STREAM_SOCKET_CLIENT));

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public Class<?> getRecordClass() {
        return char[].class;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        return streamFactory.getPartitionConstraint();
    }

    private void configureInputStreamFactory(Map<String, String> config) throws CompilationException {
        String reader = config.get(ExternalDataConstants.KEY_READER);
        if (reader.equals(ExternalDataConstants.ALIAS_LOCALFS_ADAPTER)) {
            streamFactory = new LocalFSInputStreamFactory();
        } else if (reader.equals(ExternalDataConstants.KEY_ALIAS_ADAPTER_NAME_SOCKET)
                || reader.equals(ExternalDataConstants.KEY_ADAPTER_NAME_SOCKET)) {
            streamFactory = new SocketServerInputStreamFactory();
        } else if (reader.equals(ExternalDataConstants.STREAM_SOCKET_CLIENT)) {
            streamFactory = new SocketClientInputStreamFactory();
        } else {
            throw new CompilationException(ErrorCode.FEED_UNKNOWN_ADAPTER_NAME);
        }
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration)
            throws HyracksDataException, AlgebricksException {
        this.configuration = configuration;
        configureInputStreamFactory(configuration);
        streamFactory.configure(serviceCtx, configuration);
        recordReaderClazz = StreamRecordReaderProvider.getRecordReaderClazz(configuration);
    }

    @Override
    public IRecordReader<? extends char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        try {
            StreamRecordReader streamRecordReader =
                    (StreamRecordReader) recordReaderClazz.getConstructor().newInstance();
            streamRecordReader.configure(streamFactory.createInputStream(ctx, partition), configuration);
            return streamRecordReader;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }
}
