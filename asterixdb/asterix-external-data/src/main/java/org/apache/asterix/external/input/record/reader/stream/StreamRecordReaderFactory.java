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
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.stream.factory.LocalFSInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketClientInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketServerInputStreamFactory;
import org.apache.asterix.external.provider.StreamRecordReaderProvider;
import org.apache.asterix.external.provider.context.ExternalReaderRuntimeDataContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class StreamRecordReaderFactory implements IRecordReaderFactory<char[]> {

    private static final long serialVersionUID = 1L;
    protected IInputStreamFactory streamFactory;
    protected Map<String, String> configuration;
    protected Class recordReaderClazz;
    protected IExternalFilterEvaluatorFactory filterEvaluatorFactory;
    private static final List<String> recordReaderNames =
            Collections.unmodifiableList(Arrays.asList(ExternalDataConstants.ALIAS_LOCALFS_ADAPTER,
                    ExternalDataConstants.KEY_ALIAS_ADAPTER_NAME_SOCKET, ExternalDataConstants.KEY_ADAPTER_NAME_SOCKET,
                    ExternalDataConstants.STREAM_SOCKET_CLIENT));

    @Override
    public final DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public final Class<?> getRecordClass() {
        return char[].class;
    }

    @Override
    public final AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        return streamFactory.getPartitionConstraint();
    }

    @Override
    public final void configure(IServiceContext serviceCtx, Map<String, String> configuration,
            IWarningCollector warningCollector, IExternalFilterEvaluatorFactory filterEvaluatorFactory)
            throws HyracksDataException, AlgebricksException {
        this.configuration = configuration;
        setStreamFactory(configuration);
        streamFactory.configure(serviceCtx, configuration, warningCollector, filterEvaluatorFactory);
        recordReaderClazz = StreamRecordReaderProvider.getRecordReaderClazz(configuration);
        this.filterEvaluatorFactory = filterEvaluatorFactory;
    }

    @Override
    public final IRecordReader<? extends char[]> createRecordReader(IExternalDataRuntimeContext context)
            throws HyracksDataException {
        StreamRecordReader reader = createReader(context);
        ((ExternalReaderRuntimeDataContext) context).setReader(reader);
        return reader;
    }

    @Override
    public final IExternalDataRuntimeContext createExternalDataRuntimeContext(IHyracksTaskContext context,
            int partition) throws HyracksDataException {
        IExternalFilterValueEmbedder valueEmbedder =
                filterEvaluatorFactory.createValueEmbedder(context.getWarningCollector());
        return new ExternalReaderRuntimeDataContext(context, partition, valueEmbedder);
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    protected void setStreamFactory(Map<String, String> config) throws CompilationException {
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

    @SuppressWarnings("unchecked")
    private StreamRecordReader createReader(IExternalDataRuntimeContext context) throws HyracksDataException {
        try {
            StreamRecordReader streamRecordReader =
                    (StreamRecordReader) recordReaderClazz.getConstructor().newInstance();
            streamRecordReader.configure(context.getTaskContext(), streamFactory.createInputStream(context),
                    configuration);
            return streamRecordReader;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw HyracksDataException.create(e);
        }
    }
}
