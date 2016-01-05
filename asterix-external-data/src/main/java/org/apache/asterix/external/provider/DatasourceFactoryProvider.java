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
package org.apache.asterix.external.provider;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IInputStreamProviderFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.HDFSDataSourceFactory;
import org.apache.asterix.external.input.record.reader.factory.LineRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.factory.SemiStructuredRecordReaderFactory;
import org.apache.asterix.external.input.stream.factory.LocalFSInputStreamProviderFactory;
import org.apache.asterix.external.input.stream.factory.SocketInputStreamProviderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;

public class DatasourceFactoryProvider {

    public static IExternalDataSourceFactory getExternalDataSourceFactory(Map<String, String> configuration)
            throws Exception {
        switch (ExternalDataUtils.getDataSourceType(configuration)) {
            case RECORDS:
                return DatasourceFactoryProvider.getRecordReaderFactory(configuration);
            case STREAM:
                return DatasourceFactoryProvider
                        .getInputStreamFactory(configuration.get(ExternalDataConstants.KEY_STREAM), configuration);
        }
        return null;
    }

    public static IInputStreamProviderFactory getInputStreamFactory(String stream, Map<String, String> configuration)
            throws Exception {
        IInputStreamProviderFactory streamFactory;
        if (ExternalDataUtils.isExternal(stream)) {
            String dataverse = ExternalDataUtils.getDataverse(configuration);
            streamFactory = ExternalDataUtils.createExternalInputStreamFactory(dataverse, stream);
        } else {
            switch (stream) {
                case ExternalDataConstants.STREAM_HDFS:
                    streamFactory = new HDFSDataSourceFactory();
                    break;
                case ExternalDataConstants.STREAM_LOCAL_FILESYSTEM:
                    streamFactory = new LocalFSInputStreamProviderFactory();
                    break;
                case ExternalDataConstants.STREAM_SOCKET:
                    streamFactory = new SocketInputStreamProviderFactory();
                    break;
                default:
                    throw new AsterixException("unknown input stream factory");
            }
        }
        return streamFactory;
    }

    public static IRecordReaderFactory<?> getRecordReaderFactory(Map<String, String> configuration) throws Exception {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        IRecordReaderFactory<?> readerFactory;
        if (ExternalDataUtils.isExternal(reader)) {
            String dataverse = ExternalDataUtils.getDataverse(configuration);
            readerFactory = ExternalDataUtils.createExternalRecordReaderFactory(dataverse, reader);
        } else {
            switch (reader) {
                case ExternalDataConstants.READER_HDFS:
                    readerFactory = new HDFSDataSourceFactory();
                    break;
                case ExternalDataConstants.READER_ADM:
                case ExternalDataConstants.READER_SEMISTRUCTURED:
                    readerFactory = new SemiStructuredRecordReaderFactory()
                            .setInputStreamFactoryProvider(DatasourceFactoryProvider.getInputStreamFactory(
                                    ExternalDataUtils.getRecordReaderStreamName(configuration), configuration));
                    break;
                case ExternalDataConstants.READER_DELIMITED:
                    readerFactory = new LineRecordReaderFactory()
                            .setInputStreamFactoryProvider(DatasourceFactoryProvider.getInputStreamFactory(
                                    ExternalDataUtils.getRecordReaderStreamName(configuration), configuration));;
                    break;
                default:
                    throw new AsterixException("unknown input stream factory");
            }
        }
        return readerFactory;
    }
}
