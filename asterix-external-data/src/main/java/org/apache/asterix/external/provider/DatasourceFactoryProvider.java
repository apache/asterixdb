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
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.HDFSDataSourceFactory;
import org.apache.asterix.external.input.record.reader.RecordWithPKTestReaderFactory;
import org.apache.asterix.external.input.record.reader.kv.KVReaderFactory;
import org.apache.asterix.external.input.record.reader.kv.KVTestReaderFactory;
import org.apache.asterix.external.input.record.reader.stream.EmptyLineSeparatedRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.stream.LineRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.stream.SemiStructuredRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.twitter.TwitterRecordReaderFactory;
import org.apache.asterix.external.input.stream.factory.LocalFSInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketServerInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.TwitterFirehoseStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;

public class DatasourceFactoryProvider {

    public static IExternalDataSourceFactory getExternalDataSourceFactory(Map<String, String> configuration)
            throws AsterixException {
        if (ExternalDataUtils.getDataSourceType(configuration).equals(DataSourceType.RECORDS)) {
            String reader = configuration.get(ExternalDataConstants.KEY_READER);
            return DatasourceFactoryProvider.getRecordReaderFactory(reader, configuration);
        } else {
            // get stream source
            String streamSource = configuration.get(ExternalDataConstants.KEY_STREAM_SOURCE);
            return DatasourceFactoryProvider.getInputStreamFactory(streamSource, configuration);
        }
    }

    public static IInputStreamFactory getInputStreamFactory(String streamSource,
            Map<String, String> configuration) throws AsterixException {
        IInputStreamFactory streamSourceFactory;
        if (ExternalDataUtils.isExternal(streamSource)) {
            String dataverse = ExternalDataUtils.getDataverse(configuration);
            streamSourceFactory = ExternalDataUtils.createExternalInputStreamFactory(dataverse, streamSource);
        } else {
            switch (streamSource) {
                case ExternalDataConstants.STREAM_HDFS:
                    streamSourceFactory = new HDFSDataSourceFactory();
                    break;
                case ExternalDataConstants.STREAM_LOCAL_FILESYSTEM:
                    streamSourceFactory = new LocalFSInputStreamFactory();
                    break;
                case ExternalDataConstants.STREAM_SOCKET:
                case ExternalDataConstants.ALIAS_SOCKET_ADAPTER:
                    streamSourceFactory = new SocketServerInputStreamFactory();
                    break;
                case ExternalDataConstants.STREAM_SOCKET_CLIENT:
                    streamSourceFactory = new SocketServerInputStreamFactory();
                    break;
                case ExternalDataConstants.ALIAS_TWITTER_FIREHOSE_ADAPTER:
                    streamSourceFactory = new TwitterFirehoseStreamFactory();
                    break;
                default:
                    throw new AsterixException("unknown input stream factory");
            }
        }
        return streamSourceFactory;
    }

    public static IRecordReaderFactory<?> getRecordReaderFactory(String reader, Map<String, String> configuration)
            throws AsterixException {
        if (reader.equals(ExternalDataConstants.EXTERNAL)) {
            return ExternalDataUtils.createExternalRecordReaderFactory(configuration);
        }
        String parser = configuration.get(ExternalDataConstants.KEY_PARSER);
        IInputStreamFactory inputStreamFactory;
        switch (parser) {
            case ExternalDataConstants.FORMAT_ADM:
            case ExternalDataConstants.FORMAT_JSON:
            case ExternalDataConstants.FORMAT_SEMISTRUCTURED:
                inputStreamFactory = DatasourceFactoryProvider.getInputStreamFactory(reader, configuration);
                return new SemiStructuredRecordReaderFactory().setInputStreamFactoryProvider(inputStreamFactory);
            case ExternalDataConstants.FORMAT_LINE_SEPARATED:
                inputStreamFactory = DatasourceFactoryProvider.getInputStreamFactory(reader, configuration);
                return new EmptyLineSeparatedRecordReaderFactory().setInputStreamFactoryProvider(inputStreamFactory);
            case ExternalDataConstants.FORMAT_DELIMITED_TEXT:
            case ExternalDataConstants.FORMAT_CSV:
                inputStreamFactory = DatasourceFactoryProvider.getInputStreamFactory(reader, configuration);
                return new LineRecordReaderFactory().setInputStreamFactoryProvider(inputStreamFactory);
            case ExternalDataConstants.FORMAT_RECORD_WITH_METADATA:
                switch (reader) {
                    case ExternalDataConstants.READER_KV:
                        return new KVReaderFactory();
                    case ExternalDataConstants.READER_KV_TEST:
                        return new KVTestReaderFactory();
                }
        }
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (format != null) {
            switch (format) {
                case ExternalDataConstants.FORMAT_ADM:
                case ExternalDataConstants.FORMAT_JSON:
                case ExternalDataConstants.FORMAT_SEMISTRUCTURED:
                    inputStreamFactory = DatasourceFactoryProvider.getInputStreamFactory(reader, configuration);
                    return new SemiStructuredRecordReaderFactory().setInputStreamFactoryProvider(inputStreamFactory);
                case ExternalDataConstants.FORMAT_LINE_SEPARATED:
                    inputStreamFactory = DatasourceFactoryProvider.getInputStreamFactory(reader, configuration);
                    return new EmptyLineSeparatedRecordReaderFactory()
                            .setInputStreamFactoryProvider(inputStreamFactory);
                case ExternalDataConstants.FORMAT_DELIMITED_TEXT:
                case ExternalDataConstants.FORMAT_CSV:
                    inputStreamFactory = DatasourceFactoryProvider.getInputStreamFactory(reader, configuration);
                    return new LineRecordReaderFactory().setInputStreamFactoryProvider(inputStreamFactory);
            }
        }
        switch (reader) {
            case ExternalDataConstants.READER_HDFS:
                return new HDFSDataSourceFactory();
            case ExternalDataConstants.READER_TWITTER_PULL:
            case ExternalDataConstants.READER_TWITTER_PUSH:
                return new TwitterRecordReaderFactory();
            case ExternalDataConstants.READER_KV:
                return new KVReaderFactory();
            case ExternalDataConstants.READER_KV_TEST:
                return new KVTestReaderFactory();
            case ExternalDataConstants.TEST_RECORD_WITH_PK:
                return new RecordWithPKTestReaderFactory();
            default:
                throw new AsterixException("unknown record reader factory: " + reader);
        }
    }
}
