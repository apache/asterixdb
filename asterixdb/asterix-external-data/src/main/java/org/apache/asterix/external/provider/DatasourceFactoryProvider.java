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
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.HDFSDataSourceFactory;
import org.apache.asterix.external.input.record.reader.rss.RSSRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.stream.StreamRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.twitter.TwitterRecordReaderFactory;
import org.apache.asterix.external.input.stream.factory.LocalFSInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketClientInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketServerInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;

public class DatasourceFactoryProvider {

    private DatasourceFactoryProvider() {
    }

    public static IExternalDataSourceFactory getExternalDataSourceFactory(ILibraryManager libraryManager,
            Map<String, String> configuration) throws AsterixException {
        if (ExternalDataUtils.getDataSourceType(configuration).equals(DataSourceType.RECORDS)) {
            String reader = configuration.get(ExternalDataConstants.KEY_READER);
            return DatasourceFactoryProvider.getRecordReaderFactory(libraryManager, reader, configuration);
        } else {
            // get stream source
            String streamSource = configuration.get(ExternalDataConstants.KEY_STREAM_SOURCE);
            return DatasourceFactoryProvider.getInputStreamFactory(libraryManager, streamSource, configuration);
        }
    }

    public static IInputStreamFactory getInputStreamFactory(ILibraryManager libraryManager, String streamSource,
            Map<String, String> configuration) throws AsterixException {
        IInputStreamFactory streamSourceFactory;
        if (ExternalDataUtils.isExternal(streamSource)) {
            String dataverse = ExternalDataUtils.getDataverse(configuration);
            streamSourceFactory = ExternalDataUtils.createExternalInputStreamFactory(libraryManager, dataverse,
                    streamSource);
        } else {
            switch (streamSource) {
                case ExternalDataConstants.STREAM_LOCAL_FILESYSTEM:
                    streamSourceFactory = new LocalFSInputStreamFactory();
                    break;
                case ExternalDataConstants.SOCKET:
                case ExternalDataConstants.ALIAS_SOCKET_ADAPTER:
                    streamSourceFactory = new SocketServerInputStreamFactory();
                    break;
                case ExternalDataConstants.STREAM_SOCKET_CLIENT:
                    streamSourceFactory = new SocketServerInputStreamFactory();
                    break;
                default:
                    try {
                        streamSourceFactory = (IInputStreamFactory) Class.forName(streamSource).newInstance();
                    } catch (Exception e) {
                        throw new AsterixException("unknown input stream factory: " + streamSource, e);
                    }
            }
        }
        return streamSourceFactory;
    }

    public static IRecordReaderFactory<?> getRecordReaderFactory(ILibraryManager libraryManager, String reader,
            Map<String, String> configuration) throws AsterixException {
        if (reader.equals(ExternalDataConstants.EXTERNAL)) {
            return ExternalDataUtils.createExternalRecordReaderFactory(libraryManager, configuration);
        }
        switch (reader) {
            case ExternalDataConstants.READER_HDFS:
                return new HDFSDataSourceFactory();
            case ExternalDataConstants.ALIAS_LOCALFS_ADAPTER:
                return new StreamRecordReaderFactory(new LocalFSInputStreamFactory());
            case ExternalDataConstants.READER_TWITTER_PULL:
            case ExternalDataConstants.READER_TWITTER_PUSH:
            case ExternalDataConstants.READER_PUSH_TWITTER:
            case ExternalDataConstants.READER_PULL_TWITTER:
            case ExternalDataConstants.READER_USER_STREAM_TWITTER:
                return new TwitterRecordReaderFactory();
            case ExternalDataConstants.ALIAS_SOCKET_ADAPTER:
            case ExternalDataConstants.SOCKET:
                return new StreamRecordReaderFactory(new SocketServerInputStreamFactory());
            case ExternalDataConstants.STREAM_SOCKET_CLIENT:
                return new StreamRecordReaderFactory(new SocketClientInputStreamFactory());
            case ExternalDataConstants.READER_RSS:
                return new RSSRecordReaderFactory();
            default:
                try {
                    return (IRecordReaderFactory<?>) Class.forName(reader).newInstance();
                } catch (IllegalAccessException | ClassNotFoundException | InstantiationException
                        | ClassCastException e) {
                    throw new AsterixException("Unknown record reader factory: " + reader, e);
                }
        }
    }
}
