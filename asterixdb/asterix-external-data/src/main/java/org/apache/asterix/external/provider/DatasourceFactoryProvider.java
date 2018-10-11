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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.stream.factory.LocalFSInputStreamFactory;
import org.apache.asterix.external.input.stream.factory.SocketServerInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class DatasourceFactoryProvider {

    private static final String RESOURCE = "META-INF/services/org.apache.asterix.external.api.IRecordReaderFactory";
    private static Map<String, Class> factories = null;

    private DatasourceFactoryProvider() {
    }

    public static IExternalDataSourceFactory getExternalDataSourceFactory(ILibraryManager libraryManager,
            Map<String, String> configuration) throws HyracksDataException, AsterixException {
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
            Map<String, String> configuration) throws HyracksDataException {
        IInputStreamFactory streamSourceFactory;
        if (ExternalDataUtils.isExternal(streamSource)) {
            String dataverse = ExternalDataUtils.getDataverse(configuration);
            streamSourceFactory =
                    ExternalDataUtils.createExternalInputStreamFactory(libraryManager, dataverse, streamSource);
        } else {
            switch (streamSource) {
                case ExternalDataConstants.KEY_ADAPTER_NAME_LOCALFS:
                    streamSourceFactory = new LocalFSInputStreamFactory();
                    break;
                case ExternalDataConstants.KEY_ADAPTER_NAME_SOCKET:
                case ExternalDataConstants.KEY_ALIAS_ADAPTER_NAME_SOCKET:
                    streamSourceFactory = new SocketServerInputStreamFactory();
                    break;
                case ExternalDataConstants.STREAM_SOCKET_CLIENT:
                    streamSourceFactory = new SocketServerInputStreamFactory();
                    break;
                default:
                    try {
                        streamSourceFactory = (IInputStreamFactory) Class.forName(streamSource).newInstance();
                    } catch (Exception e) {
                        throw new RuntimeDataException(
                                ErrorCode.PROVIDER_DATASOURCE_FACTORY_UNKNOWN_INPUT_STREAM_FACTORY, e, streamSource);
                    }
            }
        }
        return streamSourceFactory;
    }

    protected static IRecordReaderFactory getInstance(Class clazz) throws AsterixException {
        try {
            return (IRecordReaderFactory) clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassCastException e) {
            throw new AsterixException("Cannot create: " + clazz.getSimpleName(), e);
        }
    }

    public static IRecordReaderFactory getRecordReaderFactory(ILibraryManager libraryManager, String adaptorName,
            Map<String, String> configuration) throws HyracksDataException, AsterixException {
        if (adaptorName.equals(ExternalDataConstants.EXTERNAL)) {
            return ExternalDataUtils.createExternalRecordReaderFactory(libraryManager, configuration);
        }

        if (factories == null) {
            factories = initFactories();
        }

        if (factories.containsKey(adaptorName)) {
            return getInstance(factories.get(adaptorName));
        }

        try {
            return (IRecordReaderFactory) Class.forName(adaptorName).newInstance();
        } catch (IllegalAccessException | ClassNotFoundException | InstantiationException | ClassCastException e) {
            throw new RuntimeDataException(ErrorCode.UNKNOWN_RECORD_READER_FACTORY, e, adaptorName);
        }
    }

    protected static Map<String, Class> initFactories() throws AsterixException {
        Map<String, Class> factories = new HashMap<>();
        ClassLoader cl = ParserFactoryProvider.class.getClassLoader();
        final Charset encoding = Charset.forName("UTF-8");
        try {
            Enumeration<URL> urls = cl.getResources(RESOURCE);
            for (URL url : Collections.list(urls)) {
                InputStream is = url.openStream();
                String config = IOUtils.toString(is, encoding);
                is.close();
                String[] classNames = config.split("\n");
                for (String className : classNames) {
                    if (className.startsWith("#")) {
                        continue;
                    }
                    final Class<?> clazz = Class.forName(className);
                    List<String> formats = ((IRecordReaderFactory) clazz.newInstance()).getRecordReaderNames();
                    for (String format : formats) {
                        if (factories.containsKey(format)) {
                            throw new AsterixException(ErrorCode.PROVIDER_DATASOURCE_FACTORY_DUPLICATE_FORMAT_MAPPING,
                                    format);
                        }
                        factories.put(format, clazz);
                    }
                }
            }
        } catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new AsterixException(e);
        }
        return factories;
    }
}
