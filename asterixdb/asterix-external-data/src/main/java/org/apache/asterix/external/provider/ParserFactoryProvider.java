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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.commons.io.IOUtils;

public class ParserFactoryProvider {

    private static final String RESOURCE = "META-INF/services/org.apache.asterix.external.api.IDataParserFactory";
    private static Map<String, Class> factories = null;

    private ParserFactoryProvider() {
    }

    public static IDataParserFactory getDataParserFactory(ILibraryManager libraryManager,
            Map<String, String> configuration) throws AsterixException {
        IDataParserFactory parserFactory;
        String parserFactoryName = configuration.get(ExternalDataConstants.KEY_DATA_PARSER);
        if (ExternalDataUtils.isExternal(parserFactoryName)) {
            return ExternalDataUtils.createExternalParserFactory(libraryManager,
                    ExternalDataUtils.getDataverse(configuration), parserFactoryName);
        } else {
            String parserFactoryKey = ExternalDataUtils.getRecordFormat(configuration);
            if (parserFactoryKey == null) {
                parserFactoryKey = configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
            }
            parserFactory = ParserFactoryProvider.getDataParserFactory(parserFactoryKey);
        }
        return parserFactory;
    }

    protected static IDataParserFactory getInstance(Class clazz) throws AsterixException {
        try {
            return (IDataParserFactory) clazz.newInstance();
        } catch (IllegalAccessException | InstantiationException | ClassCastException e) {
            throw new AsterixException("Cannot create: " + clazz.getSimpleName(), e);
        }
    }

    @SuppressWarnings("rawtypes")
    public static IDataParserFactory getDataParserFactory(String parser) throws AsterixException {

        if (factories == null) {
            factories = initFactories();
        }

        if (factories.containsKey(parser)) {
            return getInstance(factories.get(parser));
        }

        try {
            // ideally, this should not happen
            return (IDataParserFactory) Class.forName(parser).newInstance();
        } catch (IllegalAccessException | ClassNotFoundException | InstantiationException | ClassCastException e) {
            throw new AsterixException("Unknown format: " + parser, e);
        }
    }

    protected static Map<String, Class> initFactories() throws AsterixException {
        Map<String, Class> factories = new HashMap<>();
        ClassLoader cl = ParserFactoryProvider.class.getClassLoader();
        try {
            Enumeration<URL> urls = cl.getResources(RESOURCE);
            for (URL url : Collections.list(urls)) {
                List<String> classNames;
                try (InputStream is = url.openStream()) {
                    classNames = IOUtils.readLines(is, StandardCharsets.UTF_8);
                }
                for (String className : classNames) {
                    if (className.startsWith("#")) {
                        continue;
                    }
                    final Class<?> clazz = Class.forName(className);
                    List<String> formats = ((IDataParserFactory) clazz.newInstance()).getParserFormats();
                    for (String format : formats) {
                        if (factories.containsKey(format)) {
                            throw new AsterixException("Duplicate format " + format);
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
