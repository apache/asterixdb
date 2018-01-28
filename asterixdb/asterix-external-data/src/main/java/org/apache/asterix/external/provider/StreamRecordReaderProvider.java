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
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.input.record.reader.stream.StreamRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

public class StreamRecordReaderProvider {

    private static final String RESOURCE =
            "META-INF/services/org.apache.asterix.external.input.record." + "reader.stream.StreamRecordReader";
    private static Map<String, List<Pair<String[], Class>>> recordReaders = null;

    protected static StreamRecordReader getInstance(Class clazz) throws AsterixException {
        try {
            return (StreamRecordReader) clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AsterixException("Cannot create RecordReader: " + clazz.getSimpleName(), e);
        }
    }

    private StreamRecordReaderProvider() {
        // do nothing
    }

    public static Class findRecordReaderClazzWithConfig(Map<String, String> configuration, String format)
            throws AsterixException {
        List<Pair<String[], Class>> requiredConfigs = recordReaders.get(format);
        Class clazz = null;
        int maxOptNum = 0;
        for (Pair<String[], Class> configPair : requiredConfigs) {
            Boolean matchFlag = true;
            int matchOptNum = 0;
            // Here StreamRecordReaderProvider will try its best to find the most suitable
            // record reader by matching options. However, if there is a option key error,
            // it will be ignored and a potential wrong record reader with less required options
            // will be returned.
            for (String configKey : configPair.getLeft()) {
                if (configKey.length() > 0 && !configuration.containsKey(configKey)) {
                    matchFlag = false;
                    break;
                }
                if (configKey.length() > 0) {
                    matchOptNum++;
                }
            }
            if (matchFlag && matchOptNum >= maxOptNum) {
                clazz = configPair.getRight();
                maxOptNum = matchOptNum;
            }
        }
        if (clazz != null) {
            return clazz;
        } else {
            throw new AsterixException(ErrorCode.PROVIDER_STREAM_RECORD_READER_WRONG_CONFIGURATION, format);
        }
    }

    public static Class getRecordReaderClazz(Map<String, String> configuration) throws AsterixException {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);

        if (recordReaders == null) {
            recordReaders = initRecordReaders();
        }

        if (format != null) {
            if (recordReaders.containsKey(format)) {
                return findRecordReaderClazzWithConfig(configuration, format);
            }
            throw new AsterixException(ErrorCode.PROVIDER_STREAM_RECORD_READER_UNKNOWN_FORMAT, format);
        }
        throw new AsterixException("Unspecified parameter: " + ExternalDataConstants.KEY_FORMAT);
    }

    protected static Map<String, List<Pair<String[], Class>>> initRecordReaders() throws AsterixException {
        Map<String, List<Pair<String[], Class>>> recordReaders = new HashMap<>();
        ClassLoader cl = StreamRecordReaderProvider.class.getClassLoader();
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
                    StreamRecordReader newInstance = (StreamRecordReader) clazz.getConstructor().newInstance();
                    List<String> formats = newInstance.getRecordReaderFormats();
                    String[] configs = newInstance.getRequiredConfigs().split(":");
                    for (String format : formats) {
                        if (!recordReaders.containsKey(format)) {
                            recordReaders.put(format, new ArrayList<>());
                        }
                        recordReaders.get(format).add(Pair.of(configs, clazz));
                    }
                }
            }
        } catch (IOException | ClassNotFoundException | InvocationTargetException | IllegalAccessException
                | NoSuchMethodException | InstantiationException e) {
            throw new AsterixException(e);
        }
        return recordReaders;
    }
}
