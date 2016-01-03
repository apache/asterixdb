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
package org.apache.asterix.external.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IInputStreamProviderFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.LongParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;

public class ExternalDataUtils {

    // Get a delimiter from the given configuration
    public static char getDelimiter(Map<String, String> configuration) throws AsterixException {
        String delimiterValue = configuration.get(ExternalDataConstants.KEY_DELIMITER);
        if (delimiterValue == null) {
            delimiterValue = ExternalDataConstants.DEFAULT_DELIMITER;
        } else if (delimiterValue.length() != 1) {
            throw new AsterixException(
                    "'" + delimiterValue + "' is not a valid delimiter. The length of a delimiter should be 1.");
        }
        return delimiterValue.charAt(0);
    }

    // Get a quote from the given configuration when the delimiter is given
    // Need to pass delimiter to check whether they share the same character
    public static char getQuote(Map<String, String> configuration, char delimiter) throws AsterixException {
        String quoteValue = configuration.get(ExternalDataConstants.KEY_QUOTE);
        if (quoteValue == null) {
            quoteValue = ExternalDataConstants.DEFAULT_QUOTE;
        } else if (quoteValue.length() != 1) {
            throw new AsterixException("'" + quoteValue + "' is not a valid quote. The length of a quote should be 1.");
        }

        // Since delimiter (char type value) can't be null,
        // we only check whether delimiter and quote use the same character
        if (quoteValue.charAt(0) == delimiter) {
            throw new AsterixException(
                    "Quote '" + quoteValue + "' cannot be used with the delimiter '" + delimiter + "'. ");
        }

        return quoteValue.charAt(0);
    }

    // Get the header flag
    public static boolean getHasHeader(Map<String, String> configuration) {
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_HEADER));
    }

    public static DataSourceType getDataSourceType(Map<String, String> configuration) throws AsterixException {
        if (isDataSourceStreamProvider(configuration)) {
            return DataSourceType.STREAM;
        } else if (isDataSourceRecordReader(configuration)) {
            return DataSourceType.RECORDS;
        } else {
            throw new AsterixException(
                    "unable to determine whether input is a stream provider or a record reader. parameters: "
                            + ExternalDataConstants.KEY_STREAM + " or " + ExternalDataConstants.KEY_READER
                            + " must be specified");
        }
    }

    public static boolean isExternal(String aString) {
        return (aString.contains(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR) && aString.trim().length() > 1);
    }

    public static ClassLoader getClassLoader(String dataverse, String library) {
        return ExternalLibraryManager.getLibraryClassLoader(dataverse, library);
    }

    public static String getLibraryName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[0];
    }

    public static String getExternalClassName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[1];
    }

    public static IInputStreamProviderFactory createExternalInputStreamFactory(String dataverse, String stream)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String libraryName = getLibraryName(stream);
        String className = getExternalClassName(stream);
        ClassLoader classLoader = getClassLoader(dataverse, libraryName);
        return ((IInputStreamProviderFactory) (classLoader.loadClass(className).newInstance()));
    }

    public static String getDataverse(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_DATAVERSE);
    }

    public static boolean isDataSourceStreamProvider(Map<String, String> configuration) {
        return configuration.containsKey(ExternalDataConstants.KEY_STREAM);
    }

    private static boolean isDataSourceRecordReader(Map<String, String> configuration) {
        return configuration.containsKey(ExternalDataConstants.KEY_READER);
    }

    public static String getRecordFormat(Map<String, String> configuration) {
        String parserFormat = configuration.get(ExternalDataConstants.KEY_DATA_PARSER);
        return parserFormat != null ? parserFormat : configuration.get(ExternalDataConstants.KEY_FORMAT);
    }

    private static Map<ATypeTag, IValueParserFactory> valueParserFactoryMap = initializeValueParserFactoryMap();

    private static Map<ATypeTag, IValueParserFactory> initializeValueParserFactoryMap() {
        Map<ATypeTag, IValueParserFactory> m = new HashMap<ATypeTag, IValueParserFactory>();
        m.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        m.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        m.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        m.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        m.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
        return m;
    }

    public static IValueParserFactory[] getValueParserFactories(ARecordType recordType) {
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = null;
            if (recordType.getFieldTypes()[i].getTypeTag() == ATypeTag.UNION) {
                List<IAType> unionTypes = ((AUnionType) recordType.getFieldTypes()[i]).getUnionList();
                if (unionTypes.size() != 2 && unionTypes.get(0).getTypeTag() != ATypeTag.NULL) {
                    throw new NotImplementedException("Non-optional UNION type is not supported.");
                }
                tag = unionTypes.get(1).getTypeTag();
            } else {
                tag = recordType.getFieldTypes()[i].getTypeTag();
            }
            if (tag == null) {
                throw new NotImplementedException("Failed to get the type information for field " + i + ".");
            }
            IValueParserFactory vpf = valueParserFactoryMap.get(tag);
            if (vpf == null) {
                throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
            }
            fieldParserFactories[i] = vpf;
        }
        return fieldParserFactories;
    }

    public static String getRecordReaderStreamName(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_READER_STREAM);
    }

    public static boolean hasHeader(Map<String, String> configuration) {
        String value = configuration.get(ExternalDataConstants.KEY_HEADER);
        if (value != null) {
            return Boolean.valueOf(value);
        }
        return false;
    }

    public static boolean isPull(Map<String, String> configuration) {
        String pull = configuration.get(ExternalDataConstants.KEY_PULL);
        if (pull == null) {
            return false;
        }
        return Boolean.parseBoolean(pull);
    }

    public static boolean isPush(Map<String, String> configuration) {
        String push = configuration.get(ExternalDataConstants.KEY_PUSH);
        if (push == null) {
            return false;
        }
        return Boolean.parseBoolean(push);
    }

    public static IRecordReaderFactory<?> createExternalRecordReaderFactory(String dataverse, String reader)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String library = reader.substring(0, reader.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR));
        ClassLoader classLoader = ExternalLibraryManager.getLibraryClassLoader(dataverse, library);
        return (IRecordReaderFactory<?>) classLoader
                .loadClass(reader.substring(reader.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR) + 1))
                .newInstance();
    }

    public static IDataParserFactory createExternalParserFactory(String dataverse, String parserFactoryName)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String library = parserFactoryName.substring(0,
                parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR));
        ClassLoader classLoader = ExternalLibraryManager.getLibraryClassLoader(dataverse, library);
        return (IDataParserFactory) classLoader
                .loadClass(parserFactoryName
                        .substring(parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR) + 1))
                .newInstance();
    }
}
