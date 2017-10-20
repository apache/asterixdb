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

import java.util.EnumMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
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

    public static void validateParameters(Map<String, String> configuration) throws AsterixException {
        validateDataSourceParameters(configuration);
        validateDataParserParameters(configuration);
    }

    public static void validateDataParserParameters(Map<String, String> configuration) throws AsterixException {
        String parser = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (parser == null) {
            String parserFactory = configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
            if (parserFactory == null) {
                throw new AsterixException("The parameter " + ExternalDataConstants.KEY_FORMAT + " or "
                        + ExternalDataConstants.KEY_PARSER_FACTORY + " must be specified.");
            }
        }
    }

    public static void validateDataSourceParameters(Map<String, String> configuration) throws AsterixException {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        if (reader == null) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER + " must be specified.");
        }
    }

    public static DataSourceType getDataSourceType(Map<String, String> configuration) {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        if ((reader != null) && reader.equals(ExternalDataConstants.READER_STREAM)) {
            return DataSourceType.STREAM;
        } else {
            return DataSourceType.RECORDS;
        }
    }

    public static boolean isExternal(String aString) {
        return ((aString != null) && aString.contains(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR)
                && (aString.trim().length() > 1));
    }

    public static ClassLoader getClassLoader(ILibraryManager libraryManager, String dataverse, String library) {
        return libraryManager.getLibraryClassLoader(dataverse, library);
    }

    public static String getLibraryName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[0];
    }

    public static String getExternalClassName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[1];
    }

    public static IInputStreamFactory createExternalInputStreamFactory(ILibraryManager libraryManager, String dataverse,
            String stream) throws HyracksDataException {
        try {
            String libraryName = getLibraryName(stream);
            String className = getExternalClassName(stream);
            ClassLoader classLoader = getClassLoader(libraryManager, dataverse, libraryName);
            return ((IInputStreamFactory) (classLoader.loadClass(className).newInstance()));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeDataException(ErrorCode.UTIL_EXTERNAL_DATA_UTILS_FAIL_CREATE_STREAM_FACTORY, e);
        }
    }

    public static String getDataverse(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_DATAVERSE);
    }

    public static String getRecordFormat(Map<String, String> configuration) {
        String parserFormat = configuration.get(ExternalDataConstants.KEY_DATA_PARSER);
        return parserFormat != null ? parserFormat : configuration.get(ExternalDataConstants.KEY_FORMAT);
    }

    public static void setRecordFormat(Map<String, String> configuration, String format) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_DATA_PARSER)) {
            configuration.put(ExternalDataConstants.KEY_DATA_PARSER, format);
        }
        if (!configuration.containsKey(ExternalDataConstants.KEY_FORMAT)) {
            configuration.put(ExternalDataConstants.KEY_FORMAT, format);
        }
    }

    private static Map<ATypeTag, IValueParserFactory> valueParserFactoryMap = initializeValueParserFactoryMap();

    private static Map<ATypeTag, IValueParserFactory> initializeValueParserFactoryMap() {
        Map<ATypeTag, IValueParserFactory> m = new EnumMap<>(ATypeTag.class);
        m.put(ATypeTag.INTEGER, IntegerParserFactory.INSTANCE);
        m.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        m.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        m.put(ATypeTag.BIGINT, LongParserFactory.INSTANCE);
        m.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
        return m;
    }

    public static IValueParserFactory[] getValueParserFactories(ARecordType recordType) {
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = null;
            if (recordType.getFieldTypes()[i].getTypeTag() == ATypeTag.UNION) {
                AUnionType unionType = (AUnionType) recordType.getFieldTypes()[i];
                if (!unionType.isUnknownableType()) {
                    throw new NotImplementedException("Non-optional UNION type is not supported.");
                }
                tag = unionType.getActualType().getTypeTag();
            } else {
                tag = recordType.getFieldTypes()[i].getTypeTag();
            }
            if (tag == null) {
                throw new NotImplementedException("Failed to get the type information for field " + i + ".");
            }
            fieldParserFactories[i] = getParserFactory(tag);
        }
        return fieldParserFactories;
    }

    public static IValueParserFactory getParserFactory(ATypeTag tag) {
        IValueParserFactory vpf = valueParserFactoryMap.get(tag);
        if (vpf == null) {
            throw new NotImplementedException("No value parser factory for fields of type " + tag);
        }
        return vpf;
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

    public static IRecordReaderFactory<?> createExternalRecordReaderFactory(ILibraryManager libraryManager,
            Map<String, String> configuration) throws AsterixException {
        String readerFactory = configuration.get(ExternalDataConstants.KEY_READER_FACTORY);
        if (readerFactory == null) {
            throw new AsterixException("to use " + ExternalDataConstants.EXTERNAL + " reader, the parameter "
                    + ExternalDataConstants.KEY_READER_FACTORY + " must be specified.");
        }
        String[] libraryAndFactory = readerFactory.split(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR);
        if (libraryAndFactory.length != 2) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER_FACTORY
                    + " must follow the format \"DataverseName.LibraryName#ReaderFactoryFullyQualifiedName\"");
        }
        String[] dataverseAndLibrary = libraryAndFactory[0].split(".");
        if (dataverseAndLibrary.length != 2) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER_FACTORY
                    + " must follow the format \"DataverseName.LibraryName#ReaderFactoryFullyQualifiedName\"");
        }

        ClassLoader classLoader = libraryManager.getLibraryClassLoader(dataverseAndLibrary[0], dataverseAndLibrary[1]);
        try {
            return (IRecordReaderFactory<?>) classLoader.loadClass(libraryAndFactory[1]).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AsterixException("Failed to create record reader factory", e);
        }
    }

    public static IDataParserFactory createExternalParserFactory(ILibraryManager libraryManager, String dataverse,
            String parserFactoryName) throws AsterixException {
        try {
            String library = parserFactoryName.substring(0,
                    parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR));
            ClassLoader classLoader = libraryManager.getLibraryClassLoader(dataverse, library);
            return (IDataParserFactory) classLoader
                    .loadClass(parserFactoryName
                            .substring(parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR) + 1))
                    .newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AsterixException("Failed to create an external parser factory", e);
        }
    }

    public static boolean isFeed(Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_IS_FEED)) {
            return false;
        } else {
            return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_FEED));
        }
    }

    public static void prepareFeed(Map<String, String> configuration, String dataverseName, String feedName) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_IS_FEED)) {
            configuration.put(ExternalDataConstants.KEY_IS_FEED, ExternalDataConstants.TRUE);
        }
        configuration.put(ExternalDataConstants.KEY_DATAVERSE, dataverseName);
        configuration.put(ExternalDataConstants.KEY_FEED_NAME, feedName);
    }

    public static boolean keepDataSourceOpen(Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_WAIT_FOR_DATA)) {
            return true;
        }
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_WAIT_FOR_DATA));
    }

    public static String getFeedName(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_FEED_NAME);
    }

    public static int getQueueSize(Map<String, String> configuration) {
        return configuration.containsKey(ExternalDataConstants.KEY_QUEUE_SIZE)
                ? Integer.parseInt(configuration.get(ExternalDataConstants.KEY_QUEUE_SIZE))
                : ExternalDataConstants.DEFAULT_QUEUE_SIZE;
    }

    public static boolean isRecordWithMeta(Map<String, String> configuration) {
        return configuration.containsKey(ExternalDataConstants.KEY_META_TYPE_NAME);
    }

    public static void setRecordWithMeta(Map<String, String> configuration, String booleanString) {
        configuration.put(ExternalDataConstants.FORMAT_RECORD_WITH_METADATA, booleanString);
    }

    public static boolean isChangeFeed(Map<String, String> configuration) {
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_CHANGE_FEED));
    }

    public static boolean isInsertFeed(Map<String, String> configuration) {
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_INSERT_FEED));
    }

    public static int getNumberOfKeys(Map<String, String> configuration) throws AsterixException {
        String keyIndexes = configuration.get(ExternalDataConstants.KEY_KEY_INDEXES);
        if (keyIndexes == null) {
            throw new AsterixException(
                    "A change feed must have the parameter " + ExternalDataConstants.KEY_KEY_INDEXES);
        }
        return keyIndexes.split(",").length;
    }

    public static void setNumberOfKeys(Map<String, String> configuration, int value) {
        configuration.put(ExternalDataConstants.KEY_KEY_SIZE, String.valueOf(value));
    }

    public static void setChangeFeed(Map<String, String> configuration, String booleanString) {
        configuration.put(ExternalDataConstants.KEY_IS_CHANGE_FEED, booleanString);
    }

    public static int[] getPKIndexes(Map<String, String> configuration) {
        String keyIndexes = configuration.get(ExternalDataConstants.KEY_KEY_INDEXES);
        String[] stringIndexes = keyIndexes.split(",");
        int[] intIndexes = new int[stringIndexes.length];
        for (int i = 0; i < stringIndexes.length; i++) {
            intIndexes[i] = Integer.parseInt(stringIndexes[i]);
        }
        return intIndexes;
    }

    public static int[] getPKSourceIndicators(Map<String, String> configuration) {
        String keyIndicators = configuration.get(ExternalDataConstants.KEY_KEY_INDICATORS);
        String[] stringIndicators = keyIndicators.split(",");
        int[] intIndicators = new int[stringIndicators.length];
        for (int i = 0; i < stringIndicators.length; i++) {
            intIndicators[i] = Integer.parseInt(stringIndicators[i]);
        }
        return intIndicators;
    }
}
