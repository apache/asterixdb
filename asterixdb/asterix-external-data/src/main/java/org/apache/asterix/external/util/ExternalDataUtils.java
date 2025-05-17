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

import static com.google.cloud.storage.Storage.BlobListOption;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_REQ_PARAM_VAL;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_NOT_ALLOWED_AT_SAME_TIME;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_REQUIRED;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_OR_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.S3_REGION_NOT_SUPPORTED;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_ADAPTER_NAME_GCS;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_DELIMITER;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_ESCAPE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_EXCLUDE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_EXTERNAL_SCAN_BUFFER_SIZE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_FORMAT;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_INCLUDE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_QUOTE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_RECORD_END;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_RECORD_START;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.ACCESS_KEY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.ERROR_METHOD_NOT_IMPLEMENTED;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_ACCESS_KEY_ID;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_ANONYMOUS_ACCESS;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_CREDENTIAL_PROVIDER_KEY;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_PATH_STYLE_ACCESS;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_S3_CONNECTION_POOL_SIZE;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_S3_PROTOCOL;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_SECRET_ACCESS_KEY;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_SESSION_TOKEN;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.HADOOP_TEMP_ACCESS;
import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3.SECRET_ACCESS_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.ACCOUNT_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.ACCOUNT_NAME_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.CLIENT_CERTIFICATE_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.CLIENT_ID_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.CLIENT_SECRET_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.ENDPOINT_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.HADOOP_AZURE_BLOB_PROTOCOL;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.HADOOP_AZURE_FS_ACCOUNT_KEY;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.HADOOP_AZURE_FS_SAS;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.MANAGED_IDENTITY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.RECURSIVE_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.SHARED_ACCESS_SIGNATURE_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.Azure.TENANT_ID_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.GCS.JSON_CREDENTIALS_FIELD_NAME;
import static org.apache.asterix.runtime.evaluators.functions.StringEvaluatorUtils.RESERVED_REGEX_CHARS;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.ILibrary;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory.IncludeExcludeMatcher;
import org.apache.asterix.external.library.JavaLibrary;
import org.apache.asterix.external.util.ExternalDataConstants.ParquetOptions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.asterix.runtime.projection.DataProjectionInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.parsers.BooleanParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.LongParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.util.StorageUtil;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.http.rest.PagedIterable;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.S3Response;

public class ExternalDataUtils {
    private static final Map<ATypeTag, IValueParserFactory> valueParserFactoryMap = new EnumMap<>(ATypeTag.class);
    private static final int DEFAULT_MAX_ARGUMENT_SZ = 1024 * 1024;
    private static final int HEADER_FUDGE = 64;

    static {
        valueParserFactoryMap.put(ATypeTag.INTEGER, IntegerParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.BIGINT, LongParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.BOOLEAN, BooleanParserFactory.INSTANCE);
    }

    private ExternalDataUtils() {
    }

    public static int getOrDefaultBufferSize(Map<String, String> configuration) {
        String bufferSize = configuration.get(KEY_EXTERNAL_SCAN_BUFFER_SIZE);
        return bufferSize != null ? Integer.parseInt(bufferSize) : ExternalDataConstants.DEFAULT_BUFFER_SIZE;
    }

    // Get a delimiter from the given configuration
    public static char validateGetDelimiter(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_DELIMITER, ExternalDataConstants.DEFAULT_DELIMITER.charAt(0));
    }

    // Get a quote from the given configuration when the delimiter is given
    // Need to pass delimiter to check whether they share the same character
    public static char validateGetQuote(Map<String, String> configuration, char delimiter) throws HyracksDataException {
        char quote = validateCharOrDefault(configuration, KEY_QUOTE, ExternalDataConstants.DEFAULT_QUOTE.charAt(0));
        validateDelimiterAndQuote(delimiter, quote);
        return quote;
    }

    public static char validateGetEscape(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_ESCAPE, ExternalDataConstants.ESCAPE);
    }

    public static char validateGetRecordStart(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_RECORD_START, ExternalDataConstants.DEFAULT_RECORD_START);
    }

    public static char validateGetRecordEnd(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_RECORD_END, ExternalDataConstants.DEFAULT_RECORD_END);
    }

    public static void validateDataParserParameters(Map<String, String> configuration) throws AsterixException {
        String parser = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (parser == null) {
            String parserFactory = configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
            if (parserFactory == null) {
                throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED,
                        ExternalDataConstants.KEY_FORMAT + " or " + ExternalDataConstants.KEY_PARSER_FACTORY);
            }
        }
    }

    public static void validateDataSourceParameters(Map<String, String> configuration) throws AsterixException {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        if (reader == null) {
            throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_READER);
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

    public static String getLibraryName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[0];
    }

    public static String getExternalClassName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[1];
    }

    public static IInputStreamFactory createExternalInputStreamFactory(ILibraryManager libraryManager,
            DataverseName dataverse, String stream) throws HyracksDataException {
        try {
            String libraryName = getLibraryName(stream);
            String className = getExternalClassName(stream);
            ILibrary lib = libraryManager.getLibrary(dataverse, libraryName);
            if (lib.getLanguage() != ExternalFunctionLanguage.JAVA) {
                throw new HyracksDataException("Unexpected library language: " + lib.getLanguage());
            }
            ClassLoader classLoader = ((JavaLibrary) lib).getClassLoader();
            return ((IInputStreamFactory) (classLoader.loadClass(className).newInstance()));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeDataException(ErrorCode.UTIL_EXTERNAL_DATA_UTILS_FAIL_CREATE_STREAM_FACTORY, e);
        }
    }

    public static DataverseName getDatasetDataverse(Map<String, String> configuration) throws AsterixException {
        return DataverseName.createFromCanonicalForm(configuration.get(ExternalDataConstants.KEY_DATASET_DATAVERSE));
    }

    public static String getParserFactory(Map<String, String> configuration) {
        String parserFactory = configuration.get(ExternalDataConstants.KEY_PARSER);
        if (parserFactory != null) {
            return parserFactory;
        }
        parserFactory = configuration.get(ExternalDataConstants.KEY_FORMAT);
        return parserFactory != null ? parserFactory : configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
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

    public static boolean hasHeader(Map<String, String> configuration) {
        return isTrue(configuration, ExternalDataConstants.KEY_HEADER);
    }

    public static boolean isTrue(Map<String, String> configuration, String key) {
        String value = configuration.get(key);
        return value == null ? false : Boolean.valueOf(value);
    }

    // Currently not used.
    public static IRecordReaderFactory<?> createExternalRecordReaderFactory(ILibraryManager libraryManager,
            Map<String, String> configuration) throws AsterixException {
        String readerFactory = configuration.get(ExternalDataConstants.KEY_READER_FACTORY);
        if (readerFactory == null) {
            throw new AsterixException("to use " + ExternalDataConstants.EXTERNAL + " reader, the parameter "
                    + ExternalDataConstants.KEY_READER_FACTORY + " must be specified.");
        }
        String[] libraryAndFactory = readerFactory.split(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
        if (libraryAndFactory.length != 2) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER_FACTORY
                    + " must follow the format \"DataverseName.LibraryName#ReaderFactoryFullyQualifiedName\"");
        }
        String[] dataverseAndLibrary = libraryAndFactory[0].split("\\.");
        if (dataverseAndLibrary.length != 2) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER_FACTORY
                    + " must follow the format \"DataverseName.LibraryName#ReaderFactoryFullyQualifiedName\"");
        }
        DataverseName dataverseName = DataverseName.createSinglePartName(dataverseAndLibrary[0]); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
        String libraryName = dataverseAndLibrary[1];
        ILibrary lib;
        try {
            lib = libraryManager.getLibrary(dataverseName, libraryName);
        } catch (HyracksDataException e) {
            throw new AsterixException("Cannot load library", e);
        }
        if (lib.getLanguage() != ExternalFunctionLanguage.JAVA) {
            throw new AsterixException("Unexpected library language: " + lib.getLanguage());
        }
        ClassLoader classLoader = ((JavaLibrary) lib).getClassLoader();
        try {
            return (IRecordReaderFactory<?>) classLoader.loadClass(libraryAndFactory[1]).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AsterixException("Failed to create record reader factory", e);
        }
    }

    // Currently not used.
    public static IDataParserFactory createExternalParserFactory(ILibraryManager libraryManager,
            DataverseName dataverse, String parserFactoryName) throws AsterixException {
        try {
            String library = parserFactoryName.substring(0,
                    parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR));
            ILibrary lib;
            try {
                lib = libraryManager.getLibrary(dataverse, library);
            } catch (HyracksDataException e) {
                throw new AsterixException("Cannot load library", e);
            }
            if (lib.getLanguage() != ExternalFunctionLanguage.JAVA) {
                throw new AsterixException("Unexpected library language: " + lib.getLanguage());
            }
            ClassLoader classLoader = ((JavaLibrary) lib).getClassLoader();
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

    public static void prepareFeed(Map<String, String> configuration, DataverseName dataverseName, String feedName) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_IS_FEED)) {
            configuration.put(ExternalDataConstants.KEY_IS_FEED, ExternalDataConstants.TRUE);
        }
        configuration.put(ExternalDataConstants.KEY_DATASET_DATAVERSE, dataverseName.getCanonicalForm());
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
            throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_KEY_INDEXES);
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

    /**
     * Fills the configuration of the external dataset and its adapter with default values if not provided by user.
     *
     * @param configuration external data configuration
     */
    public static void defaultConfiguration(Map<String, String> configuration) {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (format != null) {
            // default quote, escape character for quote and fields delimiter for csv and tsv format
            if (format.equals(ExternalDataConstants.FORMAT_CSV)) {
                configuration.putIfAbsent(KEY_DELIMITER, ExternalDataConstants.DEFAULT_DELIMITER);
                configuration.putIfAbsent(KEY_QUOTE, ExternalDataConstants.DEFAULT_QUOTE);
                configuration.putIfAbsent(KEY_ESCAPE, ExternalDataConstants.DEFAULT_QUOTE);
            } else if (format.equals(ExternalDataConstants.FORMAT_TSV)) {
                configuration.putIfAbsent(KEY_DELIMITER, ExternalDataConstants.TAB_STR);
                configuration.putIfAbsent(KEY_QUOTE, ExternalDataConstants.NULL_STR);
                configuration.putIfAbsent(KEY_ESCAPE, ExternalDataConstants.NULL_STR);
            }
        }
    }

    /**
     * Prepares the configuration of the external data and its adapter by filling the information required by
     * adapters and parsers.
     *
     * @param adapterName   adapter name
     * @param configuration external data configuration
     */
    public static void prepare(String adapterName, Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_READER)) {
            configuration.put(ExternalDataConstants.KEY_READER, adapterName);
        }
        final String inputFormat = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT);
        if (ExternalDataConstants.INPUT_FORMAT_PARQUET.equals(inputFormat)) {
            //Parquet supports binary-to-binary conversion. No parsing is required
            configuration.put(ExternalDataConstants.KEY_PARSER, ExternalDataConstants.FORMAT_NOOP);
            configuration.put(ExternalDataConstants.KEY_FORMAT, ExternalDataConstants.FORMAT_PARQUET);
        }
        if (!configuration.containsKey(ExternalDataConstants.KEY_PARSER)
                && configuration.containsKey(ExternalDataConstants.KEY_FORMAT)) {
            configuration.put(ExternalDataConstants.KEY_PARSER, configuration.get(ExternalDataConstants.KEY_FORMAT));
        }
    }

    /**
     * Normalizes the values of certain parameters of the adapter configuration. This should happen before persisting
     * the metadata (e.g. when creating external datasets or feeds) and when creating an adapter factory.
     *
     * @param configuration external data configuration
     */
    public static void normalize(Map<String, String> configuration) {
        // normalize the "format" parameter
        String paramValue = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (paramValue != null) {
            String lowerCaseFormat = paramValue.toLowerCase().trim();
            if (ExternalDataConstants.ALL_FORMATS.contains(lowerCaseFormat)) {
                configuration.put(ExternalDataConstants.KEY_FORMAT, lowerCaseFormat);
            }
        }
        // normalize "header" parameter
        putToLowerIfExists(configuration, ExternalDataConstants.KEY_HEADER);
        // normalize "redact-warnings" parameter
        putToLowerIfExists(configuration, ExternalDataConstants.KEY_REDACT_WARNINGS);
    }

    /**
     * Validates the parameter values of the adapter configuration. This should happen after normalizing the values.
     *
     * @param configuration external data configuration
     * @throws HyracksDataException HyracksDataException
     */
    public static void validate(Map<String, String> configuration) throws HyracksDataException {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        String header = configuration.get(ExternalDataConstants.KEY_HEADER);
        if (format != null && isHeaderRequiredFor(format) && header == null) {
            throw new RuntimeDataException(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_HEADER);
        }
        if (header != null && !isBoolean(header)) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, ExternalDataConstants.KEY_HEADER, header);
        }
        char delimiter = validateGetDelimiter(configuration);
        validateGetQuote(configuration, delimiter);
        validateGetEscape(configuration);
        String value = configuration.get(ExternalDataConstants.KEY_REDACT_WARNINGS);
        if (value != null && !isBoolean(value)) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, ExternalDataConstants.KEY_REDACT_WARNINGS,
                    value);
        }
    }

    private static boolean isHeaderRequiredFor(String format) {
        return format.equals(ExternalDataConstants.FORMAT_CSV) || format.equals(ExternalDataConstants.FORMAT_TSV);
    }

    private static boolean isBoolean(String value) {
        return value.equals(ExternalDataConstants.TRUE) || value.equals(ExternalDataConstants.FALSE);
    }

    private static void validateDelimiterAndQuote(char delimiter, char quote) throws RuntimeDataException {
        if (quote == delimiter) {
            throw new RuntimeDataException(ErrorCode.QUOTE_DELIMITER_MISMATCH, quote, delimiter);
        }
    }

    private static char validateCharOrDefault(Map<String, String> configuration, String key, char defaultValue)
            throws HyracksDataException {
        String value = configuration.get(key);
        if (value == null) {
            return defaultValue;
        }
        validateChar(value, key);
        return value.charAt(0);
    }

    public static void validateChar(String parameterValue, String parameterName) throws RuntimeDataException {
        if (parameterValue.length() != 1) {
            throw new RuntimeDataException(ErrorCode.INVALID_CHAR_LENGTH, parameterValue, parameterName);
        }
    }

    private static void putToLowerIfExists(Map<String, String> configuration, String key) {
        String paramValue = configuration.get(key);
        if (paramValue != null) {
            configuration.put(key, paramValue.toLowerCase().trim());
        }
    }

    /**
     * Validates adapter specific external dataset properties. Specific properties for different adapters should be
     * validated here
     *
     * @param configuration properties
     */
    public static void validateAdapterSpecificProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector, IApplicationContext appCtx) throws CompilationException {
        String type = configuration.get(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE);

        switch (type) {
            case ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3:
                AwsS3.validateProperties(configuration, srcLoc, collector);
                break;
            case ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_BLOB:
                Azure.validateAzureBlobProperties(configuration, srcLoc, collector, appCtx);
                break;
            case ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_DATA_LAKE:
                Azure.validateAzureDataLakeProperties(configuration, srcLoc, collector, appCtx);
                break;
            case KEY_ADAPTER_NAME_GCS:
                GCS.validateProperties(configuration, srcLoc, collector);
                break;
            default:
                // Nothing needs to be done
                break;
        }
    }

    /**
     * Regex matches all the provided patterns against the provided path
     *
     * @param path path to check against
     * @return {@code true} if all patterns match, {@code false} otherwise
     */
    public static boolean matchPatterns(List<Matcher> matchers, String path) {
        for (Matcher matcher : matchers) {
            if (matcher.reset(path).matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts the wildcard to proper regex
     *
     * @param pattern wildcard pattern to convert
     * @return regex expression
     */
    public static String patternToRegex(String pattern) {
        int charPosition = 0;
        int patternLength = pattern.length();
        StringBuilder stuffBuilder = new StringBuilder();
        StringBuilder result = new StringBuilder();

        while (charPosition < patternLength) {
            char c = pattern.charAt(charPosition);
            charPosition++;

            switch (c) {
                case '*':
                    result.append(".*");
                    break;
                case '?':
                    result.append(".");
                    break;
                case '[':
                    int closingBracketPosition = charPosition;
                    if (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) == '!') {
                        closingBracketPosition++;
                    }

                    // 2 cases can happen here:
                    // 1- Empty character class [] which is invalid for java, so treat ] as literal and find another
                    // closing bracket, if no closing bracket is found, the whole thing is a literal
                    // 2- Negated empty class [!] converted to [^] which is invalid for java, so treat ] as literal and
                    // find another closing bracket, if no closing bracket is found, the whole thing is a literal
                    if (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) == ']') {
                        closingBracketPosition++;
                    }

                    // No [] and [!] cases, search for the closing bracket
                    while (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) != ']') {
                        closingBracketPosition++;
                    }

                    // No closing bracket found (or [] or [!]), escape the opening bracket, treat it as literals
                    if (closingBracketPosition >= patternLength) {
                        result.append("\\[");
                    } else {
                        // Found closing bracket, get the stuff in between the found the character class ("[" and "]")
                        String stuff = pattern.substring(charPosition, closingBracketPosition);

                        stuffBuilder.setLength(0);
                        int stuffCharPos = 0;

                        // If first character in the character class is "!" then convert it to "^"
                        if (stuff.charAt(0) == '!') {
                            stuffBuilder.append('^');
                            stuffCharPos++; // ignore first character when escaping metacharacters next step
                        }

                        for (; stuffCharPos < stuff.length(); stuffCharPos++) {
                            char stuffChar = stuff.charAt(stuffCharPos);
                            if (stuffChar != '-' && Arrays.binarySearch(RESERVED_REGEX_CHARS, stuffChar) >= 0) {
                                stuffBuilder.append("\\");
                            }
                            stuffBuilder.append(stuffChar);
                        }

                        String stuffEscaped = stuffBuilder.toString();

                        // Escape the set operations
                        stuffEscaped = stuffEscaped.replace("&&", "\\&\\&").replace("~~", "\\~\\~")
                                .replace("||", "\\|\\|").replace("--", "\\-\\-");

                        result.append("[").append(stuffEscaped).append("]");
                        charPosition = closingBracketPosition + 1;
                    }
                    break;
                default:
                    if (Arrays.binarySearch(RESERVED_REGEX_CHARS, c) >= 0) {
                        result.append("\\");
                    }
                    result.append(c);
                    break;
            }
        }

        return result.toString();
    }

    /**
     * Adjusts the prefix (if needed) and returns it
     *
     * @param configuration configuration
     */
    public static String getPrefix(Map<String, String> configuration) {
        return getPrefix(configuration, true);
    }

    public static String getPrefix(Map<String, String> configuration, boolean appendSlash) {
        String definition = configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);
        if (definition != null && !definition.isEmpty()) {
            return appendSlash ? definition + (!definition.endsWith("/") ? "/" : "") : definition;
        }
        return "";
    }

    /**
     * @param configuration configuration map
     * @throws CompilationException Compilation exception
     */
    public static void validateIncludeExclude(Map<String, String> configuration) throws CompilationException {
        // Ensure that include and exclude are not provided at the same time + ensure valid format or property
        List<Map.Entry<String, String>> includes = new ArrayList<>();
        List<Map.Entry<String, String>> excludes = new ArrayList<>();

        // Accepted formats are include, include#1, include#2, ... etc, same for excludes
        for (Map.Entry<String, String> entry : configuration.entrySet()) {
            String key = entry.getKey();

            if (key.equals(ExternalDataConstants.KEY_INCLUDE)) {
                includes.add(entry);
            } else if (key.equals(ExternalDataConstants.KEY_EXCLUDE)) {
                excludes.add(entry);
            } else if (key.startsWith(ExternalDataConstants.KEY_INCLUDE)
                    || key.startsWith(ExternalDataConstants.KEY_EXCLUDE)) {

                // Split by the "#", length should be 2, left should be include/exclude, right should be integer
                String[] splits = key.split("#");

                if (key.startsWith(ExternalDataConstants.KEY_INCLUDE) && splits.length == 2
                        && splits[0].equals(ExternalDataConstants.KEY_INCLUDE)
                        && NumberUtils.isIntegerNumericString(splits[1])) {
                    includes.add(entry);
                } else if (key.startsWith(ExternalDataConstants.KEY_EXCLUDE) && splits.length == 2
                        && splits[0].equals(ExternalDataConstants.KEY_EXCLUDE)
                        && NumberUtils.isIntegerNumericString(splits[1])) {
                    excludes.add(entry);
                } else {
                    throw new CompilationException(ErrorCode.INVALID_PROPERTY_FORMAT, key);
                }
            }
        }

        // Ensure either include or exclude are provided, but not both of them
        if (!includes.isEmpty() && !excludes.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_NOT_ALLOWED_AT_SAME_TIME,
                    ExternalDataConstants.KEY_INCLUDE, ExternalDataConstants.KEY_EXCLUDE);
        }
    }

    public static IncludeExcludeMatcher getIncludeExcludeMatchers(Map<String, String> configuration)
            throws CompilationException {
        // Get and compile the patterns for include/exclude if provided
        List<Matcher> includeMatchers = new ArrayList<>();
        List<Matcher> excludeMatchers = new ArrayList<>();
        String pattern = null;
        try {
            for (Map.Entry<String, String> entry : configuration.entrySet()) {
                if (entry.getKey().startsWith(KEY_INCLUDE)) {
                    pattern = entry.getValue();
                    includeMatchers.add(Pattern.compile(patternToRegex(pattern)).matcher(""));
                } else if (entry.getKey().startsWith(KEY_EXCLUDE)) {
                    pattern = entry.getValue();
                    excludeMatchers.add(Pattern.compile(patternToRegex(pattern)).matcher(""));
                }
            }
        } catch (PatternSyntaxException ex) {
            throw new CompilationException(ErrorCode.INVALID_REGEX_PATTERN, pattern);
        }

        IncludeExcludeMatcher includeExcludeMatcher;
        if (!includeMatchers.isEmpty()) {
            includeExcludeMatcher =
                    new IncludeExcludeMatcher(includeMatchers, (matchers1, key) -> matchPatterns(matchers1, key));
        } else if (!excludeMatchers.isEmpty()) {
            includeExcludeMatcher =
                    new IncludeExcludeMatcher(excludeMatchers, (matchers1, key) -> !matchPatterns(matchers1, key));
        } else {
            includeExcludeMatcher = new IncludeExcludeMatcher(Collections.emptyList(), (matchers1, key) -> true);
        }

        return includeExcludeMatcher;
    }

    public static boolean supportsPushdown(Map<String, String> properties) {
        //Currently, only Apache Parquet format is supported
        return isParquetFormat(properties);
    }

    /**
     * Validate Parquet dataset's declared type and configuration
     *
     * @param properties        external dataset configuration
     * @param datasetRecordType dataset declared type
     */
    public static void validateParquetTypeAndConfiguration(Map<String, String> properties,
            ARecordType datasetRecordType) throws CompilationException {
        if (isParquetFormat(properties)) {
            if (datasetRecordType.getFieldTypes().length != 0) {
                throw new CompilationException(ErrorCode.UNSUPPORTED_TYPE_FOR_PARQUET, datasetRecordType.getTypeName());
            } else if (properties.containsKey(ParquetOptions.TIMEZONE)
                    && !ParquetOptions.VALID_TIME_ZONES.contains(properties.get(ParquetOptions.TIMEZONE))) {
                //Ensure the configured time zone id is correct
                throw new CompilationException(ErrorCode.INVALID_TIMEZONE, properties.get(ParquetOptions.TIMEZONE));
            }
        }
    }

    private static boolean isParquetFormat(Map<String, String> properties) {
        String inputFormat = properties.get(ExternalDataConstants.KEY_INPUT_FORMAT);
        return ExternalDataConstants.CLASS_NAME_PARQUET_INPUT_FORMAT.equals(inputFormat)
                || ExternalDataConstants.INPUT_FORMAT_PARQUET.equals(inputFormat)
                || ExternalDataConstants.FORMAT_PARQUET.equals(properties.get(ExternalDataConstants.KEY_FORMAT));
    }

    public static void setExternalDataProjectionInfo(DataProjectionInfo projectionInfo, Map<String, String> properties)
            throws IOException {
        properties.put(ExternalDataConstants.KEY_REQUESTED_FIELDS,
                serializeExpectedTypeToString(projectionInfo.getProjectionInfo()));
        properties.put(ExternalDataConstants.KEY_HADOOP_ASTERIX_FUNCTION_CALL_INFORMATION,
                serializeFunctionCallInfoToString(projectionInfo.getFunctionCallInfoMap()));
    }

    /**
     * Serialize {@link ARecordType} as Base64 string to pass it to {@link org.apache.hadoop.conf.Configuration}
     *
     * @param expectedType expected type
     * @return the expected type as Base64 string
     */
    private static String serializeExpectedTypeToString(ARecordType expectedType) throws IOException {
        if (expectedType == DataProjectionInfo.EMPTY_TYPE || expectedType == DataProjectionInfo.ALL_FIELDS_TYPE) {
            //Return the type name of EMPTY_TYPE and ALL_FIELDS_TYPE
            return expectedType.getTypeName();
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        Base64.Encoder encoder = Base64.getEncoder();
        DataProjectionInfo.writeTypeField(expectedType, dataOutputStream);
        return encoder.encodeToString(byteArrayOutputStream.toByteArray());
    }

    /**
     * Serialize {@link FunctionCallInformation} map as Base64 string to pass it to
     * {@link org.apache.hadoop.conf.Configuration}
     *
     * @param functionCallInfoMap function information map
     * @return function information map as Base64 string
     */
    static String serializeFunctionCallInfoToString(Map<String, FunctionCallInformation> functionCallInfoMap)
            throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        Base64.Encoder encoder = Base64.getEncoder();
        DataProjectionInfo.writeFunctionCallInformationMapField(functionCallInfoMap, dataOutputStream);
        return encoder.encodeToString(byteArrayOutputStream.toByteArray());
    }

    public static class AwsS3 {
        private AwsS3() {
            throw new AssertionError("do not instantiate");
        }

        /**
         * Builds the S3 client using the provided configuration
         *
         * @param configuration properties
         * @return S3 client
         * @throws CompilationException CompilationException
         */
        public static S3Client buildAwsS3Client(Map<String, String> configuration) throws CompilationException {
            // TODO(Hussain): Need to ensure that all required parameters are present in a previous step
            String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
            String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
            String sessionToken = configuration.get(ExternalDataConstants.AwsS3.SESSION_TOKEN_FIELD_NAME);
            String regionId = configuration.get(ExternalDataConstants.AwsS3.REGION_FIELD_NAME);
            String serviceEndpoint = configuration.get(ExternalDataConstants.AwsS3.SERVICE_END_POINT_FIELD_NAME);

            S3ClientBuilder builder = S3Client.builder();

            // Credentials
            AwsCredentialsProvider credentialsProvider;

            // No auth required
            if (accessKeyId == null) {
                credentialsProvider = AnonymousCredentialsProvider.create();
            } else {
                // auth required, check for temporary or permanent credentials
                if (sessionToken != null) {
                    credentialsProvider = StaticCredentialsProvider
                            .create(AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
                } else {
                    credentialsProvider =
                            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
                }
            }

            builder.credentialsProvider(credentialsProvider);

            // Validate the region
            List<Region> regions = S3Client.serviceMetadata().regions();
            Optional<Region> selectedRegion =
                    regions.stream().filter(region -> region.id().equals(regionId)).findFirst();

            if (selectedRegion.isEmpty()) {
                throw new CompilationException(S3_REGION_NOT_SUPPORTED, regionId);
            }
            builder.region(selectedRegion.get());

            // Validate the service endpoint if present
            if (serviceEndpoint != null) {
                try {
                    URI uri = new URI(serviceEndpoint);
                    try {
                        builder.endpointOverride(uri);
                    } catch (NullPointerException ex) {
                        throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
                    }
                } catch (URISyntaxException ex) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR,
                            String.format("Invalid service endpoint %s", serviceEndpoint));
                }
            }

            return builder.build();
        }

        /**
         * Builds the S3 client using the provided configuration
         *
         * @param configuration      properties
         * @param numberOfPartitions number of partitions in the cluster
         */
        public static void configureAwsS3HdfsJobConf(JobConf conf, Map<String, String> configuration,
                int numberOfPartitions) {
            String accessKeyId = configuration.get(ExternalDataConstants.AwsS3.ACCESS_KEY_ID_FIELD_NAME);
            String secretAccessKey = configuration.get(ExternalDataConstants.AwsS3.SECRET_ACCESS_KEY_FIELD_NAME);
            String sessionToken = configuration.get(ExternalDataConstants.AwsS3.SESSION_TOKEN_FIELD_NAME);
            String serviceEndpoint = configuration.get(ExternalDataConstants.AwsS3.SERVICE_END_POINT_FIELD_NAME);

            //Disable caching S3 FileSystem
            HDFSUtils.disableHadoopFileSystemCache(conf, HADOOP_S3_PROTOCOL);

            /*
             * Authentication Methods:
             * 1- Anonymous: no accessKeyId and no secretAccessKey
             * 2- Temporary: has to provide accessKeyId, secretAccessKey and sessionToken
             * 3- Private: has to provide accessKeyId and secretAccessKey
             */
            if (accessKeyId == null) {
                //Tells hadoop-aws it is an anonymous access
                conf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_ANONYMOUS_ACCESS);
            } else {
                conf.set(HADOOP_ACCESS_KEY_ID, accessKeyId);
                conf.set(HADOOP_SECRET_ACCESS_KEY, secretAccessKey);
                if (sessionToken != null) {
                    conf.set(HADOOP_SESSION_TOKEN, sessionToken);
                    //Tells hadoop-aws it is a temporary access
                    conf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_TEMP_ACCESS);
                }
            }

            /*
             * This is to allow S3 definition to have path-style form. Should always be true to match the current
             * way we access files in S3
             */
            conf.set(HADOOP_PATH_STYLE_ACCESS, ExternalDataConstants.TRUE);

            /*
             * Set the size of S3 connection pool to be the number of partitions
             */
            conf.set(HADOOP_S3_CONNECTION_POOL_SIZE, String.valueOf(numberOfPartitions));

            if (serviceEndpoint != null) {
                // Validation of the URL should be done at hadoop-aws level
                conf.set(ExternalDataConstants.AwsS3.HADOOP_SERVICE_END_POINT, serviceEndpoint);
            } else {
                //Region is ignored and buckets could be found by the central endpoint
                conf.set(ExternalDataConstants.AwsS3.HADOOP_SERVICE_END_POINT, Constants.CENTRAL_ENDPOINT);
            }
        }

        /**
         * Validate external dataset properties
         *
         * @param configuration properties
         * @throws CompilationException Compilation exception
         */
        public static void validateProperties(Map<String, String> configuration, SourceLocation srcLoc,
                IWarningCollector collector) throws CompilationException {

            // check if the format property is present
            if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
                throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
            }

            // Both parameters should be passed, or neither should be passed (for anonymous/no auth)
            String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
            String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
            if (accessKeyId == null || secretAccessKey == null) {
                // If one is passed, the other is required
                if (accessKeyId != null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, SECRET_ACCESS_KEY_FIELD_NAME,
                            ACCESS_KEY_ID_FIELD_NAME);
                } else if (secretAccessKey != null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCESS_KEY_ID_FIELD_NAME,
                            SECRET_ACCESS_KEY_FIELD_NAME);
                }
            }

            validateIncludeExclude(configuration);

            // Check if the bucket is present
            S3Client s3Client = buildAwsS3Client(configuration);
            S3Response response;
            boolean useOldApi = false;
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
            String prefix = getPrefix(configuration);

            try {
                response = isBucketEmpty(s3Client, container, prefix, false);
            } catch (S3Exception ex) {
                // Method not implemented, try falling back to old API
                try {
                    // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                    if (ex.awsErrorDetails().errorCode().equals(ERROR_METHOD_NOT_IMPLEMENTED)) {
                        useOldApi = true;
                        response = isBucketEmpty(s3Client, container, prefix, true);
                    } else {
                        throw ex;
                    }
                } catch (SdkException ex2) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
                }
            } catch (SdkException ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            } finally {
                if (s3Client != null) {
                    CleanupUtils.close(s3Client, null);
                }
            }

            boolean isEmpty = useOldApi ? ((ListObjectsResponse) response).contents().isEmpty()
                    : ((ListObjectsV2Response) response).contents().isEmpty();
            if (isEmpty && collector.shouldWarn()) {
                Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                collector.warn(warning);
            }

            // Returns 200 only in case the bucket exists, otherwise, throws an exception. However, to
            // ensure coverage, check if the result is successful as well and not only catch exceptions
            if (!response.sdkHttpResponse().isSuccessful()) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_CONTAINER_NOT_FOUND, container);
            }
        }

        /**
         * Checks for a single object in the specified bucket to determine if the bucket is empty or not.
         *
         * @param s3Client  s3 client
         * @param container the container name
         * @param prefix    Prefix to be used
         * @param useOldApi flag whether to use the old API or not
         * @return returns the S3 response
         */
        private static S3Response isBucketEmpty(S3Client s3Client, String container, String prefix, boolean useOldApi) {
            S3Response response;
            if (useOldApi) {
                ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder();
                listObjectsBuilder.prefix(prefix);
                response = s3Client.listObjects(listObjectsBuilder.bucket(container).maxKeys(1).build());
            } else {
                ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder();
                listObjectsBuilder.prefix(prefix);
                response = s3Client.listObjectsV2(listObjectsBuilder.bucket(container).maxKeys(1).build());
            }
            return response;
        }

        /**
         * Returns the lists of S3 objects.
         *
         * @param configuration         properties
         * @param includeExcludeMatcher include/exclude matchers to apply
         */
        public static List<S3Object> listS3Objects(Map<String, String> configuration,
                IncludeExcludeMatcher includeExcludeMatcher, IWarningCollector warningCollector)
                throws CompilationException {
            // Prepare to retrieve the objects
            List<S3Object> filesOnly;
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
            S3Client s3Client = buildAwsS3Client(configuration);
            String prefix = getPrefix(configuration);

            try {
                filesOnly = listS3Objects(s3Client, container, prefix, includeExcludeMatcher);
            } catch (S3Exception ex) {
                // New API is not implemented, try falling back to old API
                try {
                    // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                    if (ex.awsErrorDetails().errorCode()
                            .equals(ExternalDataConstants.AwsS3.ERROR_METHOD_NOT_IMPLEMENTED)) {
                        filesOnly = oldApiListS3Objects(s3Client, container, prefix, includeExcludeMatcher);
                    } else {
                        throw ex;
                    }
                } catch (SdkException ex2) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
                }
            } catch (SdkException ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            } finally {
                if (s3Client != null) {
                    CleanupUtils.close(s3Client, null);
                }
            }

            // Warn if no files are returned
            if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
                Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                warningCollector.warn(warning);
            }

            return filesOnly;
        }

        /**
         * Uses the latest API to retrieve the objects from the storage.
         *
         * @param s3Client              S3 client
         * @param container             container name
         * @param prefix                definition prefix
         * @param includeExcludeMatcher include/exclude matchers to apply
         */
        private static List<S3Object> listS3Objects(S3Client s3Client, String container, String prefix,
                IncludeExcludeMatcher includeExcludeMatcher) {
            String newMarker = null;
            List<S3Object> filesOnly = new ArrayList<>();

            ListObjectsV2Response listObjectsResponse;
            ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder().bucket(container);
            listObjectsBuilder.prefix(prefix);

            while (true) {
                // List the objects from the start, or from the last marker in case of truncated result
                if (newMarker == null) {
                    listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.build());
                } else {
                    listObjectsResponse =
                            s3Client.listObjectsV2(listObjectsBuilder.continuationToken(newMarker).build());
                }

                // Collect the paths to files only
                collectAndFilterFiles(listObjectsResponse.contents(), includeExcludeMatcher.getPredicate(),
                        includeExcludeMatcher.getMatchersList(), filesOnly);

                // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
                if (listObjectsResponse.isTruncated() != null && listObjectsResponse.isTruncated()) {
                    newMarker = listObjectsResponse.nextContinuationToken();
                } else {
                    break;
                }
            }

            return filesOnly;
        }

        /**
         * Uses the old API (in case the new API is not implemented) to retrieve the objects from the storage
         *
         * @param s3Client              S3 client
         * @param container             container name
         * @param prefix                definition prefix
         * @param includeExcludeMatcher include/exclude matchers to apply
         */
        private static List<S3Object> oldApiListS3Objects(S3Client s3Client, String container, String prefix,
                IncludeExcludeMatcher includeExcludeMatcher) {
            String newMarker = null;
            List<S3Object> filesOnly = new ArrayList<>();

            ListObjectsResponse listObjectsResponse;
            ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder().bucket(container);
            listObjectsBuilder.prefix(prefix);

            while (true) {
                // List the objects from the start, or from the last marker in case of truncated result
                if (newMarker == null) {
                    listObjectsResponse = s3Client.listObjects(listObjectsBuilder.build());
                } else {
                    listObjectsResponse = s3Client.listObjects(listObjectsBuilder.marker(newMarker).build());
                }

                // Collect the paths to files only
                collectAndFilterFiles(listObjectsResponse.contents(), includeExcludeMatcher.getPredicate(),
                        includeExcludeMatcher.getMatchersList(), filesOnly);

                // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
                if (!listObjectsResponse.isTruncated()) {
                    break;
                } else {
                    newMarker = listObjectsResponse.nextMarker();
                }
            }

            return filesOnly;
        }

        /**
         * AWS S3 returns all the objects as paths, not differentiating between folder and files. The path is considered
         * a file if it does not end up with a "/" which is the separator in a folder structure.
         *
         * @param s3Objects List of returned objects
         */
        private static void collectAndFilterFiles(List<S3Object> s3Objects,
                BiPredicate<List<Matcher>, String> predicate, List<Matcher> matchers, List<S3Object> filesOnly) {
            for (S3Object object : s3Objects) {
                // skip folders
                if (object.key().endsWith("/")) {
                    continue;
                }

                // No filter, add file
                if (predicate.test(matchers, object.key())) {
                    filesOnly.add(object);
                }
            }
        }
    }

    /*
     * Note: Azure Blob and Azure Datalake use identical authentication, so they are using the same properties.
     * If they end up diverging, then properties for AzureBlob and AzureDataLake need to be created.
     */
    public static class Azure {
        private Azure() {
            throw new AssertionError("do not instantiate");
        }

        /**
         * Builds the Azure storage account using the provided configuration
         *
         * @param configuration properties
         * @return client
         */
        public static BlobServiceClient buildAzureBlobClient(IApplicationContext appCtx,
                Map<String, String> configuration) throws CompilationException {
            String managedIdentityId = configuration.get(MANAGED_IDENTITY_ID_FIELD_NAME);
            String accountName = configuration.get(ACCOUNT_NAME_FIELD_NAME);
            String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
            String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);
            String tenantId = configuration.get(TENANT_ID_FIELD_NAME);
            String clientId = configuration.get(CLIENT_ID_FIELD_NAME);
            String clientSecret = configuration.get(CLIENT_SECRET_FIELD_NAME);
            String clientCertificate = configuration.get(CLIENT_CERTIFICATE_FIELD_NAME);
            String clientCertificatePassword = configuration.get(CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME);
            String endpoint = configuration.get(ENDPOINT_FIELD_NAME);

            // Client builder
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
            int timeout = appCtx.getExternalProperties().getAzureRequestTimeout();
            RequestRetryOptions requestRetryOptions = new RequestRetryOptions(null, null, timeout, null, null, null);
            builder.retryOptions(requestRetryOptions);

            // Endpoint is required
            if (endpoint == null) {
                throw new CompilationException(PARAMETERS_REQUIRED, ENDPOINT_FIELD_NAME);
            }
            builder.endpoint(endpoint);

            // Shared Key
            if (accountName != null || accountKey != null) {
                if (accountName == null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCOUNT_NAME_FIELD_NAME,
                            ACCOUNT_KEY_FIELD_NAME);
                }

                if (accountKey == null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCOUNT_KEY_FIELD_NAME,
                            ACCOUNT_NAME_FIELD_NAME);
                }

                Optional<String> provided = getFirstNotNull(configuration, SHARED_ACCESS_SIGNATURE_FIELD_NAME,
                        MANAGED_IDENTITY_ID_FIELD_NAME, CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME,
                        CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
                if (provided.isPresent()) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                            ACCOUNT_KEY_FIELD_NAME);
                }
                StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
                builder.credential(credential);
            }

            // Shared access signature
            if (sharedAccessSignature != null) {
                Optional<String> provided = getFirstNotNull(configuration, MANAGED_IDENTITY_ID_FIELD_NAME,
                        CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME,
                        CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
                if (provided.isPresent()) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                            SHARED_ACCESS_SIGNATURE_FIELD_NAME);
                }
                AzureSasCredential credential = new AzureSasCredential(sharedAccessSignature);
                builder.credential(credential);
            }

            // Managed Identity auth
            if (managedIdentityId != null) {
                Optional<String> provided = getFirstNotNull(configuration, CLIENT_ID_FIELD_NAME,
                        CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME,
                        TENANT_ID_FIELD_NAME);
                if (provided.isPresent()) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                            MANAGED_IDENTITY_ID_FIELD_NAME);
                }
                builder.credential(new ManagedIdentityCredentialBuilder().clientId(managedIdentityId).build());
            }

            // Client secret & certificate auth
            if (clientId != null) {
                // Both (or neither) client secret and client secret were provided, only one is allowed
                if ((clientSecret == null) == (clientCertificate == null)) {
                    if (clientSecret != null) {
                        throw new CompilationException(PARAMETERS_NOT_ALLOWED_AT_SAME_TIME, CLIENT_SECRET_FIELD_NAME,
                                CLIENT_CERTIFICATE_FIELD_NAME);
                    } else {
                        throw new CompilationException(REQUIRED_PARAM_OR_PARAM_IF_PARAM_IS_PRESENT,
                                CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_ID_FIELD_NAME);
                    }
                }

                // Tenant ID is required
                if (tenantId == null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, TENANT_ID_FIELD_NAME,
                            CLIENT_ID_FIELD_NAME);
                }

                // Client certificate password is not allowed if client secret is used
                if (clientCertificatePassword != null && clientSecret != null) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT,
                            CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, CLIENT_SECRET_FIELD_NAME);
                }

                // Use AD authentication
                if (clientSecret != null) {
                    ClientSecretCredentialBuilder secret = new ClientSecretCredentialBuilder();
                    secret.clientId(clientId);
                    secret.tenantId(tenantId);
                    secret.clientSecret(clientSecret);
                    builder.credential(secret.build());
                } else {
                    // Certificate
                    ClientCertificateCredentialBuilder certificate = new ClientCertificateCredentialBuilder();
                    certificate.clientId(clientId);
                    certificate.tenantId(tenantId);
                    try {
                        InputStream certificateContent = new ByteArrayInputStream(clientCertificate.getBytes(UTF_8));
                        if (clientCertificatePassword == null) {
                            Method pemCertificate = ClientCertificateCredentialBuilder.class
                                    .getDeclaredMethod("pemCertificate", InputStream.class);
                            pemCertificate.setAccessible(true);
                            pemCertificate.invoke(certificate, certificateContent);
                        } else {
                            Method pemCertificate = ClientCertificateCredentialBuilder.class
                                    .getDeclaredMethod("pfxCertificate", InputStream.class, String.class);
                            pemCertificate.setAccessible(true);
                            pemCertificate.invoke(certificate, certificateContent, clientCertificatePassword);
                        }
                    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
                        throw new CompilationException(EXTERNAL_SOURCE_ERROR, ex.getMessage());
                    }
                    builder.credential(certificate.build());
                }
            }

            // If client id is not present, ensure client secret, certificate, tenant id and client certificate
            // password are not present
            if (clientId == null) {
                Optional<String> provided = getFirstNotNull(configuration, CLIENT_SECRET_FIELD_NAME,
                        CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
                if (provided.isPresent()) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                            SHARED_ACCESS_SIGNATURE_FIELD_NAME);
                }
            }

            try {
                return builder.buildClient();
            } catch (Exception ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }
        }

        /**
         * Builds the Azure data lake storage account using the provided configuration
         *
         * @param configuration properties
         * @return client
         */
        public static DataLakeServiceClient buildAzureDatalakeClient(IApplicationContext appCtx,
                Map<String, String> configuration) throws CompilationException {
            String managedIdentityId = configuration.get(MANAGED_IDENTITY_ID_FIELD_NAME);
            String accountName = configuration.get(ACCOUNT_NAME_FIELD_NAME);
            String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
            String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);
            String tenantId = configuration.get(TENANT_ID_FIELD_NAME);
            String clientId = configuration.get(CLIENT_ID_FIELD_NAME);
            String clientSecret = configuration.get(CLIENT_SECRET_FIELD_NAME);
            String clientCertificate = configuration.get(CLIENT_CERTIFICATE_FIELD_NAME);
            String clientCertificatePassword = configuration.get(CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME);
            String endpoint = configuration.get(ENDPOINT_FIELD_NAME);

            // Client builder
            DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
            int timeout = appCtx.getExternalProperties().getAzureRequestTimeout();
            RequestRetryOptions requestRetryOptions = new RequestRetryOptions(null, null, timeout, null, null, null);
            builder.retryOptions(requestRetryOptions);

            // Endpoint is required
            if (endpoint == null) {
                throw new CompilationException(PARAMETERS_REQUIRED, ENDPOINT_FIELD_NAME);
            }
            builder.endpoint(endpoint);

            // Shared Key
            if (accountName != null || accountKey != null) {
                if (accountName == null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCOUNT_NAME_FIELD_NAME,
                            ACCOUNT_KEY_FIELD_NAME);
                }

                if (accountKey == null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCOUNT_KEY_FIELD_NAME,
                            ACCOUNT_NAME_FIELD_NAME);
                }

                Optional<String> provided = getFirstNotNull(configuration, SHARED_ACCESS_SIGNATURE_FIELD_NAME,
                        MANAGED_IDENTITY_ID_FIELD_NAME, CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME,
                        CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
                if (provided.isPresent()) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                            ACCOUNT_KEY_FIELD_NAME);
                }
                StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
                builder.credential(credential);
            }

            // Shared access signature
            if (sharedAccessSignature != null) {
                Optional<String> provided = getFirstNotNull(configuration, MANAGED_IDENTITY_ID_FIELD_NAME,
                        CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME,
                        CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
                if (provided.isPresent()) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                            SHARED_ACCESS_SIGNATURE_FIELD_NAME);
                }
                AzureSasCredential credential = new AzureSasCredential(sharedAccessSignature);
                builder.credential(credential);
            }

            // Managed Identity auth
            if (managedIdentityId != null) {
                Optional<String> provided = getFirstNotNull(configuration, CLIENT_ID_FIELD_NAME,
                        CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME,
                        TENANT_ID_FIELD_NAME);
                if (provided.isPresent()) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                            MANAGED_IDENTITY_ID_FIELD_NAME);
                }
                builder.credential(new ManagedIdentityCredentialBuilder().clientId(managedIdentityId).build());
            }

            // Client secret & certificate auth
            if (clientId != null) {
                // Both (or neither) client secret and client secret were provided, only one is allowed
                if ((clientSecret == null) == (clientCertificate == null)) {
                    if (clientSecret != null) {
                        throw new CompilationException(PARAMETERS_NOT_ALLOWED_AT_SAME_TIME, CLIENT_SECRET_FIELD_NAME,
                                CLIENT_CERTIFICATE_FIELD_NAME);
                    } else {
                        throw new CompilationException(REQUIRED_PARAM_OR_PARAM_IF_PARAM_IS_PRESENT,
                                CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_ID_FIELD_NAME);
                    }
                }

                // Tenant ID is required
                if (tenantId == null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, TENANT_ID_FIELD_NAME,
                            CLIENT_ID_FIELD_NAME);
                }

                // Client certificate password is not allowed if client secret is used
                if (clientCertificatePassword != null && clientSecret != null) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT,
                            CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, CLIENT_SECRET_FIELD_NAME);
                }

                // Use AD authentication
                if (clientSecret != null) {
                    ClientSecretCredentialBuilder secret = new ClientSecretCredentialBuilder();
                    secret.clientId(clientId);
                    secret.tenantId(tenantId);
                    secret.clientSecret(clientSecret);
                    builder.credential(secret.build());
                } else {
                    // Certificate
                    ClientCertificateCredentialBuilder certificate = new ClientCertificateCredentialBuilder();
                    certificate.clientId(clientId);
                    certificate.tenantId(tenantId);
                    try {
                        InputStream certificateContent = new ByteArrayInputStream(clientCertificate.getBytes(UTF_8));
                        if (clientCertificatePassword == null) {
                            Method pemCertificate = ClientCertificateCredentialBuilder.class
                                    .getDeclaredMethod("pemCertificate", InputStream.class);
                            pemCertificate.setAccessible(true);
                            pemCertificate.invoke(certificate, certificateContent);
                        } else {
                            Method pemCertificate = ClientCertificateCredentialBuilder.class
                                    .getDeclaredMethod("pfxCertificate", InputStream.class, String.class);
                            pemCertificate.setAccessible(true);
                            pemCertificate.invoke(certificate, certificateContent, clientCertificatePassword);
                        }
                    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
                        throw new CompilationException(EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
                    }
                    builder.credential(certificate.build());
                }
            }

            // If client id is not present, ensure client secret, certificate, tenant id and client certificate
            // password are not present
            if (clientId == null) {
                Optional<String> provided = getFirstNotNull(configuration, CLIENT_SECRET_FIELD_NAME,
                        CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
                if (provided.isPresent()) {
                    throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                            SHARED_ACCESS_SIGNATURE_FIELD_NAME);
                }
            }

            try {
                return builder.buildClient();
            } catch (Exception ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }
        }

        public static List<BlobItem> listBlobItems(BlobServiceClient blobServiceClient,
                Map<String, String> configuration, IncludeExcludeMatcher includeExcludeMatcher,
                IWarningCollector warningCollector) throws CompilationException {
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

            List<BlobItem> filesOnly = new ArrayList<>();

            // Ensure the validity of include/exclude
            ExternalDataUtils.validateIncludeExclude(configuration);

            BlobContainerClient blobContainer;
            try {
                blobContainer = blobServiceClient.getBlobContainerClient(container);

                // Get all objects in a container and extract the paths to files
                ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
                listBlobsOptions.setPrefix(ExternalDataUtils.getPrefix(configuration));
                Iterable<BlobItem> blobItems = blobContainer.listBlobs(listBlobsOptions, null);

                // Collect the paths to files only
                collectAndFilterBlobFiles(blobItems, includeExcludeMatcher.getPredicate(),
                        includeExcludeMatcher.getMatchersList(), filesOnly);

                // Warn if no files are returned
                if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
                    Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                    warningCollector.warn(warning);
                }
            } catch (Exception ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }

            return filesOnly;
        }

        /**
         * Collects and filters the files only, and excludes any folders
         *
         * @param items     storage items
         * @param predicate predicate to test with for file filtration
         * @param matchers  include/exclude matchers to test against
         * @param filesOnly List containing the files only (excluding folders)
         */
        private static void collectAndFilterBlobFiles(Iterable<BlobItem> items,
                BiPredicate<List<Matcher>, String> predicate, List<Matcher> matchers, List<BlobItem> filesOnly) {
            for (BlobItem item : items) {
                String uri = item.getName();

                // skip folders
                if (uri.endsWith("/")) {
                    continue;
                }

                // No filter, add file
                if (predicate.test(matchers, uri)) {
                    filesOnly.add(item);
                }
            }
        }

        public static List<PathItem> listDatalakePathItems(DataLakeServiceClient client,
                Map<String, String> configuration, IncludeExcludeMatcher includeExcludeMatcher,
                IWarningCollector warningCollector) throws CompilationException {
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

            List<PathItem> filesOnly = new ArrayList<>();

            // Ensure the validity of include/exclude
            ExternalDataUtils.validateIncludeExclude(configuration);

            DataLakeFileSystemClient fileSystemClient;
            try {
                fileSystemClient = client.getFileSystemClient(container);

                // Get all objects in a container and extract the paths to files
                ListPathsOptions listOptions = new ListPathsOptions();
                boolean recursive = Boolean.parseBoolean(configuration.get(RECURSIVE_FIELD_NAME));
                listOptions.setRecursive(recursive);
                listOptions.setPath(ExternalDataUtils.getPrefix(configuration, false));
                PagedIterable<PathItem> pathItems = fileSystemClient.listPaths(listOptions, null);

                // Collect the paths to files only
                collectAndFilterDatalakeFiles(pathItems, includeExcludeMatcher.getPredicate(),
                        includeExcludeMatcher.getMatchersList(), filesOnly);

                // Warn if no files are returned
                if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
                    Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                    warningCollector.warn(warning);
                }
            } catch (Exception ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }

            return filesOnly;
        }

        /**
         * Collects and filters the files only, and excludes any folders
         *
         * @param items     storage items
         * @param predicate predicate to test with for file filtration
         * @param matchers  include/exclude matchers to test against
         * @param filesOnly List containing the files only (excluding folders)
         */
        private static void collectAndFilterDatalakeFiles(Iterable<PathItem> items,
                BiPredicate<List<Matcher>, String> predicate, List<Matcher> matchers, List<PathItem> filesOnly) {
            for (PathItem item : items) {
                String uri = item.getName();

                // skip folders
                if (uri.endsWith("/")) {
                    continue;
                }

                // No filter, add file
                if (predicate.test(matchers, uri)) {
                    filesOnly.add(item);
                }
            }
        }

        /**
         * Validate external dataset properties
         *
         * @param configuration properties
         * @throws CompilationException Compilation exception
         */
        public static void validateAzureBlobProperties(Map<String, String> configuration, SourceLocation srcLoc,
                IWarningCollector collector, IApplicationContext appCtx) throws CompilationException {

            // check if the format property is present
            if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
                throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
            }

            validateIncludeExclude(configuration);

            // Check if the bucket is present
            BlobServiceClient blobServiceClient;
            try {
                String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
                blobServiceClient = buildAzureBlobClient(appCtx, configuration);
                BlobContainerClient blobContainer = blobServiceClient.getBlobContainerClient(container);

                // Get all objects in a container and extract the paths to files
                ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
                listBlobsOptions.setPrefix(getPrefix(configuration));
                Iterable<BlobItem> blobItems = blobContainer.listBlobs(listBlobsOptions, null);

                if (!blobItems.iterator().hasNext() && collector.shouldWarn()) {
                    Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                    collector.warn(warning);
                }
            } catch (CompilationException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }
        }

        /**
         * Validate external dataset properties
         *
         * @param configuration properties
         * @throws CompilationException Compilation exception
         */
        public static void validateAzureDataLakeProperties(Map<String, String> configuration, SourceLocation srcLoc,
                IWarningCollector collector, IApplicationContext appCtx) throws CompilationException {

            // check if the format property is present
            if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
                throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
            }

            validateIncludeExclude(configuration);

            // Check if the bucket is present
            DataLakeServiceClient dataLakeServiceClient;
            try {
                String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
                dataLakeServiceClient = buildAzureDatalakeClient(appCtx, configuration);
                DataLakeFileSystemClient fileSystemClient = dataLakeServiceClient.getFileSystemClient(container);

                // Get all objects in a container and extract the paths to files
                ListPathsOptions listPathsOptions = new ListPathsOptions();
                listPathsOptions.setPath(getPrefix(configuration));
                Iterable<PathItem> blobItems = fileSystemClient.listPaths(listPathsOptions, null);

                if (!blobItems.iterator().hasNext() && collector.shouldWarn()) {
                    Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                    collector.warn(warning);
                }
            } catch (CompilationException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }
        }

        /**
         * Builds the Azure Blob storage client using the provided configuration
         *
         * @param configuration properties
         * @see <a href="https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage">Azure
         * Blob storage</a>
         */
        public static void configureAzureHdfsJobConf(JobConf conf, Map<String, String> configuration, String endPoint) {
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
            String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
            String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);

            //Disable caching S3 FileSystem
            HDFSUtils.disableHadoopFileSystemCache(conf, HADOOP_AZURE_BLOB_PROTOCOL);

            //Key for Hadoop configuration
            StringBuilder hadoopKey = new StringBuilder();
            //Value for Hadoop configuration
            String hadoopValue;
            if (accountKey != null || sharedAccessSignature != null) {
                if (accountKey != null) {
                    hadoopKey.append(HADOOP_AZURE_FS_ACCOUNT_KEY).append('.');
                    //Set only the AccountKey
                    hadoopValue = accountKey;
                } else {
                    //Use SAS for Hadoop FS as connectionString is provided
                    hadoopKey.append(HADOOP_AZURE_FS_SAS).append('.');
                    //Setting the container is required for SAS
                    hadoopKey.append(container).append('.');
                    //Set the connection string for SAS
                    hadoopValue = sharedAccessSignature;
                }
                //Set the endPoint, which includes the AccountName
                hadoopKey.append(endPoint);
                //Tells Hadoop we are reading from Blob Storage
                conf.set(hadoopKey.toString(), hadoopValue);
            }
        }
    }

    public static class GCS {
        private GCS() {
            throw new AssertionError("do not instantiate");

        }

        //TODO(htowaileb): Add validation step similar to other externals, which also checks if empty bucket
        //upon creating the external dataset

        /**
         * Builds the client using the provided configuration
         *
         * @param configuration properties
         * @return clientasterixdb/asterix-external-data/src/main/java/org/apache/asterix/external/util/ExternalDataUtils.java
         * @throws CompilationException CompilationException
         */
        public static Storage buildClient(Map<String, String> configuration) throws CompilationException {
            String jsonCredentials = configuration.get(JSON_CREDENTIALS_FIELD_NAME);

            StorageOptions.Builder builder = StorageOptions.newBuilder();

            // Use credentials if available
            if (jsonCredentials != null) {
                try (InputStream credentialsStream = new ByteArrayInputStream(jsonCredentials.getBytes())) {
                    builder.setCredentials(ServiceAccountCredentials.fromStream(credentialsStream));
                } catch (IOException ex) {
                    throw new CompilationException(EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
                }
            }

            return builder.build().getService();
        }

        /**
         * Validate external dataset properties
         *
         * @param configuration properties
         * @throws CompilationException Compilation exception
         */
        public static void validateProperties(Map<String, String> configuration, SourceLocation srcLoc,
                IWarningCollector collector) throws CompilationException {

            // check if the format property is present
            if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
                throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
            }

            // parquet is not supported for google cloud storage
            if (isParquetFormat(configuration)) {
                throw new CompilationException(INVALID_REQ_PARAM_VAL, srcLoc, KEY_FORMAT,
                        configuration.get(KEY_FORMAT));
            }

            validateIncludeExclude(configuration);
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

            try {
                BlobListOption limitOption = BlobListOption.pageSize(1);
                BlobListOption prefixOption = BlobListOption.prefix(getPrefix(configuration));
                Storage storage = buildClient(configuration);
                Page<Blob> items = storage.list(container, limitOption, prefixOption);

                if (!items.iterateAll().iterator().hasNext() && collector.shouldWarn()) {
                    Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                    collector.warn(warning);
                }
            } catch (CompilationException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }
        }
    }

    public static int roundUpToNearestFrameSize(int size, int framesize) {
        return ((size / framesize) + 1) * framesize;
    }

    public static int getArgBufferSize() {
        int maxArgSz = DEFAULT_MAX_ARGUMENT_SZ + HEADER_FUDGE;
        String userArgSz = System.getProperty("udf.buf.size");
        if (userArgSz != null) {
            long parsedSize = StorageUtil.getByteValue(userArgSz) + HEADER_FUDGE;
            if (parsedSize < Integer.MAX_VALUE && parsedSize > 0) {
                maxArgSz = (int) parsedSize;
            }
        }
        return maxArgSz;
    }

    private static Optional<String> getFirstNotNull(Map<String, String> configuration, String... parameters) {
        return Arrays.stream(parameters).filter(field -> configuration.get(field) != null).findFirst();
    }
}
