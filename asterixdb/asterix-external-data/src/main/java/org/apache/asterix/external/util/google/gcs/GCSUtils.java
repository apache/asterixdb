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
package org.apache.asterix.external.util.google.gcs;

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_PARAM_VALUE_ALLOWED_VALUE;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.ENDPOINT_FIELD_NAME;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_AUTH_SERVICE_ACCOUNT_JSON_KEY_FILE;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_AUTH_SERVICE_ACCOUNT_JSON_KEY_FILE_PATH;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_AUTH_TYPE;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_AUTH_UNAUTHENTICATED;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_ENDPOINT;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_GCS_PROTOCOL;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.JSON_CREDENTIALS_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory.IncludeExcludeMatcher;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.BaseServiceException;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GCSUtils {
    private GCSUtils() {
        throw new AssertionError("do not instantiate");

    }

    /**
     * Builds the client using the provided configuration
     *
     * @param configuration properties
     * @return clientasterixdb/asterix-external-data/src/main/java/org/apache/asterix/external/util/ExternalDataUtils.java
     * @throws CompilationException CompilationException
     */
    public static Storage buildClient(Map<String, String> configuration) throws CompilationException {
        String applicationDefaultCredentials = configuration.get(APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME);
        String jsonCredentials = configuration.get(JSON_CREDENTIALS_FIELD_NAME);
        String endpoint = configuration.get(ENDPOINT_FIELD_NAME);

        StorageOptions.Builder builder = StorageOptions.newBuilder();

        // default credentials provider
        if (applicationDefaultCredentials != null) {
            // only "true" value is allowed
            if (!applicationDefaultCredentials.equalsIgnoreCase("true")) {
                throw new CompilationException(INVALID_PARAM_VALUE_ALLOWED_VALUE,
                        APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME, "true");
            }

            // no other authentication parameters are allowed
            if (jsonCredentials != null) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, JSON_CREDENTIALS_FIELD_NAME,
                        APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME);
            }

            try {
                builder.setCredentials(GoogleCredentials.getApplicationDefault());
            } catch (IOException ex) {
                throw CompilationException.create(EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
            }
        } else if (jsonCredentials != null) {
            try (InputStream credentialsStream = new ByteArrayInputStream(jsonCredentials.getBytes())) {
                builder.setCredentials(GoogleCredentials.fromStream(credentialsStream));
            } catch (IOException ex) {
                throw new CompilationException(EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
            }
        } else {
            builder.setCredentials(NoCredentials.getInstance());
        }

        if (endpoint != null) {
            builder.setHost(endpoint);
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

        validateIncludeExclude(configuration);
        try {
            // TODO(htowaileb): maybe something better, this will check to ensure type is supported before creation
            new ExternalDataPrefix(configuration);
        } catch (AlgebricksException ex) {
            throw new CompilationException(ErrorCode.FAILED_TO_CALCULATE_COMPUTED_FIELDS, ex);
        }

        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

        try {
            Storage.BlobListOption limitOption = Storage.BlobListOption.pageSize(1);
            Storage.BlobListOption prefixOption = Storage.BlobListOption.prefix(getPrefix(configuration));
            Storage storage = buildClient(configuration);
            Page<Blob> items = storage.list(container, limitOption, prefixOption);

            if (!items.iterateAll().iterator().hasNext() && collector.shouldWarn()) {
                Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                collector.warn(warning);
            }
        } catch (CompilationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    public static List<Blob> listItems(Map<String, String> configuration, IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator) throws CompilationException, HyracksDataException {
        // Prepare to retrieve the objects
        List<Blob> filesOnly = new ArrayList<>();
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        Storage gcs = buildClient(configuration);
        Storage.BlobListOption options = Storage.BlobListOption.prefix(ExternalDataUtils.getPrefix(configuration));
        Page<Blob> items;

        try {
            items = gcs.list(container, options);
        } catch (BaseServiceException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }

        // Collect the paths to files only
        collectAndFilterFiles(items, includeExcludeMatcher.getPredicate(), includeExcludeMatcher.getMatchersList(),
                filesOnly, externalDataPrefix, evaluator, warningCollector);

        // Warn if no files are returned
        if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
            Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
            warningCollector.warn(warning);
        }

        return filesOnly;
    }

    /**
     * Excludes paths ending with "/" as that's a directory indicator, we need to return the files only
     *
     * @param items List of returned objects
     */
    private static void collectAndFilterFiles(Page<Blob> items, BiPredicate<List<Matcher>, String> predicate,
            List<Matcher> matchers, List<Blob> filesOnly, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator, IWarningCollector warningCollector) throws HyracksDataException {
        for (Blob item : items.iterateAll()) {
            if (ExternalDataUtils.evaluate(item.getName(), predicate, matchers, externalDataPrefix, evaluator,
                    warningCollector)) {
                filesOnly.add(item);
            }
        }
    }

    /**
     * Builds the client using the provided configuration
     *
     * @param configuration      properties
     * @param numberOfPartitions number of partitions in the cluster
     */
    public static void configureHdfsJobConf(JobConf conf, Map<String, String> configuration, int numberOfPartitions) {
        String jsonCredentials = configuration.get(JSON_CREDENTIALS_FIELD_NAME);
        String endpoint = configuration.get(ENDPOINT_FIELD_NAME);

        // disable caching FileSystem
        HDFSUtils.disableHadoopFileSystemCache(conf, HADOOP_GCS_PROTOCOL);

        // TODO(htowaileb): needs further testing, recommended to disable by gcs-hadoop team
        conf.set(GCSConstants.HADOOP_SUPPORT_COMPRESSED, ExternalDataConstants.FALSE);

        // TODO(htowaileb): needs further testing
        // set number of threads
        //        conf.set(GCSConstants.HADOOP_MAX_REQUESTS_PER_BATCH, String.valueOf(numberOfPartitions));
        //        conf.set(GCSConstants.HADOOP_BATCH_THREADS, String.valueOf(numberOfPartitions));

        // authentication method
        // TODO(htowaileb): find a way to pass the content instead of the path to keyfile, this line is temporary
        Path credentials = Path.of("credentials.json");
        if (jsonCredentials == null) {
            // anonymous access
            conf.set(HADOOP_AUTH_TYPE, HADOOP_AUTH_UNAUTHENTICATED);
        } else {
            // TODO(htowaileb) need to pass the file content
            conf.set(HADOOP_AUTH_TYPE, HADOOP_AUTH_SERVICE_ACCOUNT_JSON_KEY_FILE);
            conf.set(HADOOP_AUTH_SERVICE_ACCOUNT_JSON_KEY_FILE_PATH, credentials.toAbsolutePath().toString());
        }

        // set endpoint if provided, default is https://storage.googleapis.com/
        if (endpoint != null) {
            conf.set(HADOOP_ENDPOINT, endpoint);
        }
    }
}
