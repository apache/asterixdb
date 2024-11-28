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
package org.apache.asterix.external.util.aws.s3;

import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ERROR_METHOD_NOT_IMPLEMENTED;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.CleanupUtils;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3Utils {
    private S3Utils() {
        throw new AssertionError("do not instantiate");
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
    protected static S3Response isBucketEmpty(S3Client s3Client, String container, String prefix, boolean useOldApi) {
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
    public static List<S3Object> listS3Objects(IApplicationContext appCtx, Map<String, String> configuration,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator) throws CompilationException, HyracksDataException {
        // Prepare to retrieve the objects
        List<S3Object> filesOnly;
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        S3Client s3Client = S3AuthUtils.buildAwsS3Client(appCtx, configuration);
        String prefix = getPrefix(configuration);

        try {
            filesOnly = listS3Objects(s3Client, container, prefix, includeExcludeMatcher, externalDataPrefix, evaluator,
                    warningCollector);
        } catch (S3Exception ex) {
            // New API is not implemented, try falling back to old API
            try {
                // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.awsErrorDetails().errorCode().equals(ERROR_METHOD_NOT_IMPLEMENTED)) {
                    filesOnly = oldApiListS3Objects(s3Client, container, prefix, includeExcludeMatcher,
                            externalDataPrefix, evaluator, warningCollector);
                } else {
                    throw ex;
                }
            } catch (SdkException ex2) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex2, getMessageOrToString(ex));
            }
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
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
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator,
            IWarningCollector warningCollector) throws HyracksDataException {
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
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.continuationToken(newMarker).build());
            }

            // Collect the paths to files only
            collectAndFilterFiles(listObjectsResponse.contents(), includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly, externalDataPrefix, evaluator,
                    warningCollector);

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
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator,
            IWarningCollector warningCollector) throws HyracksDataException {
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
                    includeExcludeMatcher.getMatchersList(), filesOnly, externalDataPrefix, evaluator,
                    warningCollector);

            // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
            if (listObjectsResponse.isTruncated() != null && listObjectsResponse.isTruncated()) {
                newMarker = listObjectsResponse.nextMarker();
            } else {
                break;
            }
        }

        return filesOnly;
    }

    /**
     * Collects only files that pass all tests
     *
     * @param s3Objects          s3 objects
     * @param predicate          predicate
     * @param matchers           matchers
     * @param filesOnly          filtered files
     * @param externalDataPrefix external data prefix
     * @param evaluator          evaluator
     */
    private static void collectAndFilterFiles(List<S3Object> s3Objects, BiPredicate<List<Matcher>, String> predicate,
            List<Matcher> matchers, List<S3Object> filesOnly, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator, IWarningCollector warningCollector) throws HyracksDataException {
        for (S3Object object : s3Objects) {
            if (ExternalDataUtils.evaluate(object.key(), predicate, matchers, externalDataPrefix, evaluator,
                    warningCollector)) {
                filesOnly.add(object);
            }
        }
    }

    public static Map<String, List<String>> S3ObjectsOfSingleDepth(IApplicationContext appCtx,
            Map<String, String> configuration, String container, String prefix)
            throws CompilationException, HyracksDataException {
        // create s3 client
        S3Client s3Client = S3AuthUtils.buildAwsS3Client(appCtx, configuration);
        // fetch all the s3 objects
        return listS3ObjectsOfSingleDepth(s3Client, container, prefix);
    }

    /**
     * Uses the latest API to retrieve the objects from the storage of a single level.
     *
     * @param s3Client              S3 client
     * @param container             container name
     * @param prefix                definition prefix
     */
    private static Map<String, List<String>> listS3ObjectsOfSingleDepth(S3Client s3Client, String container,
            String prefix) {
        Map<String, List<String>> allObjects = new HashMap<>();
        ListObjectsV2Iterable listObjectsInterable;
        ListObjectsV2Request.Builder listObjectsBuilder =
                ListObjectsV2Request.builder().bucket(container).prefix(prefix).delimiter("/");
        listObjectsBuilder.prefix(prefix);
        List<String> files = new ArrayList<>();
        List<String> folders = new ArrayList<>();
        // to skip the prefix as a file from the response
        boolean checkPrefixInFile = true;
        listObjectsInterable = s3Client.listObjectsV2Paginator(listObjectsBuilder.build());
        for (ListObjectsV2Response response : listObjectsInterable) {
            // put all the files
            for (S3Object object : response.contents()) {
                String fileName = object.key();
                fileName = fileName.substring(prefix.length(), fileName.length());
                if (checkPrefixInFile) {
                    if (prefix.equals(object.key()))
                        checkPrefixInFile = false;
                    else {
                        files.add(fileName);
                    }
                } else {
                    files.add(fileName);
                }
            }
            // put all the folders
            for (CommonPrefix object : response.commonPrefixes()) {
                String folderName = object.prefix();
                folderName = folderName.substring(prefix.length(), folderName.length());
                folders.add(folderName.endsWith("/") ? folderName.substring(0, folderName.length() - 1) : folderName);
            }
        }
        allObjects.put("files", files);
        allObjects.put("folders", folders);
        return allObjects;
    }

    public static String getPath(Map<String, String> configuration) {
        return S3Constants.HADOOP_S3_PROTOCOL + "://"
                + configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME) + '/'
                + configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);
    }
}
