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
package org.apache.asterix.external.input.record.reader.aws;

import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.CleanupUtils;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3InputStreamFactory extends AbstractExternalInputStreamFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        return new AwsS3InputStream(configuration, partitionWorkLoadsBasedOnSize.get(partition).getFilePaths());
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration, IWarningCollector warningCollector)
            throws AlgebricksException {
        this.configuration = configuration;
        ICcApplicationContext ccApplicationContext = (ICcApplicationContext) ctx.getApplicationContext();

        // Ensure the validity of include/exclude
        ExternalDataUtils.validateIncludeExclude(configuration);
        IncludeExcludeMatcher includeExcludeMatcher = getIncludeExcludeMatchers();

        // Prepare to retrieve the objects
        List<S3Object> filesOnly;
        String container = configuration.get(AwsS3.CONTAINER_NAME_FIELD_NAME);
        S3Client s3Client = ExternalDataUtils.AwsS3.buildAwsS3Client(configuration);

        try {
            filesOnly = listS3Objects(s3Client, container, includeExcludeMatcher);
        } catch (S3Exception ex) {
            // New API is not implemented, try falling back to old API
            try {
                // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.awsErrorDetails().errorCode().equals("NotImplemented")) {
                    filesOnly = oldApiListS3Objects(s3Client, container, includeExcludeMatcher);
                } else {
                    throw ex;
                }
            } catch (SdkException ex2) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex2.getMessage());
            }
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex.getMessage());
        } finally {
            if (s3Client != null) {
                CleanupUtils.close(s3Client, null);
            }
        }

        // Warn if no files are returned
        if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
            Warning warning = WarningUtil.forAsterix(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
            warningCollector.warn(warning);
        }

        // Partition constraints
        partitionConstraint = ccApplicationContext.getClusterStateManager().getClusterLocations();
        int partitionsCount = partitionConstraint.getLocations().length;

        // Distribute work load amongst the partitions
        distributeWorkLoad(filesOnly, partitionsCount);
    }

    /**
     * Uses the latest API to retrieve the objects from the storage.
     *
     * @param s3Client S3 client
     * @param container container name
     * @param includeExcludeMatcher include/exclude matchers to apply
     */
    private List<S3Object> listS3Objects(S3Client s3Client, String container,
            IncludeExcludeMatcher includeExcludeMatcher) {
        String newMarker = null;
        List<S3Object> filesOnly = new ArrayList<>();

        ListObjectsV2Response listObjectsResponse;
        ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder().bucket(container);
        listObjectsBuilder.prefix(ExternalDataUtils.getPrefix(configuration));

        while (true) {
            // List the objects from the start, or from the last marker in case of truncated result
            if (newMarker == null) {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.build());
            } else {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.continuationToken(newMarker).build());
            }

            // Collect the paths to files only
            collectAndFilterFiles(listObjectsResponse.contents(), includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly);

            // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
            if (!listObjectsResponse.isTruncated()) {
                break;
            } else {
                newMarker = listObjectsResponse.nextContinuationToken();
            }
        }

        return filesOnly;
    }

    /**
     * Uses the old API (in case the new API is not implemented) to retrieve the objects from the storage
     *
     * @param s3Client S3 client
     * @param container container name
     * @param includeExcludeMatcher include/exclude matchers to apply
     */
    private List<S3Object> oldApiListS3Objects(S3Client s3Client, String container,
            IncludeExcludeMatcher includeExcludeMatcher) {
        String newMarker = null;
        List<S3Object> filesOnly = new ArrayList<>();

        ListObjectsResponse listObjectsResponse;
        ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder().bucket(container);
        listObjectsBuilder.prefix(ExternalDataUtils.getPrefix(configuration));

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
    private void collectAndFilterFiles(List<S3Object> s3Objects, BiPredicate<List<Matcher>, String> predicate,
            List<Matcher> matchers, List<S3Object> filesOnly) {
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

    /**
     * To efficiently utilize the parallelism, work load will be distributed amongst the partitions based on the file
     * size.
     *
     * Example:
     * File1 1mb, File2 300kb, File3 300kb, File4 300kb
     *
     * Distribution:
     * Partition1: [File1]
     * Partition2: [File2, File3, File4]
     *
     * @param fileObjects AWS S3 file objects
     * @param partitionsCount Partitions count
     */
    private void distributeWorkLoad(List<S3Object> fileObjects, int partitionsCount) {
        // Prepare the workloads based on the number of partitions
        for (int i = 0; i < partitionsCount; i++) {
            partitionWorkLoadsBasedOnSize.add(new PartitionWorkLoadBasedOnSize());
        }

        for (S3Object object : fileObjects) {
            PartitionWorkLoadBasedOnSize smallest = getSmallestWorkLoad();
            smallest.addFilePath(object.key(), object.size());
        }
    }
}
