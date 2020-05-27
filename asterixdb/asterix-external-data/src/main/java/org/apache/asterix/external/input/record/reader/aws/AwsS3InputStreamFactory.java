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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3InputStreamFactory implements IInputStreamFactory {

    private static final long serialVersionUID = 1L;
    private Map<String, String> configuration;

    // Files to read from
    private final List<S3Object> filesOnly = new ArrayList<>();
    private final List<PartitionWorkLoadBasedOnSize> partitionWorkLoadsBasedOnSize = new ArrayList<>();

    private transient AlgebricksAbsolutePartitionConstraint partitionConstraint;

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.STREAM;
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        return new AwsS3InputStream(configuration, partitionWorkLoadsBasedOnSize.get(partition).getFilePaths());
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return partitionConstraint;
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration) throws AlgebricksException {
        this.configuration = configuration;
        ICcApplicationContext ccApplicationContext = (ICcApplicationContext) ctx.getApplicationContext();

        String container = configuration.get(AwsS3.CONTAINER_NAME_FIELD_NAME);

        S3Client s3Client = ExternalDataUtils.AwsS3.buildAwsS3Client(configuration);

        // Get all objects in a bucket and extract the paths to files
        ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder().bucket(container);
        String path = configuration.get(AwsS3.DEFINITION_FIELD_NAME);
        if (path != null) {
            listObjectsBuilder.prefix(path + (!path.isEmpty() && !path.endsWith("/") ? "/" : ""));
        }

        ListObjectsV2Response listObjectsResponse;
        boolean done = false;
        String newMarker = null;

        String fileFormat = configuration.get(ExternalDataConstants.KEY_FORMAT);

        try {
            while (!done) {
                // List the objects from the start, or from the last marker in case of truncated result
                if (newMarker == null) {
                    listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.build());
                } else {
                    listObjectsResponse =
                            s3Client.listObjectsV2(listObjectsBuilder.continuationToken(newMarker).build());
                }

                // Collect the paths to files only
                collectFilesOnly(listObjectsResponse.contents(), fileFormat);

                // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
                if (!listObjectsResponse.isTruncated()) {
                    done = true;
                } else {
                    newMarker = listObjectsResponse.nextContinuationToken();
                }
            }
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex.getMessage());
        } finally {
            if (s3Client != null) {
                CleanupUtils.close(s3Client, null);
            }
        }

        // Partition constraints
        partitionConstraint = ccApplicationContext.getClusterStateManager().getClusterLocations();
        int partitionsCount = partitionConstraint.getLocations().length;

        // Distribute work load amongst the partitions
        distributeWorkLoad(filesOnly, partitionsCount);
    }

    /**
     * AWS S3 returns all the objects as paths, not differentiating between folder and files. The path is considered
     * a file if it does not end up with a "/" which is the separator in a folder structure.
     *
     * @param s3Objects List of returned objects
     * @param fileFormat The expected file format
     *
     * @throws AsterixException AsterixException
     */
    private void collectFilesOnly(List<S3Object> s3Objects, String fileFormat) throws AsterixException {
        String fileExtension = getFileExtension(fileFormat);
        if (fileExtension == null) {
            throw AsterixException.create(ErrorCode.PROVIDER_STREAM_RECORD_READER_UNKNOWN_FORMAT, fileFormat);
        }

        s3Objects.stream().filter(object -> isValidFile(object.key(), fileFormat)).forEach(filesOnly::add);
    }

    /**
     * Checks if the file name is of the provided format, or in the provided format in a compressed (.gz or .gzip) state
     *
     * @param fileName file name to be checked
     * @param format expected format
     * @return {@code true} if the file name is of the expected format, {@code false} otherwise
     */
    private boolean isValidFile(String fileName, String format) {
        String lowCaseName = fileName.toLowerCase();
        String lowCaseFormat = format.toLowerCase();
        String gzExt = lowCaseFormat + ".gz";
        String gzipExt = lowCaseFormat + ".gzip";

        return lowCaseName.endsWith(lowCaseFormat) || lowCaseName.endsWith(gzExt) || lowCaseName.endsWith(gzipExt);
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

    /**
     * Finds the smallest workload and returns it
     *
     * @return the smallest workload
     */
    private PartitionWorkLoadBasedOnSize getSmallestWorkLoad() {
        PartitionWorkLoadBasedOnSize smallest = partitionWorkLoadsBasedOnSize.get(0);
        for (PartitionWorkLoadBasedOnSize partition : partitionWorkLoadsBasedOnSize) {
            // If the current total size is 0, add the file directly as this is a first time partition
            if (partition.getTotalSize() == 0) {
                smallest = partition;
                break;
            }
            if (partition.getTotalSize() < smallest.getTotalSize()) {
                smallest = partition;
            }
        }

        return smallest;
    }

    /**
     * Returns the file extension for the provided file format.
     *
     * @param format file format
     *
     * @return file extension for the provided file format, null otherwise.
     */
    private String getFileExtension(String format) {
        switch (format.toLowerCase()) {
            case ExternalDataConstants.FORMAT_JSON_LOWER_CASE:
                return ".json";
            case ExternalDataConstants.FORMAT_CSV:
                return ".csv";
            case ExternalDataConstants.FORMAT_TSV:
                return ".tsv";
            default:
                return null;
        }
    }

    private static class PartitionWorkLoadBasedOnSize implements Serializable {
        private static final long serialVersionUID = 1L;
        private List<String> filePaths = new ArrayList<>();
        private long totalSize = 0;

        PartitionWorkLoadBasedOnSize() {
        }

        public List<String> getFilePaths() {
            return filePaths;
        }

        public void addFilePath(String filePath, long size) {
            this.filePaths.add(filePath);
            this.totalSize += size;
        }

        public long getTotalSize() {
            return totalSize;
        }

        @Override
        public String toString() {
            return "Files: " + filePaths.size() + ", Total Size: " + totalSize;
        }
    }
}
