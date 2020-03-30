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

import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3Constants;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3InputStreamFactory implements IInputStreamFactory {

    private static final long serialVersionUID = 1L;
    private Map<String, String> configuration;

    // Files to read from
    private List<PartitionWorkLoadBasedOnSize> partitionWorkLoadsBasedOnSize = new ArrayList<>();

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
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition) {
        return new AwsS3InputStream(configuration, partitionWorkLoadsBasedOnSize.get(partition).getFilePaths());
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return partitionConstraint;
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration)
            throws AlgebricksException, HyracksDataException {
        this.configuration = configuration;
        ICcApplicationContext ccApplicationContext = (ICcApplicationContext) ctx.getApplicationContext();

        String container = configuration.get(AwsS3Constants.CONTAINER_NAME_FIELD_NAME);

        S3Client s3Client = buildAwsS3Client(configuration);

        // Get all objects in a bucket and extract the paths to files
        ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder().bucket(container);
        String path = configuration.get(AwsS3Constants.DEFINITION_FIELD_NAME);
        if (path != null) {
            listObjectsBuilder.prefix(path + (path.endsWith("/") ? "" : "/"));
        }
        ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsBuilder.build());
        List<S3Object> s3Objects = listObjectsResponse.contents();

        // Exclude the directories and get the files only
        String fileFormat = configuration.get(ExternalDataConstants.KEY_FORMAT);
        List<S3Object> fileObjects = getFilesOnly(s3Objects, fileFormat);

        // Partition constraints
        partitionConstraint = ccApplicationContext.getClusterStateManager().getClusterLocations();
        int partitionsCount = partitionConstraint.getLocations().length;

        // Distribute work load amongst the partitions
        distributeWorkLoad(fileObjects, partitionsCount);
    }

    /**
     * AWS S3 returns all the objects as paths, not differentiating between folder and files. The path is considered
     * a file if it does not end up with a "/" which is the separator in a folder structure.
     *
     * @param s3Objects List of returned objects
     *
     * @return A list of string paths that point to files only
     *
     * @throws HyracksDataException HyracksDataException
     */
    private List<S3Object> getFilesOnly(List<S3Object> s3Objects, String fileFormat) throws HyracksDataException {
        List<S3Object> filesOnly = new ArrayList<>();
        String fileExtension = getFileExtension(fileFormat);
        if (fileExtension == null) {
            throw HyracksDataException.create(ErrorCode.INVALID_FORMAT);
        }

        s3Objects.stream().filter(object -> object.key().endsWith(fileExtension)).forEach(filesOnly::add);

        return filesOnly;
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
     * Prepares and builds the Amazon S3 client with the provided configuration
     *
     * @param configuration S3 client configuration
     *
     * @return Amazon S3 client
     */
    private static S3Client buildAwsS3Client(Map<String, String> configuration) {
        S3ClientBuilder builder = S3Client.builder();

        // Credentials
        String accessKey = configuration.get(AwsS3Constants.ACCESS_KEY_FIELD_NAME);
        String secretKey = configuration.get(AwsS3Constants.SECRET_KEY_FIELD_NAME);
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        builder.credentialsProvider(StaticCredentialsProvider.create(credentials));

        // Region
        String region = configuration.get(AwsS3Constants.REGION_FIELD_NAME);
        builder.region(Region.of(region));

        // Use user's endpoint if provided
        if (configuration.get(AwsS3Constants.SERVICE_END_POINT_FIELD_NAME) != null) {
            String endPoint = configuration.get(AwsS3Constants.SERVICE_END_POINT_FIELD_NAME);
            builder.endpointOverride(URI.create(endPoint));
        }

        return builder.build();
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
            case "json":
                return ".json";
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
