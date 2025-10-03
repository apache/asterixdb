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
package org.apache.asterix.api.common;

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.File;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class LocalCloudUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MOCK_SERVER_PORT = 8001;
    public static final String MOCK_SERVER_ENDPOINT = "http://127.0.0.1:" + MOCK_SERVER_PORT;
    public static final String CLOUD_STORAGE_BUCKET = "cloud-storage-container";
    public static final String STORAGE_DUMMY_FILE = "storage/dummy.txt";
    public static final String MOCK_SERVER_REGION = "us-west-2";
    private static final String MOCK_FILE_BACKEND = joinPath("target", "s3mock");
    private static S3Mock s3MockServer;

    private LocalCloudUtil() {
        throw new AssertionError("Do not instantiate");
    }

    public static void main(String[] args) {
        String cleanStartString = System.getProperty("cleanup.start", "true");
        boolean cleanStart = Boolean.parseBoolean(cleanStartString);
        // Change to 'true' if you want to delete "s3mock" folder on start
        startS3CloudEnvironment(cleanStart);
    }

    public static S3Mock startS3CloudEnvironment(boolean cleanStart) {
        return startS3CloudEnvironment(cleanStart, false);
    }

    public static S3Mock startS3CloudEnvironment(boolean cleanStart, boolean createPlaygroundContainer) {
        if (cleanStart) {
            FileUtils.deleteQuietly(new File(MOCK_FILE_BACKEND));
        }
        stopS3MockServer();
        // Starting S3 mock server to be used instead of real S3 server
        LOGGER.info("Starting S3 mock server");
        // Use file backend for debugging/inspection
        s3MockServer = new S3Mock.Builder().withPort(MOCK_SERVER_PORT).withFileBackend(MOCK_FILE_BACKEND).build();
        s3MockServer.start();
        LOGGER.info("S3 mock server started successfully");

        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_ENDPOINT); // endpoint pointing to S3 mock server
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        S3Client client = builder.build();
        client.createBucket(CreateBucketRequest.builder().bucket(CLOUD_STORAGE_BUCKET).build());
        LOGGER.info("Created bucket {} for cloud storage", CLOUD_STORAGE_BUCKET);

        // create a storage container and delete stuff inside it, just to create a directory.
        PutObjectRequest putObjectRequest =
                PutObjectRequest.builder().bucket(CLOUD_STORAGE_BUCKET).key(STORAGE_DUMMY_FILE).build();

        client.putObject(putObjectRequest, RequestBody.empty());
        // delete dummy file to retain storage directory.
        client.deleteObject(DeleteObjectRequest.builder().bucket(CLOUD_STORAGE_BUCKET).key(STORAGE_DUMMY_FILE).build());

        // added for convenience since some non-external-based tests include an external collection test on this bucket
        if (createPlaygroundContainer) {
            client.createBucket(CreateBucketRequest.builder().bucket("playground").build());
            LOGGER.info("Created bucket {}", "playground");
        }
        client.close();
        return s3MockServer;
    }

    public static void recreateBucket(String bucketName, S3Client client) {
        String verb = "Created";
        try {
            client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
            verb = "Recreated";
        } catch (Exception e) {
            // ignore any failure
        }
        try {
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            LOGGER.info("{} bucket {}", verb, bucketName);
        } finally {
            client.close();
        }
    }

    public static void stopS3MockServer() {
        shutdownSilently();
        // since they are running on same port, we need to shut down other mock server as well
        LocalCloudUtilAdobeMock.shutdownSilently();
    }

    private static void shutdownSilently() {
        if (s3MockServer != null) {
            try {
                s3MockServer.shutdown();
            } catch (Exception ex) {
                // do nothing
            }
            s3MockServer = null;
        }
    }
}
