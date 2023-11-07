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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class LocalCloudUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MOCK_SERVER_PORT = 8001;
    public static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;
    public static final String CLOUD_STORAGE_BUCKET = "cloud-storage-container";
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
        if (cleanStart) {
            FileUtils.deleteQuietly(new File(MOCK_FILE_BACKEND));
        }
        // Starting S3 mock server to be used instead of real S3 server
        LOGGER.info("Starting S3 mock server");
        // Use file backend for debugging/inspection
        s3MockServer = new S3Mock.Builder().withPort(MOCK_SERVER_PORT).withFileBackend(MOCK_FILE_BACKEND).build();
        shutdownSilently();
        try {
            s3MockServer.start();
        } catch (Exception ex) {
            // it might already be started, do nothing
        }
        LOGGER.info("S3 mock server started successfully");

        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_HOSTNAME); // endpoint pointing to S3 mock server
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        S3Client client = builder.build();
        client.createBucket(CreateBucketRequest.builder().bucket(CLOUD_STORAGE_BUCKET).build());
        LOGGER.info("Created bucket {} for cloud storage", CLOUD_STORAGE_BUCKET);
        client.close();
        return s3MockServer;
    }

    private static void shutdownSilently() {
        if (s3MockServer != null) {
            try {
                s3MockServer.shutdown();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }
}
