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

import java.net.URI;

import org.apache.asterix.cloud.storage.MockCloudStorageConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class CloudUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MOCK_SERVER_PORT = 8001;
    private static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;
    private static final String CLOUD_STORAGE_BUCKET = MockCloudStorageConfiguration.INSTANCE.getContainer();
    private static final String MOCK_SERVER_REGION = "us-west-2";
    private static S3Mock s3MockServer;

    private CloudUtils() {
        throw new AssertionError("Do not instantiate");
    }

    public static void startS3CloudEnvironment() {
        // Starting S3 mock server to be used instead of real S3 server
        LOGGER.info("Starting S3 mock server");
        s3MockServer = new S3Mock.Builder().withPort(MOCK_SERVER_PORT).withInMemoryBackend().build();
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
