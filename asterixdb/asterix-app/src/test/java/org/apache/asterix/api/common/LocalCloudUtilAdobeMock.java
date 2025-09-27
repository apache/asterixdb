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

import static org.apache.asterix.api.common.LocalCloudUtil.MOCK_SERVER_ENDPOINT;
import static org.apache.asterix.api.common.LocalCloudUtil.MOCK_SERVER_REGION;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.adobe.testing.s3mock.S3MockApplication;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

// With adobe mock, the s3 objects will be found in /tmp or /var folder
// Search for line "Successfully created {} as root folder" in the info log file
// or else just call the aws cli on localhost:8001
// eg: aws s3 ls --endpoint http://localhost:8001
public class LocalCloudUtilAdobeMock {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MOCK_SERVER_PORT = 8001;
    private static final int MOCK_SERVER_PORT_HTTPS = 8002;
    public static final String CLOUD_STORAGE_BUCKET = "cloud-storage-container";
    public static final String PLAYGROUND_BUCKET = "playground";
    private static S3MockApplication s3Mock;

    private LocalCloudUtilAdobeMock() {
        throw new AssertionError("Do not instantiate");
    }

    public static void main(String[] args) {
        String cleanStartString = System.getProperty("cleanup.start", "true");
        boolean cleanStart = Boolean.parseBoolean(cleanStartString);
        // Change to 'true' if you want to delete "s3mock" folder on start
        startS3CloudEnvironment(cleanStart);
    }

    public static S3MockApplication startS3CloudEnvironment(boolean cleanStart) {
        return startS3CloudEnvironment(cleanStart, false);
    }

    public static S3MockApplication startS3CloudEnvironment(boolean cleanStart, boolean createPlaygroundContainer) {
        // Starting S3 mock server to be used instead of real S3 server
        LOGGER.info("Starting S3 mock server");

        Map<String, Object> properties = new HashMap<>();
        properties.put(S3MockApplication.PROP_HTTP_PORT, MOCK_SERVER_PORT);
        properties.put(S3MockApplication.PROP_HTTPS_PORT, MOCK_SERVER_PORT_HTTPS);
        properties.put(S3MockApplication.PROP_SILENT, false);
        LocalCloudUtil.stopS3MockServer();
        s3Mock = S3MockApplication.start(properties);

        LOGGER.info("S3 mock server started successfully");

        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_ENDPOINT); // endpoint pointing to S3 mock server
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        S3Client client = builder.build();
        client.createBucket(CreateBucketRequest.builder().bucket(CLOUD_STORAGE_BUCKET).build());
        LOGGER.info("Created bucket {} for cloud storage", CLOUD_STORAGE_BUCKET);

        if (createPlaygroundContainer) {
            client.createBucket(CreateBucketRequest.builder().bucket(PLAYGROUND_BUCKET).build());
            LOGGER.info("Created bucket {}", PLAYGROUND_BUCKET);
        }
        client.close();
        return s3Mock;
    }

    public static void shutdownSilently() {
        if (s3Mock != null) {
            try {
                LOGGER.info("test cleanup, stopping S3 mock server");
                s3Mock.stop();
                LOGGER.info("test cleanup, stopped S3 mock server");
            } catch (Exception ex) {
                // do nothing
            }
            s3Mock = null;
        }
    }

    public static S3Client getS3Client() {
        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_ENDPOINT);
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        S3Client client = builder.build();
        return client;
    }
}
