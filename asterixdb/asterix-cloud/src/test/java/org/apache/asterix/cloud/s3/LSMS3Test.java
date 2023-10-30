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
package org.apache.asterix.cloud.s3;

import java.net.URI;

import org.apache.asterix.cloud.LSMTest;
import org.apache.asterix.cloud.clients.aws.s3.S3ClientConfig;
import org.apache.asterix.cloud.clients.aws.s3.S3CloudClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;

public class LSMS3Test extends LSMTest {

    private static S3Client client;
    private static S3Mock s3MockServer;
    private static final int MOCK_SERVER_PORT = 8001;
    private static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;
    private static final String MOCK_SERVER_REGION = "us-west-2"; // does not matter the value

    @BeforeClass
    public static void setup() throws Exception {
        LOGGER.info("LSMS3Test setup");
        LOGGER.info("Starting S3 mock server");
        s3MockServer = new S3Mock.Builder().withPort(MOCK_SERVER_PORT).withInMemoryBackend().build();
        try {
            s3MockServer.start();
        } catch (Exception ex) {
            // it might already be started, do nothing
        }
        LOGGER.info("S3 mock server started successfully");

        // Create a client and add some files to the S3 mock server
        LOGGER.info("Creating S3 client to load initial files to S3 mock server");
        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_HOSTNAME); // endpoint pointing to S3 mock server
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        client = builder.build();
        cleanup();
        client.createBucket(CreateBucketRequest.builder().bucket(PLAYGROUND_CONTAINER).build());
        LOGGER.info("Client created successfully");
        S3ClientConfig config = new S3ClientConfig(MOCK_SERVER_REGION, MOCK_SERVER_HOSTNAME, "", true, 0);
        CLOUD_CLIENT = new S3CloudClient(config);
    }

    private static void cleanup() {
        try {
            client.deleteBucket(DeleteBucketRequest.builder().bucket(PLAYGROUND_CONTAINER).build());
        } catch (Exception ex) {
            // ignore
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // Shutting down S3 mock server
        LOGGER.info("Shutting down S3 mock server and client");
        if (client != null) {
            client.close();
        }
        if (s3MockServer != null) {
            s3MockServer.shutdown();
        }
        LOGGER.info("S3 mock down and client shut down successfully");
    }
}
