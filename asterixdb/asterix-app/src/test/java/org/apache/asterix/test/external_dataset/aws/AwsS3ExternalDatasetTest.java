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
package org.apache.asterix.test.external_dataset.aws;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Runs an AWS S3 mock server and test it as an external dataset
 */
@RunWith(Parameterized.class)
public class AwsS3ExternalDatasetTest {

    private static final Logger LOGGER = LogManager.getLogger();

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";

    // S3 mock server
    private static S3Mock s3MockServer;

    // IMPORTANT: The following values must be used in the AWS S3 test case
    private static S3Client client;
    private static final String S3_MOCK_SERVER_BUCKET = "playground";
    private static final String S3_MOCK_SERVER_BUCKET_DEFINITION = "json-data/reviews/"; // data resides here
    private static final String S3_MOCK_SERVER_REGION = "us-west-2";
    private static final int S3_MOCK_SERVER_PORT = 8001;
    private static final String S3_MOCK_SERVER_HOSTNAME = "http://localhost:" + S3_MOCK_SERVER_PORT;

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new TestExecutor();
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor);
        setNcEndpoints(testExecutor);
        startAwsS3MockServer();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();

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

    @Parameters(name = "SqlppExecutionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_external_dataset.xml", "testsuite_external_dataset.xml");
    }

    protected TestCaseContext tcCtx;

    public AwsS3ExternalDatasetTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }

    private static void setNcEndpoints(TestExecutor testExecutor) {
        final NodeControllerService[] ncs = ExecutionTestUtil.integrationUtil.ncs;
        final Map<String, InetSocketAddress> ncEndPoints = new HashMap<>();
        final String ip = InetAddress.getLoopbackAddress().getHostAddress();
        for (NodeControllerService nc : ncs) {
            final String nodeId = nc.getId();
            final INcApplicationContext appCtx = (INcApplicationContext) nc.getApplicationContext();
            int apiPort = appCtx.getExternalProperties().getNcApiPort();
            ncEndPoints.put(nodeId, InetSocketAddress.createUnresolved(ip, apiPort));
        }
        testExecutor.setNcEndPoints(ncEndPoints);
    }

    /**
     * Starts the AWS s3 mocking server and loads some files for testing
     */
    private static void startAwsS3MockServer() {
        // Starting S3 mock server to be used instead of real S3 server
        LOGGER.info("Starting S3 mock server");
        s3MockServer = new S3Mock.Builder().withPort(S3_MOCK_SERVER_PORT).withInMemoryBackend().build();
        s3MockServer.start();
        LOGGER.info("S3 mock server started successfully");

        // Create a client and add some files to the S3 mock server
        LOGGER.info("Creating S3 client to load initial files to S3 mock server");
        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(S3_MOCK_SERVER_HOSTNAME); // endpoint pointing to S3 mock server
        builder.region(Region.of(S3_MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        client = builder.build();
        LOGGER.info("Client created successfully");

        // Create the bucket and upload some json files
        prepareS3Bucket();
    }

    /**
     * Creates a bucket and fills it with some files for testing purpose.
     */
    private static void prepareS3Bucket() {
        LOGGER.info("creating bucket " + S3_MOCK_SERVER_BUCKET);
        client.createBucket(CreateBucketRequest.builder().bucket(S3_MOCK_SERVER_BUCKET).build());
        LOGGER.info("bucket created successfully");

        LOGGER.info("Adding JSON files to the bucket");
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "0.json").build(),
                RequestBody.fromString("{\"id\": 1, \"year\": null, \"quarter\": null, \"review\": \"good\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "1.json").build(),
                RequestBody.fromString("{\"id\": 2, \"year\": null, \"quarter\": null, \"review\": \"good\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2018/1.json").build(),
                RequestBody.fromString("{\"id\": 3, \"year\": 2018, \"quarter\": null, \"review\": \"good\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2018/2.json").build(),
                RequestBody.fromString("{\"id\": 4, \"year\": 2018, \"quarter\": null, \"review\": \"bad\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2018/q1/1.json").build(),
                RequestBody.fromString("{\"id\": 5, \"year\": 2018, \"quarter\": 1, \"review\": \"good\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2018/q1/2.json").build(),
                RequestBody.fromString("{\"id\": 6, \"year\": 2018, \"quarter\": 1, \"review\": \"bad\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2018/q2/1.json").build(),
                RequestBody.fromString("{\"id\": 7, \"year\": 2018, \"quarter\": 2, \"review\": \"good\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2018/q2/2.json").build(),
                RequestBody.fromString("{\"id\": 8, \"year\": 2018, \"quarter\": 2, \"review\": \"bad\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2019/1.json").build(),
                RequestBody.fromString("{\"id\": 9, \"year\": 2019, \"quarter\": null, \"review\": \"good\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2019/2.json").build(),
                RequestBody.fromString("{\"id\": 10, \"year\": 2019, \"quarter\": null, \"review\": \"bad\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2019/q1/1.json").build(),
                RequestBody.fromString("{\"id\": 11, \"year\": 2019, \"quarter\": 1, \"review\": \"good\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2019/q1/2.json").build(),
                RequestBody.fromString("{\"id\": 12, \"year\": 2019, \"quarter\": 1, \"review\": \"bad\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2019/q2/1.json").build(),
                RequestBody.fromString("{\"id\": 13, \"year\": 2019, \"quarter\": 2, \"review\": \"good\"}"));
        client.putObject(
                PutObjectRequest.builder().bucket(S3_MOCK_SERVER_BUCKET)
                        .key(S3_MOCK_SERVER_BUCKET_DEFINITION + "2019/q2/2.json").build(),
                RequestBody.fromString("{\"id\": 14, \"year\": 2019, \"quarter\": 2, \"review\": \"bad\"}"));
        LOGGER.info("Files added successfully");
    }
}
