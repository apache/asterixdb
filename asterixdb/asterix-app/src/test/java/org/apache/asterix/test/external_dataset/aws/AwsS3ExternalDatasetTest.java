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

import static org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils.setDataPaths;
import static org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils.setUploaders;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Runs an AWS S3 mock server and test it as an external dataset
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AwsS3ExternalDatasetTest {

    private static final Logger LOGGER = LogManager.getLogger();

    // subclasses of this class MUST instantiate these variables before using them to avoid unexpected behavior
    static String SUITE_TESTS;
    static String ONLY_TESTS;
    static String TEST_CONFIG_FILE_NAME;
    static Runnable PREPARE_BUCKET;
    static Runnable PREPARE_FIXED_DATA_BUCKET;
    static Runnable PREPARE_MIXED_DATA_BUCKET;

    // Base directory paths for data files
    private static final String JSON_DATA_PATH = joinPath("data", "json");
    private static final String CSV_DATA_PATH = joinPath("data", "csv");
    private static final String TSV_DATA_PATH = joinPath("data", "tsv");

    // Service endpoint
    private static final int MOCK_SERVER_PORT = 8001;
    private static final String MOCK_SERVER_HOSTNAME = "http://localhost:" + MOCK_SERVER_PORT;

    // Region, bucket and definitions
    private static final String MOCK_SERVER_REGION = "us-west-2";

    private static final Set<String> fileNames = new HashSet<>();
    private static final CreateBucketRequest.Builder CREATE_BUCKET_BUILDER = CreateBucketRequest.builder();
    private static final DeleteBucketRequest.Builder DELETE_BUCKET_BUILDER = DeleteBucketRequest.builder();
    private static final PutObjectRequest.Builder PUT_OBJECT_BUILDER = PutObjectRequest.builder();

    private static S3Mock s3MockServer;
    private static S3Client client;

    protected TestCaseContext tcCtx;

    public static final String PLAYGROUND_CONTAINER = "playground";
    public static final String FIXED_DATA_CONTAINER = "fixed-data"; // Do not use, has fixed data
    public static final String INCLUDE_EXCLUDE_CONTAINER = "include-exclude";
    public static final PutObjectRequest.Builder playgroundBuilder =
            PutObjectRequest.builder().bucket(PLAYGROUND_CONTAINER);
    public static final PutObjectRequest.Builder fixedDataBuilder =
            PutObjectRequest.builder().bucket(FIXED_DATA_CONTAINER);
    public static final PutObjectRequest.Builder includeExcludeBuilder =
            PutObjectRequest.builder().bucket(INCLUDE_EXCLUDE_CONTAINER);

    public AwsS3ExternalDatasetTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new AwsTestExecutor();
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

    @Parameters(name = "AwsS3ExternalDatasetTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        SUITE_TESTS = "testsuite_external_dataset_s3.xml";
        ONLY_TESTS = "only_external_dataset.xml";
        TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
        PREPARE_BUCKET = ExternalDatasetTestUtils::preparePlaygroundContainer;
        PREPARE_FIXED_DATA_BUCKET = ExternalDatasetTestUtils::prepareFixedDataContainer;
        PREPARE_MIXED_DATA_BUCKET = ExternalDatasetTestUtils::prepareMixedDataContainer;
        return LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
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
        s3MockServer = new S3Mock.Builder().withPort(MOCK_SERVER_PORT).withInMemoryBackend().build();
        s3MockServer.start();
        LOGGER.info("S3 mock server started successfully");

        // Create a client and add some files to the S3 mock server
        LOGGER.info("Creating S3 client to load initial files to S3 mock server");
        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_HOSTNAME); // endpoint pointing to S3 mock server
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        client = builder.build();
        client.createBucket(CreateBucketRequest.builder().bucket(PLAYGROUND_CONTAINER).build());
        client.createBucket(CreateBucketRequest.builder().bucket(FIXED_DATA_CONTAINER).build());
        client.createBucket(CreateBucketRequest.builder().bucket(INCLUDE_EXCLUDE_CONTAINER).build());
        LOGGER.info("Client created successfully");

        // Create the bucket and upload some json files
        setDataPaths(JSON_DATA_PATH, CSV_DATA_PATH, TSV_DATA_PATH);
        setUploaders(AwsS3ExternalDatasetTest::loadPlaygroundData, AwsS3ExternalDatasetTest::loadFixedData,
                AwsS3ExternalDatasetTest::loadMixedData);
        PREPARE_BUCKET.run();
        PREPARE_FIXED_DATA_BUCKET.run();
        PREPARE_MIXED_DATA_BUCKET.run();
    }

    private static void loadPlaygroundData(String key, String content, boolean fromFile, boolean gzipped) {
        client.putObject(playgroundBuilder.key(key).build(), getRequestBody(content, fromFile, gzipped));
    }

    private static void loadFixedData(String key, String content, boolean fromFile, boolean gzipped) {
        client.putObject(fixedDataBuilder.key(key).build(), getRequestBody(content, fromFile, gzipped));
    }

    private static void loadMixedData(String key, String content, boolean fromFile, boolean gzipped) {
        client.putObject(includeExcludeBuilder.key(key).build(), getRequestBody(content, fromFile, gzipped));
    }

    private static RequestBody getRequestBody(String content, boolean fromFile, boolean gzipped) {
        RequestBody body;

        // Content is string
        if (!fromFile) {
            body = RequestBody.fromString(content);
        } else {
            // Content is a file path
            if (!gzipped) {
                body = RequestBody.fromFile(Paths.get(content));
            } else {
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                    gzipOutputStream.write(Files.readAllBytes(Paths.get(content)));
                    gzipOutputStream.close(); // Need to close or data will be invalid
                    byte[] gzipBytes = byteArrayOutputStream.toByteArray();
                    body = RequestBody.fromBytes(gzipBytes);
                } catch (IOException ex) {
                    throw new IllegalArgumentException(ex.toString());
                }
            }
        }

        return body;
    }

    static class AwsTestExecutor extends TestExecutor {

        public void executeTestFile(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx,
                String statement, boolean isDmlRecoveryTest, ProcessBuilder pb, TestCase.CompilationUnit cUnit,
                MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath,
                BitSet expectedWarnings) throws Exception {
            String[] lines;
            switch (ctx.getType()) {
                case "container":
                    // <bucket> <def> <sub-path:new_fname:src_file1,sub-path:new_fname:src_file2,sub-path:src_file3>
                    lines = TestExecutor.stripAllComments(statement).trim().split("\n");
                    String lastLine = lines[lines.length - 1];
                    String[] command = lastLine.trim().split(" ");
                    int length = command.length;
                    if (length != 3) {
                        throw new Exception("invalid create bucket format");
                    }
                    dropRecreateBucket(command[0], command[1], command[2]);
                    break;
                default:
                    super.executeTestFile(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit,
                            queryCount, expectedResultFileCtxs, testFile, actualPath, expectedWarnings);
            }
        }
    }

    private static void dropRecreateBucket(String bucketName, String definition, String files) {
        String definitionPath = definition + (definition.endsWith("/") ? "" : "/");
        String[] fileSplits = files.split(",");

        LOGGER.info("Dropping bucket " + bucketName);
        try {
            client.deleteBucket(DELETE_BUCKET_BUILDER.bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            // ignore
        }
        LOGGER.info("Creating bucket " + bucketName);
        client.createBucket(CREATE_BUCKET_BUILDER.bucket(bucketName).build());
        LOGGER.info("Uploading to bucket " + bucketName + " definition " + definitionPath);
        fileNames.clear();
        for (int i = 0; i < fileSplits.length; i++) {
            String[] s3pathAndSourceFile = fileSplits[i].split(":");
            int size = s3pathAndSourceFile.length;
            String path;
            String sourceFilePath;
            String uploadedFileName;
            if (size == 1) {
                // case: playground json-data/reviews SOURCE_FILE1,SOURCE_FILE2
                path = definitionPath;
                sourceFilePath = s3pathAndSourceFile[0];
                uploadedFileName = FilenameUtils.getName(s3pathAndSourceFile[0]);
            } else if (size == 2) {
                // case: playground json-data/reviews level1/sub-level:SOURCE_FILE1,level2/sub-level:SOURCE_FILE2
                String subPathOrNewFileName = s3pathAndSourceFile[0];
                if (subPathOrNewFileName.startsWith("$$")) {
                    path = definitionPath;
                    sourceFilePath = s3pathAndSourceFile[1];
                    uploadedFileName = subPathOrNewFileName.substring(2);
                } else {
                    path = definitionPath + subPathOrNewFileName + (subPathOrNewFileName.endsWith("/") ? "" : "/");
                    sourceFilePath = s3pathAndSourceFile[1];
                    uploadedFileName = FilenameUtils.getName(s3pathAndSourceFile[1]);
                }
            } else if (size == 3) {
                path = definitionPath + s3pathAndSourceFile[0] + (s3pathAndSourceFile[0].endsWith("/") ? "" : "/");
                uploadedFileName = s3pathAndSourceFile[1];
                sourceFilePath = s3pathAndSourceFile[2];

            } else {
                throw new IllegalArgumentException();
            }

            String keyPath = path + uploadedFileName;
            int k = 1;
            while (fileNames.contains(keyPath)) {
                keyPath = path + (k++) + uploadedFileName;
            }
            fileNames.add(keyPath);
            client.putObject(PUT_OBJECT_BUILDER.bucket(bucketName).key(keyPath).build(),
                    RequestBody.fromFile(Paths.get(sourceFilePath)));
        }
        LOGGER.info("Done creating bucket with data");
    }
}