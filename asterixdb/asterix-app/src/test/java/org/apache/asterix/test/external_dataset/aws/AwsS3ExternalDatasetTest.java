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

import static org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils.createBinaryFiles;
import static org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils.createBinaryFilesRecursively;
import static org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils.setDataPaths;
import static org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils.setUploaders;
import static org.apache.asterix.test.external_dataset.parquet.BinaryFileConverterUtil.DEFAULT_PARQUET_SRC_PATH;
import static org.apache.hyracks.util.file.FileUtil.joinPath;
import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.apache.asterix.test.common.TestConstants;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
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

    private static final String PATH_BASE = joinPath("data");
    private static final String EXTERNAL_FILTER_DATA_PATH = joinPath(PATH_BASE, "json", "external-filter");

    // subclasses of this class MUST instantiate these variables before using them to avoid unexpected behavior
    static String SUITE_TESTS;
    static String ONLY_TESTS;
    static String TEST_CONFIG_FILE_NAME;
    static Runnable PREPARE_BUCKET;
    static Runnable PREPARE_DYNAMIC_PREFIX_AT_START_BUCKET;
    static Runnable PREPARE_FIXED_DATA_BUCKET;
    static Runnable PREPARE_MIXED_DATA_BUCKET;
    static Runnable PREPARE_BOM_FILE_BUCKET;

    static Runnable PREPARE_ICEBERG_TABLE_BUCKET;

    // Base directory paths for data files
    private static final String JSON_DATA_PATH = joinPath("data", "json");
    private static final String CSV_DATA_PATH = joinPath("data", "csv");
    private static final String TSV_DATA_PATH = joinPath("data", "tsv");

    // Service endpoint
    private static final int MOCK_SERVER_PORT = 8001;
    private static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;

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
    public static final String DYNAMIC_PREFIX_AT_START_CONTAINER = "dynamic-prefix-at-start-container";
    public static final String FIXED_DATA_CONTAINER = "fixed-data"; // Do not use, has fixed data
    public static final String INCLUDE_EXCLUDE_CONTAINER = "include-exclude";
    public static final String BOM_FILE_CONTAINER = "bom-file-container";
    public static final String ICEBERG_TABLE_CONTAINER = "iceberg-container";

    public static final PutObjectRequest.Builder playgroundBuilder =
            PutObjectRequest.builder().bucket(PLAYGROUND_CONTAINER);
    public static final PutObjectRequest.Builder dynamicPrefixAtStartBuilder =
            PutObjectRequest.builder().bucket(DYNAMIC_PREFIX_AT_START_CONTAINER);
    public static final PutObjectRequest.Builder fixedDataBuilder =
            PutObjectRequest.builder().bucket(FIXED_DATA_CONTAINER);
    public static final PutObjectRequest.Builder includeExcludeBuilder =
            PutObjectRequest.builder().bucket(INCLUDE_EXCLUDE_CONTAINER);
    public static final PutObjectRequest.Builder bomFileContainerBuilder =
            PutObjectRequest.builder().bucket(BOM_FILE_CONTAINER);

    public static final PutObjectRequest.Builder icebergContainerBuilder =
            PutObjectRequest.builder().bucket(ICEBERG_TABLE_CONTAINER);

    public AwsS3ExternalDatasetTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    // iceberg
    private static final Schema SCHEMA =
            new Schema(required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));
    private static final Configuration CONF = new Configuration();

    private static final String ICEBERG_TABLE_PATH = "s3a://" + ICEBERG_TABLE_CONTAINER + "/my-table/";
    private static final String ICEBERG_TABLE_PATH_FORMAT_VERSION_2 =
            "s3a://" + ICEBERG_TABLE_CONTAINER + "/my-table-format-version-2/";
    private static final String ICEBERG_TABLE_PATH_MIXED_DATA_FORMAT =
            "s3a://" + ICEBERG_TABLE_CONTAINER + "/my-table-mixed-data-format/";

    private static final String ICEBERG_TABLE_PATH_EMPTY = "s3a://" + ICEBERG_TABLE_CONTAINER + "/my-table-empty/";

    private static final String ICEBERG_TABLE_PATH_MULTIPLE_DATA_FILES =
            "s3a://" + ICEBERG_TABLE_CONTAINER + "/my-table-multiple-data-files/";

    private static final String ICEBERG_TABLE_PATH_MODIFIED_DATA =
            "s3a://" + ICEBERG_TABLE_CONTAINER + "/my-table-modified-data/";

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new AwsTestExecutor();
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor);
        createBinaryFiles(DEFAULT_PARQUET_SRC_PATH);
        createBinaryFilesRecursively(EXTERNAL_FILTER_DATA_PATH);
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

    private static DataFile writeFile(String filename, List<Record> records, String location) throws IOException {
        Path path = new Path(location, filename);
        FileFormat fileFormat = FileFormat.fromFileName(filename);
        Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

        FileAppender<Record> fileAppender = new GenericAppenderFactory(AwsS3ExternalDatasetTest.SCHEMA)
                .newAppender(fromPath(path, CONF), fileFormat);
        try (FileAppender<Record> appender = fileAppender) {
            appender.addAll(records);
        }

        return DataFiles.builder(PartitionSpec.unpartitioned()).withInputFile(HadoopInputFile.fromPath(path, CONF))
                .withMetrics(fileAppender.metrics()).build();
    }

    private static void prepareIcebergConfiguration() {
        CONF.set(S3Constants.HADOOP_SERVICE_END_POINT, MOCK_SERVER_HOSTNAME);
        // switch to http
        CONF.set("fs.s3a.connection.ssl.enabled", "false");
        // forces URL style access which is required by the mock. Overwrites DNS based bucket access scheme.
        CONF.set("fs.s3a.path.style.access", "true");
        // Mock server doesn't support concurrency control
        CONF.set("fs.s3a.change.detection.version.required", "false");
        CONF.set(S3Constants.HADOOP_ACCESS_KEY_ID, TestConstants.S3_ACCESS_KEY_ID_DEFAULT);
        CONF.set(S3Constants.HADOOP_SECRET_ACCESS_KEY, TestConstants.S3_SECRET_ACCESS_KEY_DEFAULT);
    }

    public static void prepareIcebergTableContainer() {
        prepareIcebergConfiguration();
        Tables tables = new HadoopTables(CONF);

        // test data
        Record genericRecord = GenericRecord.create(SCHEMA);

        List<Record> fileFirstSnapshotRecords =
                ImmutableList.of(genericRecord.copy(ImmutableMap.of("id", 0, "data", "vibrant_mclean")),
                        genericRecord.copy(ImmutableMap.of("id", 1, "data", "frosty_wilson")),
                        genericRecord.copy(ImmutableMap.of("id", 2, "data", "serene_kirby")));

        List<Record> fileSecondSnapshotRecords =
                ImmutableList.of(genericRecord.copy(ImmutableMap.of("id", 3, "data", "peaceful_pare")),
                        genericRecord.copy(ImmutableMap.of("id", 4, "data", "laughing_mahavira")),
                        genericRecord.copy(ImmutableMap.of("id", 5, "data", "vibrant_lamport")));

        // create the table
        Table table = tables.create(SCHEMA, PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()), ICEBERG_TABLE_PATH);

        // load test data
        try {
            DataFile file = writeFile(FileFormat.PARQUET.addExtension("file"), fileFirstSnapshotRecords,
                    AwsS3ExternalDatasetTest.ICEBERG_TABLE_PATH);
            table.newAppend().appendFile(file).commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // create a table with unsupported iceberg version
        Table unsupportedTable = tables.create(SCHEMA,
                PartitionSpec.unpartitioned(), ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT,
                        FileFormat.PARQUET.name(), TableProperties.FORMAT_VERSION, "2"),
                ICEBERG_TABLE_PATH_FORMAT_VERSION_2);

        // load test data
        try {
            DataFile file = writeFile(FileFormat.PARQUET.addExtension("file"), fileFirstSnapshotRecords,
                    AwsS3ExternalDatasetTest.ICEBERG_TABLE_PATH_FORMAT_VERSION_2);
            unsupportedTable.newAppend().appendFile(file).commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // create a table with mix of parquet and avro data files
        Table mixedDataFormats = tables.create(SCHEMA, PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()),
                ICEBERG_TABLE_PATH_MIXED_DATA_FORMAT);

        // load test data
        try {
            DataFile parquetFile = writeFile(FileFormat.PARQUET.addExtension("parquet-file"), fileFirstSnapshotRecords,
                    AwsS3ExternalDatasetTest.ICEBERG_TABLE_PATH_MIXED_DATA_FORMAT);
            DataFile avroFile = writeFile(FileFormat.AVRO.addExtension("avro-file"), fileSecondSnapshotRecords,
                    AwsS3ExternalDatasetTest.ICEBERG_TABLE_PATH_MIXED_DATA_FORMAT);

            mixedDataFormats.newAppend().appendFile(parquetFile).appendFile(avroFile).commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // empty table
        tables.create(SCHEMA, PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()),
                ICEBERG_TABLE_PATH_EMPTY);

        // multiple data files

        Table multipleDataFiles = tables.create(SCHEMA, PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()),
                ICEBERG_TABLE_PATH_MULTIPLE_DATA_FILES);

        // load test data
        try {
            DataFile file1 = writeFile(FileFormat.PARQUET.addExtension("file-1"), fileFirstSnapshotRecords,
                    AwsS3ExternalDatasetTest.ICEBERG_TABLE_PATH_MULTIPLE_DATA_FILES);
            DataFile file2 = writeFile(FileFormat.PARQUET.addExtension("file-2"), fileSecondSnapshotRecords,
                    AwsS3ExternalDatasetTest.ICEBERG_TABLE_PATH_MULTIPLE_DATA_FILES);

            multipleDataFiles.newAppend().appendFile(file1).appendFile(file2).commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // modify data
        Table modifiedData = tables.create(SCHEMA, PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()),
                ICEBERG_TABLE_PATH_MODIFIED_DATA);

        // load test data
        try {
            DataFile file1 = writeFile(FileFormat.PARQUET.addExtension("file-1"), fileFirstSnapshotRecords,
                    AwsS3ExternalDatasetTest.ICEBERG_TABLE_PATH_MODIFIED_DATA);
            DataFile file2 = writeFile(FileFormat.PARQUET.addExtension("file-2"), fileSecondSnapshotRecords,
                    AwsS3ExternalDatasetTest.ICEBERG_TABLE_PATH_MODIFIED_DATA);

            modifiedData.newAppend().appendFile(file1).appendFile(file2).commit();
            modifiedData.newDelete().deleteFile(file1).commit();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Parameters(name = "AwsS3ExternalDatasetTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        SUITE_TESTS = "testsuite_external_dataset_s3.xml";
        ONLY_TESTS = "only_external_dataset.xml";
        TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
        PREPARE_BUCKET = ExternalDatasetTestUtils::preparePlaygroundContainer;
        PREPARE_DYNAMIC_PREFIX_AT_START_BUCKET = ExternalDatasetTestUtils::prepareDynamicPrefixAtStartContainer;
        PREPARE_FIXED_DATA_BUCKET = ExternalDatasetTestUtils::prepareFixedDataContainer;
        PREPARE_MIXED_DATA_BUCKET = ExternalDatasetTestUtils::prepareMixedDataContainer;
        PREPARE_BOM_FILE_BUCKET = ExternalDatasetTestUtils::prepareBomFileContainer;
        PREPARE_ICEBERG_TABLE_BUCKET = AwsS3ExternalDatasetTest::prepareIcebergTableContainer;

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
        client.createBucket(CreateBucketRequest.builder().bucket(PLAYGROUND_CONTAINER).build());
        client.createBucket(CreateBucketRequest.builder().bucket(DYNAMIC_PREFIX_AT_START_CONTAINER).build());
        client.createBucket(CreateBucketRequest.builder().bucket(FIXED_DATA_CONTAINER).build());
        client.createBucket(CreateBucketRequest.builder().bucket(INCLUDE_EXCLUDE_CONTAINER).build());
        client.createBucket(CreateBucketRequest.builder().bucket(BOM_FILE_CONTAINER).build());
        client.createBucket(CreateBucketRequest.builder().bucket(ICEBERG_TABLE_CONTAINER).build());
        LOGGER.info("Client created successfully");

        // Create the bucket and upload some json files
        setDataPaths(JSON_DATA_PATH, CSV_DATA_PATH, TSV_DATA_PATH);
        setUploaders(AwsS3ExternalDatasetTest::loadPlaygroundData,
                AwsS3ExternalDatasetTest::loadDynamicPrefixAtStartData, AwsS3ExternalDatasetTest::loadFixedData,
                AwsS3ExternalDatasetTest::loadMixedData, AwsS3ExternalDatasetTest::loadBomData);
        PREPARE_BUCKET.run();
        PREPARE_DYNAMIC_PREFIX_AT_START_BUCKET.run();
        PREPARE_FIXED_DATA_BUCKET.run();
        PREPARE_MIXED_DATA_BUCKET.run();
        PREPARE_BOM_FILE_BUCKET.run();
        PREPARE_ICEBERG_TABLE_BUCKET.run();
    }

    private static void loadPlaygroundData(String key, String content, boolean fromFile, boolean gzipped) {
        client.putObject(playgroundBuilder.key(key).build(), getRequestBody(content, fromFile, gzipped));
    }

    private static void loadDynamicPrefixAtStartData(String key, String content, boolean fromFile, boolean gzipped) {
        client.putObject(dynamicPrefixAtStartBuilder.key(key).build(), getRequestBody(content, fromFile, gzipped));
    }

    private static void loadFixedData(String key, String content, boolean fromFile, boolean gzipped) {
        client.putObject(fixedDataBuilder.key(key).build(), getRequestBody(content, fromFile, gzipped));
    }

    private static void loadMixedData(String key, String content, boolean fromFile, boolean gzipped) {
        client.putObject(includeExcludeBuilder.key(key).build(), getRequestBody(content, fromFile, gzipped));
    }

    private static void loadBomData(String key, String content, boolean fromFile, boolean gzipped) {
        client.putObject(bomFileContainerBuilder.key(key).build(), getRequestBody(content, fromFile, gzipped));
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

        @Override
        public void executeTestFile(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx,
                String statement, boolean isDmlRecoveryTest, ProcessBuilder pb, TestCase.CompilationUnit cUnit,
                MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath)
                throws Exception {
            String[] lines;
            switch (ctx.getType()) {
                case "container":
                    // <bucket> <def> <sub-path:new_fname:src_file1,sub-path:new_fname:src_file2,sub-path:src_file3>
                    lines = TestExecutor.stripAllComments(statement).trim().split("\n");
                    String lastLine = lines[lines.length - 1];
                    String[] command = lastLine.trim().split(" ");
                    int length = command.length;
                    if (length == 1) {
                        createBucket(command[0]);
                    } else if (length == 3) {
                        dropRecreateBucket(command[0], command[1], command[2]);
                    } else {
                        throw new Exception("invalid create bucket format");
                    }
                    break;
                default:
                    super.executeTestFile(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit,
                            queryCount, expectedResultFileCtxs, testFile, actualPath);
            }
        }
    }

    private static void createBucket(String bucketName) {
        LOGGER.info("Dropping bucket " + bucketName);
        try {
            client.deleteBucket(DELETE_BUCKET_BUILDER.bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            // ignore
        }
        LOGGER.info("Creating bucket " + bucketName);
        client.createBucket(CREATE_BUCKET_BUILDER.bucket(bucketName).build());
    }

    private static void dropRecreateBucket(String bucketName, String definition, String files) {
        String definitionPath = definition + (definition.endsWith("/") ? "" : "/");
        String[] fileSplits = files.split(",");
        createBucket(bucketName);
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
