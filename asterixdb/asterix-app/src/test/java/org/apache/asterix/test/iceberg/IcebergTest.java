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
package org.apache.asterix.test.iceberg;

import static org.apache.hyracks.util.file.FileUtil.joinPath;
import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.asterix.api.common.LocalCloudUtilAdobeMock;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.apache.asterix.test.common.TestConstants;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Runs an AWS S3 mock server and test for iceberg catalogs and tables
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IcebergTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String PATH_BASE = joinPath("data");
    private static final String EXTERNAL_FILTER_DATA_PATH = joinPath(PATH_BASE, "json", "external-filter");

    private static final String SUITE_TESTS = "testsuite_iceberg.xml";
    private static final String ONLY_TESTS = "testsuite_iceberg_only.xml";
    private static final String CONFIG_FILE_NAME = "src/test/resources/cc-cloud-storage.conf";
    static Runnable PREPARE_ICEBERG_TABLE_BUCKET;

    // Client and service endpoint
    private static S3Client client;
    private static final int MOCK_SERVER_PORT = 8001;
    private static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;
    private static final String MOCK_SERVER_REGION = "us-west-2";

    protected TestCaseContext tcCtx;

    public static final String ICEBERG_CONTAINER = "iceberg-improved-container";
    public static final PutObjectRequest.Builder icebergContainerBuilder =
            PutObjectRequest.builder().bucket(ICEBERG_CONTAINER);

    public IcebergTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    // iceberg
    private static final Schema SCHEMA =
            new Schema(required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));
    private static final Configuration CONF = new Configuration();

    private static final String ICEBERG_TABLE_PATH = "s3a://" + ICEBERG_CONTAINER + "/my-table/";
    private static final String ICEBERG_TABLE_PATH_FORMAT_VERSION_2 =
            "s3a://" + ICEBERG_CONTAINER + "/my-table-format-version-2/";
    private static final String ICEBERG_TABLE_PATH_MIXED_DATA_FORMAT =
            "s3a://" + ICEBERG_CONTAINER + "/my-table-mixed-data-format/";

    private static final String ICEBERG_TABLE_PATH_EMPTY = "s3a://" + ICEBERG_CONTAINER + "/my-table-empty/";

    private static final String ICEBERG_TABLE_PATH_MULTIPLE_DATA_FILES =
            "s3a://" + ICEBERG_CONTAINER + "/my-table-multiple-data-files/";

    private static final String ICEBERG_TABLE_PATH_MODIFIED_DATA =
            "s3a://" + ICEBERG_CONTAINER + "/my-table-modified-data/";

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new TestExecutor();
        LocalCloudUtilAdobeMock.startS3CloudEnvironment(true);
        testExecutor.executorId = "cloud";
        testExecutor.stripSubstring = "//DB:";
        LangExecutionUtil.setUp(CONFIG_FILE_NAME, testExecutor);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, CONFIG_FILE_NAME);

        // create the iceberg bucket
        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_HOSTNAME); // endpoint pointing to S3 mock server
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        S3Client client = builder.build();
        client.createBucket(CreateBucketRequest.builder().bucket(ICEBERG_CONTAINER).build());
        client.close();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
        LocalCloudUtilAdobeMock.shutdownSilently();
    }

    @Parameters(name = "IcebergTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }

    private static DataFile writeFile(String filename, List<Record> records, String location) throws IOException {
        Path path = new Path(location, filename);
        FileFormat fileFormat = FileFormat.fromFileName(filename);
        Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

        FileAppender<Record> fileAppender =
                new GenericAppenderFactory(IcebergTest.SCHEMA).newAppender(fromPath(path, CONF), fileFormat);
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
                    IcebergTest.ICEBERG_TABLE_PATH);
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
                    IcebergTest.ICEBERG_TABLE_PATH_FORMAT_VERSION_2);
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
                    IcebergTest.ICEBERG_TABLE_PATH_MIXED_DATA_FORMAT);
            DataFile avroFile = writeFile(FileFormat.AVRO.addExtension("avro-file"), fileSecondSnapshotRecords,
                    IcebergTest.ICEBERG_TABLE_PATH_MIXED_DATA_FORMAT);

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
                    IcebergTest.ICEBERG_TABLE_PATH_MULTIPLE_DATA_FILES);
            DataFile file2 = writeFile(FileFormat.PARQUET.addExtension("file-2"), fileSecondSnapshotRecords,
                    IcebergTest.ICEBERG_TABLE_PATH_MULTIPLE_DATA_FILES);

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
                    IcebergTest.ICEBERG_TABLE_PATH_MODIFIED_DATA);
            DataFile file2 = writeFile(FileFormat.PARQUET.addExtension("file-2"), fileSecondSnapshotRecords,
                    IcebergTest.ICEBERG_TABLE_PATH_MODIFIED_DATA);

            modifiedData.newAppend().appendFile(file1).appendFile(file2).commit();
            modifiedData.newDelete().deleteFile(file1).commit();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
}
