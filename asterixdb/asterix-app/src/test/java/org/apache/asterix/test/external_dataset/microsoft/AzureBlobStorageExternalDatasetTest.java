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
package org.apache.asterix.test.external_dataset.microsoft;

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

@Ignore
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzureBlobStorageExternalDatasetTest {

    private static final Logger LOGGER = LogManager.getLogger();

    // subclasses of this class MUST instantiate these variables before using them to avoid unexpected behavior
    static String SUITE_TESTS;
    static String ONLY_TESTS;
    static String TEST_CONFIG_FILE_NAME;
    static Runnable PREPARE_PLAYGROUND_CONTAINER;
    static Runnable PREPARE_FIXED_DATA_CONTAINER;
    static Runnable PREPARE_MIXED_DATA_CONTAINER;

    // Base directory paths for data files
    private static final String JSON_DATA_PATH = joinPath("data", "json");
    private static final String CSV_DATA_PATH = joinPath("data", "csv");
    private static final String TSV_DATA_PATH = joinPath("data", "tsv");
    private static final String MIXED_DATA_PATH = joinPath("data", "mixed");

    // Service endpoint
    private static final int BLOB_SERVICE_PORT = 20000;
    private static final String BLOB_SERVICE_ENDPOINT = "http://localhost:" + BLOB_SERVICE_PORT;

    // Region, container and definitions
    private static final String PLAYGROUND_CONTAINER = "playground";
    private static final String FIXED_DATA_CONTAINER = "fixed-data"; // Do not use, has fixed data
    private static final String INCLUDE_EXCLUDE_CONTAINER = "include-exclude";
    private static final String JSON_DEFINITION = "json-data/reviews/";
    private static final String CSV_DEFINITION = "csv-data/reviews/";
    private static final String TSV_DEFINITION = "tsv-data/reviews/";

    // This is used for a test to generate over 1000 number of files
    private static final String OVER_1000_OBJECTS_PATH = "over-1000-objects";
    private static final int OVER_1000_OBJECTS_COUNT = 2999;

    private static final Set<String> fileNames = new HashSet<>();

    // Create a BlobServiceClient object which will be used to create a container client
    private static final String connectionString = "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=" + BLOB_SERVICE_ENDPOINT + "/devstoreaccount1;";
    private static BlobServiceClient blobServiceClient;
    private static BlobContainerClient playgroundContainer;

    protected TestCaseContext tcCtx;

    public AzureBlobStorageExternalDatasetTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new AzureTestExecutor();
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor);
        setNcEndpoints(testExecutor);
        createBlobServiceClient();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "AzureBlobStorageExternalDatasetTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        SUITE_TESTS = "testsuite_external_dataset_azure_blob_storage.xml";
        ONLY_TESTS = "only_external_dataset.xml";
        TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
        PREPARE_PLAYGROUND_CONTAINER = AzureBlobStorageExternalDatasetTest::preparePlaygroundContainer;
        PREPARE_FIXED_DATA_CONTAINER = AzureBlobStorageExternalDatasetTest::prepareFixedDataContainer;
        PREPARE_MIXED_DATA_CONTAINER = AzureBlobStorageExternalDatasetTest::prepareMixedDataContainer;
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

    private static void createBlobServiceClient() {
        LOGGER.info("Creating Azurite Blob Service client");
        blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        LOGGER.info("Azurite Blob Service client created successfully");

        // Create the container and upload some json files
        PREPARE_PLAYGROUND_CONTAINER.run();
        PREPARE_FIXED_DATA_CONTAINER.run();
        PREPARE_MIXED_DATA_CONTAINER.run();
    }

    /**
     * Creates a container and fills it with some files for testing purpose.
     */
    private static void preparePlaygroundContainer() {
        deleteContainerSilently(PLAYGROUND_CONTAINER);

        LOGGER.info("creating container " + PLAYGROUND_CONTAINER);
        playgroundContainer = blobServiceClient.createBlobContainer(PLAYGROUND_CONTAINER);
        LOGGER.info("container " + PLAYGROUND_CONTAINER + " created successfully");

        LOGGER.info("Adding JSON files");
        loadJsonFiles();
        LOGGER.info("JSON Files added successfully");

        LOGGER.info("Adding CSV files");
        loadCsvFiles();
        LOGGER.info("CSV Files added successfully");

        LOGGER.info("Adding TSV files");
        loadTsvFiles();
        LOGGER.info("TSV Files added successfully");

        LOGGER.info("Loading " + OVER_1000_OBJECTS_COUNT + " into " + OVER_1000_OBJECTS_PATH);
        loadLargeNumberOfFiles();
        LOGGER.info("Added " + OVER_1000_OBJECTS_COUNT + " files into " + OVER_1000_OBJECTS_PATH + " successfully");
    }

    /**
     * This container is being filled by fixed data, a test is counting all records. If this container is
     * changed, the test case will fail and its result will need to be updated each time
     */
    private static void prepareFixedDataContainer() {
        deleteContainerSilently(FIXED_DATA_CONTAINER);
        LOGGER.info("creating container " + FIXED_DATA_CONTAINER);
        BlobContainerClient fixedDataContainer = blobServiceClient.createBlobContainer(FIXED_DATA_CONTAINER);
        LOGGER.info("container " + FIXED_DATA_CONTAINER + " created successfully");

        LOGGER.info("Loading fixed data to " + FIXED_DATA_CONTAINER);

        // Files data
        Path filePath = Paths.get(JSON_DATA_PATH, "single-line", "20-records.json");
        fixedDataContainer.getBlobClient("1.json").uploadFromFile(filePath.toString());
        fixedDataContainer.getBlobClient("2.json").uploadFromFile(filePath.toString());
        fixedDataContainer.getBlobClient("lvl1/3.json").uploadFromFile(filePath.toString());
        fixedDataContainer.getBlobClient("lvl1/4.json").uploadFromFile(filePath.toString());
        fixedDataContainer.getBlobClient("lvl1/lvl2/5.json").uploadFromFile(filePath.toString());
    }

    private static void loadJsonFiles() {
        String dataBasePath = JSON_DATA_PATH;
        String definition = JSON_DEFINITION;

        // Normal format
        String definitionSegment = "json";
        loadData(dataBasePath, "single-line", "20-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines", "20-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines-with-arrays", "5-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines-with-nested-objects", "5-records.json", definition, definitionSegment,
                false);

        definitionSegment = "json-array-of-objects";
        loadData(dataBasePath, "single-line", "array_of_objects.json", "json-data/", definitionSegment, false, false);

        // gz compressed format
        definitionSegment = "gz";
        loadGzData(dataBasePath, "single-line", "20-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines", "20-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines-with-arrays", "5-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines-with-nested-objects", "5-records.json", definition, definitionSegment,
                false);

        // Mixed normal and gz compressed format
        definitionSegment = "mixed";
        loadData(dataBasePath, "single-line", "20-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines", "20-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines-with-arrays", "5-records.json", definition, definitionSegment, false);
        loadData(dataBasePath, "multi-lines-with-nested-objects", "5-records.json", definition, definitionSegment,
                false);
        loadGzData(dataBasePath, "single-line", "20-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines", "20-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines-with-arrays", "5-records.json", definition, definitionSegment, false);
        loadGzData(dataBasePath, "multi-lines-with-nested-objects", "5-records.json", definition, definitionSegment,
                false);
    }

    private static void loadCsvFiles() {
        String dataBasePath = CSV_DATA_PATH;
        String definition = CSV_DEFINITION;

        // Normal format
        String definitionSegment = "csv";
        loadData(dataBasePath, "", "01.csv", definition, definitionSegment, false);
        loadData(dataBasePath, "", "02.csv", definition, definitionSegment, false);

        // gz compressed format
        definitionSegment = "gz";
        loadGzData(dataBasePath, "", "01.csv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "02.csv", definition, definitionSegment, false);

        // Mixed normal and gz compressed format
        definitionSegment = "mixed";
        loadData(dataBasePath, "", "01.csv", definition, definitionSegment, false);
        loadData(dataBasePath, "", "02.csv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "01.csv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "02.csv", definition, definitionSegment, false);
    }

    private static void loadTsvFiles() {
        String dataBasePath = TSV_DATA_PATH;
        String definition = TSV_DEFINITION;

        // Normal format
        String definitionSegment = "tsv";
        loadData(dataBasePath, "", "01.tsv", definition, definitionSegment, false);
        loadData(dataBasePath, "", "02.tsv", definition, definitionSegment, false);

        // gz compressed format
        definitionSegment = "gz";
        loadGzData(dataBasePath, "", "01.tsv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "02.tsv", definition, definitionSegment, false);

        // Mixed normal and gz compressed format
        definitionSegment = "mixed";
        loadData(dataBasePath, "", "01.tsv", definition, definitionSegment, false);
        loadData(dataBasePath, "", "02.tsv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "01.tsv", definition, definitionSegment, false);
        loadGzData(dataBasePath, "", "02.tsv", definition, definitionSegment, false);
    }

    private static void loadData(String fileBasePath, String filePathSegment, String filename, String definition,
            String definitionSegment, boolean removeExtension) {
        loadData(fileBasePath, filePathSegment, filename, definition, definitionSegment, removeExtension, true);
    }

    private static void loadData(String fileBasePath, String filePathSegment, String filename, String definition,
            String definitionSegment, boolean removeExtension, boolean copyToSubLevels) {
        // Files data
        Path filePath = Paths.get(fileBasePath, filePathSegment, filename);

        // Keep or remove the file extension
        Assert.assertFalse("Files with no extension are not supported yet for external datasets", removeExtension);
        String finalFileName;
        if (removeExtension) {
            finalFileName = FilenameUtils.removeExtension(filename);
        } else {
            finalFileName = filename;
        }

        // Files base definition
        filePathSegment = filePathSegment.isEmpty() ? "" : filePathSegment + "/";
        definitionSegment = definitionSegment.isEmpty() ? "" : definitionSegment + "/";
        String basePath = definition + filePathSegment + definitionSegment;

        // Load the data
        playgroundContainer.getBlobClient(basePath + finalFileName).uploadFromFile(filePath.toString());
        if (copyToSubLevels) {
            playgroundContainer.getBlobClient(basePath + "level1a/" + finalFileName)
                    .uploadFromFile(filePath.toString());
            playgroundContainer.getBlobClient(basePath + "level1b/" + finalFileName)
                    .uploadFromFile(filePath.toString());
            playgroundContainer.getBlobClient(basePath + "level1a/level2a/" + finalFileName)
                    .uploadFromFile(filePath.toString());
            playgroundContainer.getBlobClient(basePath + "level1a/level2b/" + finalFileName)
                    .uploadFromFile(filePath.toString());
        }
    }

    private static void loadGzData(String fileBasePath, String filePathSegment, String filename, String definition,
            String definitionSegment, boolean removeExtension) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {

            // Files data
            Path filePath = Paths.get(fileBasePath, filePathSegment, filename);

            // Get the compressed data
            gzipOutputStream.write(Files.readAllBytes(filePath));
            gzipOutputStream.close(); // Need to close or data will be invalid
            byte[] gzipBytes = byteArrayOutputStream.toByteArray();

            // Keep or remove the file extension
            Assert.assertFalse("Files with no extension are not supported yet for external datasets", removeExtension);
            String finalFileName;
            if (removeExtension) {
                finalFileName = FilenameUtils.removeExtension(filename);
            } else {
                finalFileName = filename;
            }
            finalFileName += ".gz";

            // Files base definition
            filePathSegment = filePathSegment.isEmpty() ? "" : filePathSegment + "/";
            definitionSegment = definitionSegment.isEmpty() ? "" : definitionSegment + "/";
            String basePath = definition + filePathSegment + definitionSegment;

            // Load the data
            ByteArrayInputStream inputStream = new ByteArrayInputStream(gzipBytes);
            playgroundContainer.getBlobClient(basePath + finalFileName).upload(inputStream, inputStream.available());
            inputStream.reset();
            playgroundContainer.getBlobClient(basePath + "level1a/" + finalFileName).upload(inputStream,
                    inputStream.available());
            inputStream.reset();
            playgroundContainer.getBlobClient(basePath + "level1b/" + finalFileName).upload(inputStream,
                    inputStream.available());
            inputStream.reset();
            playgroundContainer.getBlobClient(basePath + "level1a/level2a/" + finalFileName).upload(inputStream,
                    inputStream.available());
            inputStream.reset();
            playgroundContainer.getBlobClient(basePath + "level1a/level2b/" + finalFileName).upload(inputStream,
                    inputStream.available());
            closeInputStreamSilently(inputStream);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    private static void loadLargeNumberOfFiles() {
        ByteArrayInputStream inputStream = null;
        for (int i = 0; i < OVER_1000_OBJECTS_COUNT; i++) {
            inputStream = new ByteArrayInputStream(("{\"id\":" + i + "}").getBytes());
            playgroundContainer.getBlobClient(OVER_1000_OBJECTS_PATH + "/" + i + ".json").upload(inputStream,
                    inputStream.available());
        }
        closeInputStreamSilently(inputStream);
    }

    /**
     * Loads a combination of different file formats in the same path
     */
    private static void prepareMixedDataContainer() {
        deleteContainerSilently(INCLUDE_EXCLUDE_CONTAINER);
        LOGGER.info("creating container " + INCLUDE_EXCLUDE_CONTAINER);
        BlobContainerClient includeExcludeContainer = blobServiceClient.createBlobContainer(INCLUDE_EXCLUDE_CONTAINER);
        LOGGER.info("container " + INCLUDE_EXCLUDE_CONTAINER + " created successfully");

        // JSON
        ByteArrayInputStream inputStream = new ByteArrayInputStream(("{\"id\":" + 1 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/extension/" + "hello-world-2018.json")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 2 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/extension/" + "hello-world-2019.json")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 3 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/extension/" + "hello-world-2020.json")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 4 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/EXTENSION/" + "goodbye-world-2018.json")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 5 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/EXTENSION/" + "goodbye-world-2019.json")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 6 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/EXTENSION/" + "goodbye-world-2020.json")
                .upload(inputStream, inputStream.available());

        // CSV
        inputStream = new ByteArrayInputStream(("7,\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/csv/extension/" + "hello-world-2018.csv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("8,\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/csv/extension/" + "hello-world-2019.csv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("9,\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/csv/extension/" + "hello-world-2020.csv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("10,\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/csv/EXTENSION/" + "goodbye-world-2018.csv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("11,\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/csv/EXTENSION/" + "goodbye-world-2019.csv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("12,\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/csv/EXTENSION/" + "goodbye-world-2020.csv")
                .upload(inputStream, inputStream.available());

        // TSV
        inputStream = new ByteArrayInputStream(("13\t\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/tsv/extension/" + "hello-world-2018.tsv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("14\t\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/tsv/extension/" + "hello-world-2019.tsv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("15\t\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/tsv/extension/" + "hello-world-2020.tsv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("16\t\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/tsv/EXTENSION/" + "goodbye-world-2018.tsv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("17\t\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/tsv/EXTENSION/" + "goodbye-world-2019.tsv")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("18\t\"good\"").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/tsv/EXTENSION/" + "goodbye-world-2020.tsv")
                .upload(inputStream, inputStream.available());

        // JSON no extension
        inputStream = new ByteArrayInputStream(("{\"id\":" + 1 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/no-extension/" + "hello-world-2018")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 2 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/no-extension/" + "hello-world-2019")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 3 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/no-extension/" + "hello-world-2020")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 4 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/NO-EXTENSION/" + "goodbye-world-2018")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 5 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/NO-EXTENSION/" + "goodbye-world-2019")
                .upload(inputStream, inputStream.available());
        inputStream = new ByteArrayInputStream(("{\"id\":" + 6 + "}").getBytes());
        includeExcludeContainer.getBlobClient(MIXED_DATA_PATH + "/json/NO-EXTENSION/" + "goodbye-world-2020")
                .upload(inputStream, inputStream.available());

        closeInputStreamSilently(inputStream);
    }

    static class AzureTestExecutor extends TestExecutor {

        public void executeTestFile(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx,
                String statement, boolean isDmlRecoveryTest, ProcessBuilder pb, TestCase.CompilationUnit cUnit,
                MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath,
                BitSet expectedWarnings) throws Exception {
            String[] lines;
            switch (ctx.getType()) {
                case "container":
                    // <container> <def> <sub-path:new_fname:src_file1,sub-path:new_fname:src_file2,sub-path:src_file3>
                    lines = TestExecutor.stripAllComments(statement).trim().split("\n");
                    String lastLine = lines[lines.length - 1];
                    String[] command = lastLine.trim().split(" ");
                    int length = command.length;
                    if (length != 3) {
                        throw new Exception("invalid create container format");
                    }
                    dropRecreateContainer(command[0], command[1], command[2]);
                    break;
                default:
                    super.executeTestFile(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit,
                            queryCount, expectedResultFileCtxs, testFile, actualPath, expectedWarnings);
            }
        }
    }

    private static void dropRecreateContainer(String containerName, String definition, String files) {
        String definitionPath = definition + (definition.endsWith("/") ? "" : "/");
        String[] fileSplits = files.split(",");

        LOGGER.info("Deleting container " + containerName);
        try {
            blobServiceClient.deleteBlobContainer(containerName);
        } catch (Exception ex) {
            // Ignore
        }
        LOGGER.info("Container " + containerName + " deleted successfully");

        BlobContainerClient containerClient;
        LOGGER.info("Creating container " + containerName);
        containerClient = blobServiceClient.createBlobContainer(containerName);
        LOGGER.info("Uploading to container " + containerName + " definition " + definitionPath);
        fileNames.clear();
        for (String fileSplit : fileSplits) {
            String[] pathAndSourceFile = fileSplit.split(":");
            int size = pathAndSourceFile.length;
            String path;
            String sourceFilePath;
            String uploadedFileName;
            if (size == 1) {
                // case: playground json-data/reviews SOURCE_FILE1,SOURCE_FILE2
                path = definitionPath;
                sourceFilePath = pathAndSourceFile[0];
                uploadedFileName = FilenameUtils.getName(pathAndSourceFile[0]);
            } else if (size == 2) {
                // case: playground json-data/reviews level1/sub-level:SOURCE_FILE1,level2/sub-level:SOURCE_FILE2
                String subPathOrNewFileName = pathAndSourceFile[0];
                if (subPathOrNewFileName.startsWith("$$")) {
                    path = definitionPath;
                    sourceFilePath = pathAndSourceFile[1];
                    uploadedFileName = subPathOrNewFileName.substring(2);
                } else {
                    path = definitionPath + subPathOrNewFileName + (subPathOrNewFileName.endsWith("/") ? "" : "/");
                    sourceFilePath = pathAndSourceFile[1];
                    uploadedFileName = FilenameUtils.getName(pathAndSourceFile[1]);
                }
            } else if (size == 3) {
                path = definitionPath + pathAndSourceFile[0] + (pathAndSourceFile[0].endsWith("/") ? "" : "/");
                uploadedFileName = pathAndSourceFile[1];
                sourceFilePath = pathAndSourceFile[2];

            } else {
                throw new IllegalArgumentException();
            }

            String keyPath = path + uploadedFileName;
            int k = 1;
            while (fileNames.contains(keyPath)) {
                keyPath = path + (k++) + uploadedFileName;
            }
            fileNames.add(keyPath);
            containerClient.getBlobClient(keyPath).uploadFromFile(sourceFilePath);
        }
        LOGGER.info("Done creating container with data");
    }

    private static void deleteContainerSilently(String containerName) {
        LOGGER.info("Deleting container " + containerName);
        try {
            blobServiceClient.deleteBlobContainer(containerName);
        } catch (Exception ex) {
            // Do nothing
        }
        LOGGER.info("Container " + containerName + " deleted successfully");
    }

    private static void closeInputStreamSilently(InputStream inputStream) {
        try {
            inputStream.close();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }
}
