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

import static org.apache.asterix.test.common.TestConstants.Azure.*;
import static org.apache.asterix.test.external_dataset.ExternalDatasetTestUtils.*;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.PublicAccessType;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.AccountSasPermission;
import com.azure.storage.common.sas.AccountSasResourceType;
import com.azure.storage.common.sas.AccountSasService;
import com.azure.storage.common.sas.AccountSasSignatureValues;

// TODO(Hussain): Need to run the test manually to ensure new tests (anonymous access) are working fine
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzureBlobStorageExternalDatasetTest {

    private static final Logger LOGGER = LogManager.getLogger();

    // subclasses of this class MUST instantiate these variables before using them to avoid unexpected behavior
    static String SUITE_TESTS;
    static String ONLY_TESTS;
    static String TEST_CONFIG_FILE_NAME;
    static Runnable PREPARE_PLAYGROUND_CONTAINER;
    static Runnable PREPARE_DYNAMIC_PREFIX_AT_START_CONTAINER;
    static Runnable PREPARE_FIXED_DATA_CONTAINER;
    static Runnable PREPARE_INCLUDE_EXCLUDE_CONTAINER;
    static Runnable PREPARE_BOM_FILE_BUCKET;

    // Base directory paths for data files
    private static final String JSON_DATA_PATH = joinPath("data", "json");
    private static final String CSV_DATA_PATH = joinPath("data", "csv");
    private static final String TSV_DATA_PATH = joinPath("data", "tsv");
    private static final String PARQUET_RAW_DATA_PATH = joinPath("data", "hdfs", "parquet");
    public static final String EXTERNAL_FILTER_DATA_PATH = joinPath("data", "json", "external-filter");

    // Region, container and definitions
    private static final String PLAYGROUND_CONTAINER = "playground";
    private static final String DYNAMIC_PREFIX_AT_START_CONTAINER = "dynamic-prefix-at-start-container";
    private static final String FIXED_DATA_CONTAINER = "fixed-data"; // Do not use, has fixed data
    private static final String INCLUDE_EXCLUDE_CONTAINER = "include-exclude";
    private static final String BOM_FILE_CONTAINER = "bom-file-container";
    private static final String PUBLIC_ACCESS_CONTAINER = "public-access-container"; // requires no authentication

    private static final Set<String> fileNames = new HashSet<>();

    // Create a BlobServiceClient object which will be used to create a container client
    private static BlobServiceClient blobServiceClient;
    private static BlobContainerClient playgroundContainer;
    private static BlobContainerClient dynamicPrefixAtStartContainer;
    private static BlobContainerClient publicAccessContainer;
    private static BlobContainerClient fixedDataContainer;
    private static BlobContainerClient mixedDataContainer;
    private static BlobContainerClient bomContainer;

    protected TestCaseContext tcCtx;

    public AzureBlobStorageExternalDatasetTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new AzureTestExecutor();
        ExternalDatasetTestUtils.createBinaryFiles(PARQUET_RAW_DATA_PATH);
        createBinaryFilesRecursively(EXTERNAL_FILTER_DATA_PATH);
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
        PREPARE_PLAYGROUND_CONTAINER = ExternalDatasetTestUtils::preparePlaygroundContainer;
        PREPARE_DYNAMIC_PREFIX_AT_START_CONTAINER = ExternalDatasetTestUtils::prepareDynamicPrefixAtStartContainer;
        PREPARE_FIXED_DATA_CONTAINER = ExternalDatasetTestUtils::prepareFixedDataContainer;
        PREPARE_INCLUDE_EXCLUDE_CONTAINER = ExternalDatasetTestUtils::prepareMixedDataContainer;
        PREPARE_BOM_FILE_BUCKET = ExternalDatasetTestUtils::prepareBomFileContainer;
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
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
        builder.credential(new StorageSharedKeyCredential(AZURITE_ACCOUNT_NAME_DEFAULT, AZURITE_ACCOUNT_KEY_DEFAULT));
        builder.endpoint(AZURITE_ENDPOINT);
        blobServiceClient = builder.buildClient();
        LOGGER.info("Azurite Blob Service client created successfully");

        // delete all existing containers
        deleteContainersSilently();

        LOGGER.info("Creating containers");
        playgroundContainer = blobServiceClient.createBlobContainer(PLAYGROUND_CONTAINER);
        dynamicPrefixAtStartContainer = blobServiceClient.createBlobContainer(DYNAMIC_PREFIX_AT_START_CONTAINER);
        fixedDataContainer = blobServiceClient.createBlobContainer(FIXED_DATA_CONTAINER);
        mixedDataContainer = blobServiceClient.createBlobContainer(INCLUDE_EXCLUDE_CONTAINER);
        bomContainer = blobServiceClient.createBlobContainer(BOM_FILE_CONTAINER);
        publicAccessContainer = blobServiceClient.createBlobContainer(PUBLIC_ACCESS_CONTAINER);
        publicAccessContainer.setAccessPolicy(PublicAccessType.CONTAINER, null);
        LOGGER.info("Created containers successfully");

        // Load 20 files in the public access container
        Path filePath = Paths.get(JSON_DATA_PATH, "single-line", "20-records.json");
        publicAccessContainer.getBlobClient("20-records.json").uploadFromFile(filePath.toAbsolutePath().toString());

        // Generate the SAS token for the SAS test cases
        sasToken = generateSasToken();

        // Create the container and upload some json files
        // Create the bucket and upload some json files
        setDataPaths(JSON_DATA_PATH, CSV_DATA_PATH, TSV_DATA_PATH);
        setUploaders(AzureBlobStorageExternalDatasetTest::loadPlaygroundData,
                AzureBlobStorageExternalDatasetTest::loadDynamicPrefixAtStartData,
                AzureBlobStorageExternalDatasetTest::loadFixedData, AzureBlobStorageExternalDatasetTest::loadMixedData,
                AzureBlobStorageExternalDatasetTest::loadBomData);
        PREPARE_PLAYGROUND_CONTAINER.run();
        PREPARE_DYNAMIC_PREFIX_AT_START_CONTAINER.run();
        PREPARE_FIXED_DATA_CONTAINER.run();
        PREPARE_INCLUDE_EXCLUDE_CONTAINER.run();
        PREPARE_BOM_FILE_BUCKET.run();
    }

    private static String generateSasToken() {
        OffsetDateTime expiry = OffsetDateTime.now().plus(1, ChronoUnit.YEARS);
        AccountSasService service = AccountSasService.parse("b");
        AccountSasPermission permission = AccountSasPermission.parse("acdlpruw");
        AccountSasResourceType type = AccountSasResourceType.parse("cos");
        return blobServiceClient.generateAccountSas(new AccountSasSignatureValues(expiry, permission, service, type));
    }

    private static void loadPlaygroundData(String key, String content, boolean fromFile, boolean gzipped) {
        if (!fromFile) {
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes())) {
                playgroundContainer.getBlobClient(key).upload(inputStream, inputStream.available());
            } catch (IOException ex) {
                throw new IllegalArgumentException(ex.toString());
            }
        } else {
            if (!gzipped) {
                playgroundContainer.getBlobClient(key).uploadFromFile(content);
            } else {
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                    gzipOutputStream.write(Files.readAllBytes(Paths.get(content)));
                    gzipOutputStream.close(); // Need to close or data will be invalid
                    byte[] gzipBytes = byteArrayOutputStream.toByteArray();

                    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(gzipBytes)) {
                        playgroundContainer.getBlobClient(key).upload(inputStream, inputStream.available());
                    } catch (IOException ex) {
                        throw new IllegalArgumentException(ex.toString());
                    }
                } catch (IOException ex) {
                    throw new IllegalArgumentException(ex.toString());
                }
            }
        }
    }

    private static void loadDynamicPrefixAtStartData(String key, String content, boolean fromFile, boolean gzipped) {
        if (!fromFile) {
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes())) {
                dynamicPrefixAtStartContainer.getBlobClient(key).upload(inputStream, inputStream.available());
            } catch (IOException ex) {
                throw new IllegalArgumentException(ex.toString());
            }
        } else {
            if (!gzipped) {
                dynamicPrefixAtStartContainer.getBlobClient(key).uploadFromFile(content);
            } else {
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                    gzipOutputStream.write(Files.readAllBytes(Paths.get(content)));
                    gzipOutputStream.close(); // Need to close or data will be invalid
                    byte[] gzipBytes = byteArrayOutputStream.toByteArray();

                    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(gzipBytes)) {
                        dynamicPrefixAtStartContainer.getBlobClient(key).upload(inputStream, inputStream.available());
                    } catch (IOException ex) {
                        throw new IllegalArgumentException(ex.toString());
                    }
                } catch (IOException ex) {
                    throw new IllegalArgumentException(ex.toString());
                }
            }
        }
    }

    private static void loadFixedData(String key, String content, boolean fromFile, boolean gzipped) {
        if (!fromFile) {
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes())) {
                fixedDataContainer.getBlobClient(key).upload(inputStream, inputStream.available());
            } catch (IOException ex) {
                throw new IllegalArgumentException(ex.toString());
            }
        } else {
            if (!gzipped) {
                fixedDataContainer.getBlobClient(key).uploadFromFile(content);
            } else {
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                    gzipOutputStream.write(Files.readAllBytes(Paths.get(content)));
                    gzipOutputStream.close(); // Need to close or data will be invalid
                    byte[] gzipBytes = byteArrayOutputStream.toByteArray();

                    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(gzipBytes)) {
                        fixedDataContainer.getBlobClient(key).upload(inputStream, inputStream.available());
                    } catch (IOException ex) {
                        throw new IllegalArgumentException(ex.toString());
                    }
                } catch (IOException ex) {
                    throw new IllegalArgumentException(ex.toString());
                }
            }
        }
    }

    private static void loadMixedData(String key, String content, boolean fromFile, boolean gzipped) {
        if (!fromFile) {
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes())) {
                mixedDataContainer.getBlobClient(key).upload(inputStream, inputStream.available());
            } catch (IOException ex) {
                throw new IllegalArgumentException(ex.toString());
            }
        } else {
            if (!gzipped) {
                mixedDataContainer.getBlobClient(key).uploadFromFile(content);
            } else {
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                    gzipOutputStream.write(Files.readAllBytes(Paths.get(content)));
                    gzipOutputStream.close(); // Need to close or data will be invalid
                    byte[] gzipBytes = byteArrayOutputStream.toByteArray();

                    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(gzipBytes)) {
                        mixedDataContainer.getBlobClient(key).upload(inputStream, inputStream.available());
                    } catch (IOException ex) {
                        throw new IllegalArgumentException(ex.toString());
                    }
                } catch (IOException ex) {
                    throw new IllegalArgumentException(ex.toString());
                }
            }
        }
    }

    private static void loadBomData(String key, String content, boolean fromFile, boolean gzipped) {
        if (!fromFile) {
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getBytes())) {
                bomContainer.getBlobClient(key).upload(inputStream, inputStream.available());
            } catch (IOException ex) {
                throw new IllegalArgumentException(ex.toString());
            }
        } else {
            if (!gzipped) {
                bomContainer.getBlobClient(key).uploadFromFile(content);
            } else {
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                    gzipOutputStream.write(Files.readAllBytes(Paths.get(content)));
                    gzipOutputStream.close(); // Need to close or data will be invalid
                    byte[] gzipBytes = byteArrayOutputStream.toByteArray();

                    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(gzipBytes)) {
                        bomContainer.getBlobClient(key).upload(inputStream, inputStream.available());
                    } catch (IOException ex) {
                        throw new IllegalArgumentException(ex.toString());
                    }
                } catch (IOException ex) {
                    throw new IllegalArgumentException(ex.toString());
                }
            }
        }
    }

    static class AzureTestExecutor extends TestExecutor {

        @Override
        public void executeTestFile(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx,
                String statement, boolean isDmlRecoveryTest, ProcessBuilder pb, TestCase.CompilationUnit cUnit,
                MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath)
                throws Exception {
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
                            queryCount, expectedResultFileCtxs, testFile, actualPath);
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

    private static void deleteContainersSilently() {
        deleteContainerSilently(PLAYGROUND_CONTAINER);
        deleteContainerSilently(DYNAMIC_PREFIX_AT_START_CONTAINER);
        deleteContainerSilently(FIXED_DATA_CONTAINER);
        deleteContainerSilently(PUBLIC_ACCESS_CONTAINER);
        deleteContainerSilently(INCLUDE_EXCLUDE_CONTAINER);
        deleteContainerSilently(BOM_FILE_CONTAINER);
    }

    private static void deleteContainerSilently(String containerName) {
        LOGGER.info("Deleting container " + containerName);
        try {
            blobServiceClient.deleteBlobContainer(containerName);
        } catch (Exception ex) {
            // Do nothing
            LOGGER.warn("Ignoring encountered error while deleting container {}", ex.getMessage());
        }
        LOGGER.info("Container " + containerName + " deleted successfully");
    }
}
