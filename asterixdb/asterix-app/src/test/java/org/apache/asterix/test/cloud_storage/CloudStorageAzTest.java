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
package org.apache.asterix.test.cloud_storage;

import static org.apache.asterix.api.common.LocalCloudUtil.CLOUD_STORAGE_BUCKET;
import static org.apache.asterix.api.common.LocalCloudUtilAdobeMock.fillConfigTemplate;
import static org.apache.asterix.cloud.azure.LSMAzBlobStorageTest.AZURITE_CONTAINER_VER;
import static org.apache.asterix.test.cloud_storage.CloudStorageGCSTest.S3_ONLY;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.asterix.api.common.LocalCloudUtilAdobeMock;
import org.apache.asterix.cloud.azure.LSMAzBlobStorageTest;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.Description;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.utility.MountableFile;

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import reactor.netty.http.client.HttpClient;

/**
 * Run tests in cloud deployment environment
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CloudStorageAzTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final TestCaseContext tcCtx;
    private static final String SUITE_TESTS = "testsuite_cloud_storage.xml";
    private static final String ONLY_TESTS = "testsuite_cloud_storage_only.xml";
    private static final String CONFIG_FILE = "target/cc-cloud-storage-azblob.conf";
    private static final String CONFIG_FILE_TEMPLATE = "src/test/resources/cc-cloud-storage-azblob.ftl";
    private static final String DELTA_RESULT_PATH = "results_cloud";
    private static final String EXCLUDED_TESTS = "MP";

    private static AzuriteContainer azBlob;

    public CloudStorageAzTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        LocalCloudUtilAdobeMock.startS3CloudEnvironment(true, true);
        LSMAzBlobStorageTest.generateSelfSignedTLS();
        MountableFile azureCert = MountableFile.forHostPath("target/azure_test.pfx");
        azBlob = new AzuriteContainer(AZURITE_CONTAINER_VER).withSsl(azureCert, "password");
        azBlob.start();
        SslContext insecureSslContext =
                SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(azBlob.getConnectionString())
                .httpClient(new NettyAsyncHttpClientBuilder(
                        HttpClient.create().secure(sslSpec -> sslSpec.sslContext(insecureSslContext).build())).build())
                .buildClient();
        URI blobStore = URI.create(blobServiceClient.getAccountUrl());
        String endpoint = blobStore.getScheme() + "://" + blobStore.getAuthority();
        fillConfigTemplate(endpoint, CONFIG_FILE_TEMPLATE, CONFIG_FILE);

        cleanup(blobServiceClient);
        initialize(blobServiceClient);

        TestExecutor testExecutor = new TestExecutor(DELTA_RESULT_PATH);
        testExecutor.executorId = "cloud";
        testExecutor.stripSubstring = "//DB:";
        LangExecutionUtil.setUp(CONFIG_FILE, testExecutor);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, CONFIG_FILE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
        LocalCloudUtilAdobeMock.shutdownSilently();
        azBlob.close();
        azBlob.stop();
    }

    @Parameters(name = "CloudStorageAzBlobTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        long seed = System.nanoTime();
        Random random = new Random(seed);
        LOGGER.info("CloudStorageAzBlobTest seed {}", seed);
        Collection<Object[]> tests = LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
        List<Object[]> selected = new ArrayList<>();
        for (Object[] test : tests) {
            if (!Objects.equals(((TestCaseContext) test[0]).getTestGroups()[0].getName(), "sqlpp_queries")) {
                selected.add(test);
            }
            // Select 10% of the tests randomly
            else if (random.nextInt(10) == 0) {
                selected.add(test);
            }
        }
        return selected;
    }

    @Test
    public void test() throws Exception {
        List<TestCase.CompilationUnit> cu = tcCtx.getTestCase().getCompilationUnit();
        Assume.assumeTrue(cu.size() > 1 || (!EXCLUDED_TESTS.equals(getText(cu.get(0).getDescription()))
                && !S3_ONLY.equals(getText(cu.get(0).getDescription()))));
        LangExecutionUtil.test(tcCtx);
    }

    private static String getText(Description description) {
        return description == null ? "" : description.getValue();
    }

    private static void cleanup(BlobServiceClient blobServiceClient) {
        blobServiceClient.deleteBlobContainerIfExists(CLOUD_STORAGE_BUCKET);
    }

    private static void initialize(BlobServiceClient blobServiceClient) {
        blobServiceClient.createBlobContainerIfNotExists(CLOUD_STORAGE_BUCKET);
    }
}
