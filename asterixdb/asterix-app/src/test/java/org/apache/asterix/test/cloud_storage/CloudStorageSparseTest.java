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

import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.api.common.LocalCloudUtilAdobeMock;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.Description;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

/**
 * Run tests in cloud deployment environment
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CloudStorageSparseTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final TestCaseContext tcCtx;
    public static final String SUITE_TESTS = "testsuite_cloud_storage.xml";
    public static final String ONLY_TESTS = "testsuite_cloud_storage_only.xml";
    public static final String CONFIG_FILE_NAME = "src/test/resources/cc-cloud-storage-sparse.conf";
    public static final String DELTA_RESULT_PATH = "results_cloud";
    public static final String EXCLUDED_TESTS = "MP";

    public static final String PLAYGROUND_CONTAINER = "playground";
    public static final String MOCK_SERVER_REGION = "us-west-2";
    public static final int MOCK_SERVER_PORT = 8001;
    public static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;

    public CloudStorageSparseTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        TestExecutor testExecutor = new TestExecutor(DELTA_RESULT_PATH);
        setupEnv(testExecutor);
    }

    public static void setupEnv(TestExecutor testExecutor) throws Exception {
        LocalCloudUtilAdobeMock.startS3CloudEnvironment(true);
        testExecutor.executorId = "cloud";
        testExecutor.stripSubstring = "//DB:";
        LangExecutionUtil.setUp(CONFIG_FILE_NAME, testExecutor);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, CONFIG_FILE_NAME);

        // create the playground bucket and leave it empty, just for external collection-based tests
        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(MOCK_SERVER_HOSTNAME); // endpoint pointing to S3 mock server
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        S3Client client = builder.build();
        client.createBucket(CreateBucketRequest.builder().bucket(PLAYGROUND_CONTAINER).build());
        client.close();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
        LocalCloudUtilAdobeMock.shutdownSilently();
    }

    @Parameters(name = "CloudStorageSparseTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
    }

    @Test
    public void test() throws Exception {
        List<TestCase.CompilationUnit> cu = tcCtx.getTestCase().getCompilationUnit();
        Assume.assumeTrue(cu.size() > 1 || !EXCLUDED_TESTS.equals(getText(cu.get(0).getDescription())));
        LangExecutionUtil.test(tcCtx);
        IBufferCache bufferCache;
        for (NodeControllerService nc : ExecutionTestUtil.integrationUtil.ncs) {
            bufferCache = ((INcApplicationContext) nc.getApplicationContext()).getBufferCache();
            Assert.assertTrue(((BufferCache) bufferCache).isClean());
        }
    }

    private static String getText(Description description) {
        return description == null ? "" : description.getValue();
    }
}
