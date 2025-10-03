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

import static org.apache.asterix.api.common.LocalCloudUtilAdobeMock.fillConfigTemplate;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.CONFIG_FILE_TEMPLATE;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.MOCK_SERVER_HOSTNAME_FRAGMENT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.asterix.api.common.LocalCloudUtilAdobeMock;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.Description;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.hyracks.cloud.util.CloudRetryableRequestUtil;
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

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

/**
 * Run tests in cloud deployment environment with simulated unstable connection
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CloudStorageUnstableTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final TestCaseContext tcCtx;
    private static final String SUITE_TESTS = "testsuite_cloud_storage.xml";
    private static final String ONLY_TESTS = "testsuite_cloud_storage_only.xml";
    private static final String CONFIG_FILE_NAME = "src/test/resources/cc-cloud-storage.conf";
    private static final String DELTA_RESULT_PATH = "results_cloud";
    private static final String EXCLUDED_TESTS = "MP";

    public CloudStorageUnstableTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(CloudRetryableRequestUtil.CLOUD_UNSTABLE_MODE, "true");
        S3MockContainer s3Mock = LocalCloudUtilAdobeMock.startS3CloudEnvironment(true, true);
        fillConfigTemplate(MOCK_SERVER_HOSTNAME_FRAGMENT + s3Mock.getHttpServerPort(), CONFIG_FILE_TEMPLATE,
                CONFIG_FILE_NAME);
        TestExecutor testExecutor = new TestExecutor(DELTA_RESULT_PATH);
        testExecutor.executorId = "cloud";
        testExecutor.stripSubstring = "//DB:";
        LangExecutionUtil.setUp(CONFIG_FILE_NAME, testExecutor);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, CONFIG_FILE_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.clearProperty(CloudRetryableRequestUtil.CLOUD_UNSTABLE_MODE);
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "CloudStorageUnstableTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        long seed = System.nanoTime();
        Random random = new Random(seed);
        LOGGER.info("CloudStorageUnstableTest seed {}", seed);
        Collection<Object[]> tests = LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
        List<Object[]> selected = new ArrayList<>();
        for (Object[] test : tests) {
            // Select 10% of the tests randomly
            if (random.nextInt(10) == 0) {
                selected.add(test);
            }
        }
        return selected;
    }

    @Test
    public void test() throws Exception {
        List<TestCase.CompilationUnit> cu = tcCtx.getTestCase().getCompilationUnit();
        Assume.assumeTrue(cu.size() > 1 || !EXCLUDED_TESTS.equals(getText(cu.get(0).getDescription())));
        LangExecutionUtil.test(tcCtx);
    }

    private static String getText(Description description) {
        return description == null ? "" : description.getValue();
    }
}
