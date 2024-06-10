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
import static org.apache.asterix.api.common.LocalCloudUtil.MOCK_SERVER_REGION;

import java.util.Collection;
import java.util.List;

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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;

/**
 * Run tests in cloud deployment environment
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
public class CloudStorageGCSTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final TestCaseContext tcCtx;
    private static final String SUITE_TESTS = "testsuite_cloud_storage.xml";
    private static final String ONLY_TESTS = "testsuite_cloud_storage_only.xml";
    private static final String CONFIG_FILE_NAME = "src/test/resources/cc-cloud-storage-gcs.conf";
    private static final String DELTA_RESULT_PATH = "results_cloud";
    private static final String EXCLUDED_TESTS = "MP";
    public static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:4443";
    private static final String MOCK_SERVER_PROJECT_ID = "asterixdb-gcs-test-project-id";

    public CloudStorageGCSTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Storage storage = StorageOptions.newBuilder().setHost(MOCK_SERVER_HOSTNAME)
                .setCredentials(NoCredentials.getInstance()).setProjectId(MOCK_SERVER_PROJECT_ID).build().getService();
        cleanup(storage);
        initialize(storage);
        storage.close();
        TestExecutor testExecutor = new TestExecutor(DELTA_RESULT_PATH);
        testExecutor.executorId = "cloud";
        testExecutor.stripSubstring = "//DB:";
        LangExecutionUtil.setUp(CONFIG_FILE_NAME, testExecutor);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, CONFIG_FILE_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "CloudStorageGCSTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
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

    private static void cleanup(Storage storage) {
        try {
            Iterable<Blob> blobs = storage.list(CLOUD_STORAGE_BUCKET).iterateAll();
            blobs.forEach(Blob::delete);
            storage.delete(CLOUD_STORAGE_BUCKET);
        } catch (Exception ex) {
            // ignore
        }
    }

    private static void initialize(Storage storage) {
        storage.create(BucketInfo.newBuilder(CLOUD_STORAGE_BUCKET).setStorageClass(StorageClass.STANDARD)
                .setLocation(MOCK_SERVER_REGION).build());
    }
}
