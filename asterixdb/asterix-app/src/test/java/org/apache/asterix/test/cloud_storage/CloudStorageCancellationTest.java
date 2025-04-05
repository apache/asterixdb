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

import static org.apache.asterix.test.cloud_storage.CloudStorageTest.DELTA_RESULT_PATH;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.EXCLUDED_TESTS;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.ONLY_TESTS;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.SUITE_TESTS;

import java.util.Collection;
import java.util.List;

import org.apache.asterix.api.common.LocalCloudUtilAdobeMock;
import org.apache.asterix.test.common.CancellationTestExecutor;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.Description;
import org.apache.asterix.testframework.xml.TestCase;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Run tests in cloud deployment environment
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CloudStorageCancellationTest {

    private final TestCaseContext tcCtx;

    public CloudStorageCancellationTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        TestExecutor testExecutor = new CancellationTestExecutor(DELTA_RESULT_PATH);
        CloudStorageTest.setupEnv(testExecutor);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
        LocalCloudUtilAdobeMock.shutdownSilently();
    }

    @Parameters(name = "CloudStorageCancellationTest {index}: {0}")
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
}
