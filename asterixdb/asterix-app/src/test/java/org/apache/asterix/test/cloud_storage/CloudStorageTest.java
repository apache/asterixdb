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

import java.util.Collection;

import org.apache.asterix.api.common.CloudUtils;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.Before;
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
public class CloudStorageTest {

    protected TestCaseContext tcCtx;
    private static final TestExecutor testExecutor = new TestExecutor();
    private static final String SUITE_TESTS = "testsuite_cloud_storage.xml";
    private static final String ONLY_TESTS = "testsuite_cloud_storage_only.xml";
    private static final String CONFIG_FILE_NAME = "src/main/resources/cc-cloud-storage.conf";

    public CloudStorageTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Before
    public void setUp() throws Exception {
        CloudUtils.startS3CloudEnvironment();
        LangExecutionUtil.setUp(CONFIG_FILE_NAME, testExecutor);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, CONFIG_FILE_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "CloudStorageTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests(ONLY_TESTS, SUITE_TESTS);
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }
}
