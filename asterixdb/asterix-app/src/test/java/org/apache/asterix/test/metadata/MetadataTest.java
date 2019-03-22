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
package org.apache.asterix.test.metadata;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Executes the Metadata tests.
 */
@RunWith(Parameterized.class)
public class MetadataTest {

    private TestCaseContext tcCtx;

    private static final String PATH_ACTUAL = "target" + File.separator + "mdtest" + File.separator;
    private static final String PATH_BASE =
            StringUtils.join(new String[] { "src", "test", "resources", "metadata" + File.separator }, File.separator);
    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";

    private static final TestExecutor testExecutor = new TestExecutor();
    private static AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        integrationUtil.init(true, TEST_CONFIG_FILE_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        integrationUtil.deinit(true);
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
    }

    @Parameters(name = "MetadataTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE))) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    public MetadataTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }

}
