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
package org.apache.asterix.installer.test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.test.base.RetainLogsRule;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

//This is just a simple derivative of all of the other IT test cases specifically for test cases which revolve around
//some behavior where stopping and starting Asterix causes issues
public class AsterixRestartIT {

    private static final String PATH_BASE = "src/test/resources/integrationts/restart/";
    private static final String PATH_ACTUAL = "target" + File.separator + "ittest" + File.separator;
    private static final Logger LOGGER = Logger.getLogger(AsterixRestartIT.class.getName());
    private static List<TestCaseContext> testCaseCollection;
    private static String reportPath = new File(
            StringUtils.join(new String[] { "target", "failsafe-reports" }, File.separator)).getAbsolutePath();

    private final TestExecutor testExecutor = new TestExecutor();
    private static String scriptHomePath;
    private static File asterixInstallerPath;
    private static ProcessBuilder pb;
    private static Map<String, String> env;

    @Rule
    public TestRule retainLogs = new RetainLogsRule(AsterixInstallerIntegrationUtil.getManagixHome(), reportPath);

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            pb = new ProcessBuilder();
            env = pb.environment();
            asterixInstallerPath = new File(System.getProperty("user.dir"));
            scriptHomePath = asterixInstallerPath + File.separator + "src" + File.separator + "test" + File.separator
                    + "resources" + File.separator + "integrationts" + File.separator + "restart"
                    + File.separator + "scripts";
            env.put("SCRIPT_HOME", scriptHomePath);
            AsterixInstallerIntegrationUtil.init();
            AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.ACTIVE);
            TestCaseContext.Builder b = new TestCaseContext.Builder();
            testCaseCollection = b.build(new File(PATH_BASE));
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixInstallerIntegrationUtil.deinit();
    }

    @Test
    public void test() throws Exception {
        for (TestCaseContext testCaseCtx : testCaseCollection) {
            testExecutor.executeTest(PATH_ACTUAL, testCaseCtx, pb, false);
        }

    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
            new AsterixRestartIT().test();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("TEST CASES FAILED");
        } finally {
            tearDown();
        }
    }

}
