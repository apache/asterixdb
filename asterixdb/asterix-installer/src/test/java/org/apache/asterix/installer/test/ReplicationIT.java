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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.test.base.RetainLogsRule;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ReplicationIT {

    private static final String PATH_BASE = "src/test/resources/integrationts/replication/";
    private static final String PATH_ACTUAL = "target" + File.separator + "ittest" + File.separator;
    private static final Logger LOGGER = Logger.getLogger(ReplicationIT.class.getName());
    private static String reportPath = new File(
            StringUtils.join(new String[] { "target", "failsafe-reports" }, File.separator)).getAbsolutePath();

    private final TestExecutor testExecutor = new TestExecutor();
    private TestCaseContext tcCtx;
    private static String scriptHomePath;
    private static File asterixInstallerPath;
    private static ProcessBuilder pb;
    private static Map<String, String> env;

    public ReplicationIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Rule
    public TestRule retainLogs = new RetainLogsRule(AsterixInstallerIntegrationUtil.getManagixHome(), reportPath);

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            pb = new ProcessBuilder();
            env = pb.environment();
            asterixInstallerPath = new File(System.getProperty("user.dir"));
            scriptHomePath = asterixInstallerPath + File.separator + "src" + File.separator + "test" + File.separator
                    + "resources" + File.separator + "integrationts" + File.separator + "replication" + File.separator
                    + "scripts";
            env.put("SCRIPT_HOME", scriptHomePath);
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    @Before
    public void before() throws Exception {
        LOGGER.info("Creating new instance...");
        AsterixInstallerIntegrationUtil.init(AsterixInstallerIntegrationUtil.LOCAL_CLUSTER_WITH_REPLICATION_PATH);
        LOGGER.info("Instacne created.");
        AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.ACTIVE);
        LOGGER.info("Instance is in ACTIVE state.");
    }

    @After
    public void after() throws Exception {
        LOGGER.info("Destroying instance...");
        AsterixInstallerIntegrationUtil.deinit();
        LOGGER.info("Instance destroyed.");
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, pb, false);
    }

    @Parameterized.Parameters(name = "ReplicationIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = buildTestsInXml(TestCaseContext.DEFAULT_TESTSUITE_XML_NAME);
        if (testArgs.size() == 0) {
            testArgs = buildTestsInXml(TestCaseContext.DEFAULT_TESTSUITE_XML_NAME);
        }
        return testArgs;
    }

    protected static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }
}
