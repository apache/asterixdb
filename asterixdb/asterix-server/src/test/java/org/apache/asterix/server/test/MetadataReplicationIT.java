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
package org.apache.asterix.server.test;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.asterix.test.base.RetainLogsRule;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.server.process.HyracksCCProcess;
import org.apache.hyracks.server.process.HyracksNCServiceProcess;
import org.apache.hyracks.server.process.HyracksVirtualCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MetadataReplicationIT {

    // Important paths and files for this test.

    // The "target" subdirectory of asterix-server. All outputs go here.
    public static final String TARGET_DIR = StringUtils.join(new String[] { "../asterix-server/target" },
            File.separator);

    // Directory where the NCs create and store all data, as configured by
    // src/test/resources/NCServiceExecutionIT/cc.conf.
    public static final String INSTANCE_DIR = StringUtils.join(new String[] { TARGET_DIR, "tmp" }, File.separator);

    // The log directory, where all CC, NCService, and NC logs are written. CC and
    // NCService logs are configured on the HyracksVirtualCluster below. NC logs
    // are configured in src/test/resources/NCServiceExecutionIT/ncservice*.conf.
    public static final String LOG_DIR = StringUtils.join(new String[] { TARGET_DIR, "failsafe-reports" },
            File.separator);

    // Directory where *.conf files are located.
    public static final String CONF_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "test-classes", "MetadataReplicationIT" }, File.separator);

    // The app.home specified for HyracksVirtualCluster. The NCService expects
    // to find the NC startup script in ${app.home}/bin.
    public static final String APP_HOME = StringUtils.join(new String[] { TARGET_DIR, "appassembler" }, File.separator);

    // Path to the asterix-app directory. This is used as the current working
    // directory for the CC and NCService processes, which allows relative file
    // paths in "load" statements in test queries to find the right data. It is
    // also used for HDFSCluster.
    public static final String ASTERIX_APP_DIR = StringUtils.join(new String[] { "..", "asterix-app" }, File.separator);

    // Path to the actual AQL test files, which we borrow from asterix-app. This is
    // passed to TestExecutor.
    protected static final String TESTS_DIR = StringUtils
            .join(new String[] { ASTERIX_APP_DIR, "src", "test", "resources", "runtimets" }, File.separator);

    // Path that actual results are written to. We create and clean this directory
    // here, and also pass it to TestExecutor which writes the test output there.
    public static final String ACTUAL_RESULTS_DIR = StringUtils.join(new String[] { TARGET_DIR, "ittest" },
            File.separator);
    private static final String PATH_BASE = Paths
            .get("src", "test", "resources", "integrationts", "metadata_only_replication").toString() + File.separator;
    private static final String PATH_ACTUAL = "target" + File.separator + "ittest" + File.separator;

    private static final Logger LOGGER = Logger.getLogger(MetadataReplicationIT.class.getName());
    private static String reportPath = new File(
            StringUtils.join(new String[] { "target", "failsafe-reports" }, File.separator)).getAbsolutePath();

    private final TestExecutor testExecutor = new TestExecutor();
    private TestCaseContext tcCtx;
    private static String scriptHomePath;
    private static File asterixInstallerPath;
    private static ProcessBuilder pb;
    private static Map<String, String> env;

    private static HyracksCCProcess cc;
    private static HyracksNCServiceProcess nc1;
    private static HyracksNCServiceProcess nc2;
    private static HyracksVirtualCluster cluster;

    public MetadataReplicationIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Rule
    public TestRule retainLogs = new RetainLogsRule(NCServiceExecutionIT.ASTERIX_APP_DIR, reportPath, this);

    @Before
    public void before() throws Exception {
        LOGGER.info("Creating new instance...");
        File instanceDir = new File(INSTANCE_DIR);
        if (instanceDir.isDirectory()) {
            FileUtils.deleteDirectory(instanceDir);
        }

        // HDFSCluster requires the input directory to end with a file separator.

        cluster = new HyracksVirtualCluster(new File(APP_HOME), new File(ASTERIX_APP_DIR));
        nc1 = cluster.addNCService(new File(CONF_DIR, "ncservice1.conf"), new File(LOG_DIR, "ncservice1.log"));

        nc2 = cluster.addNCService(new File(CONF_DIR, "ncservice2.conf"), new File(LOG_DIR, "ncservice2.log"));

        // Start CC
        cc = cluster.start(new File(CONF_DIR, "cc.conf"), new File(LOG_DIR, "cc.log"));

        LOGGER.info("Instance created.");
        testExecutor.waitForClusterActive(30, TimeUnit.SECONDS);
        LOGGER.info("Instance is in ACTIVE state.");

    }

    @After
    public void after() throws Exception {
        LOGGER.info("Destroying instance...");
        cluster.stop();
        LOGGER.info("Instance destroyed.");
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }

    @Parameterized.Parameters(name = "MetadataReplicationIT {index}: {0}")
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
