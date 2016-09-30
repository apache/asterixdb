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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.test.runtime.HDFSCluster;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.server.process.HyracksVirtualCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class NCServiceExecutionIT {

    // Important paths and files for this test.

    // The "target" subdirectory of asterix-server. All outputs go here.
    private static final String TARGET_DIR = StringUtils
            .join(new String[] { "target" }, File.separator);

    // Directory where the NCs create and store all data, as configured by
    // src/test/resources/NCServiceExecutionIT/cc.conf.
    private static final String INSTANCE_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "tmp" }, File.separator);

    // The log directory, where all CC, NCService, and NC logs are written. CC and
    // NCService logs are configured on the HyracksVirtualCluster below. NC logs
    // are configured in src/test/resources/NCServiceExecutionIT/ncservice*.conf.
    private static final String LOG_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "failsafe-reports" }, File.separator);

    // Directory where *.conf files are located.
    private static final String CONF_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "test-classes", "NCServiceExecutionIT" },
                    File.separator);

    // The app.home specified for HyracksVirtualCluster. The NCService expects
    // to find the NC startup script in ${app.home}/bin.
    private static final String APP_HOME = StringUtils
            .join(new String[] { TARGET_DIR, "appassembler" }, File.separator);

    // Path to the asterix-app directory. This is used as the current working
    // directory for the CC and NCService processes, which allows relative file
    // paths in "load" statements in test queries to find the right data. It is
    // also used for HDFSCluster.
    private static final String ASTERIX_APP_DIR = StringUtils
            .join(new String[] { "..", "asterix-app" },
                    File.separator);

    // Path to the actual AQL test files, which we borrow from asterix-app. This is
    // passed to TestExecutor.
    protected static final String TESTS_DIR = StringUtils
            .join(new String[] { ASTERIX_APP_DIR, "src", "test", "resources", "runtimets" },
                    File.separator);

    // Path that actual results are written to. We create and clean this directory
    // here, and also pass it to TestExecutor which writes the test output there.
    private static final String ACTUAL_RESULTS_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "ittest" }, File.separator);

    private static final Logger LOGGER = Logger.getLogger(NCServiceExecutionIT.class.getName());

    private final TestCaseContext tcCtx;
    private static final TestExecutor testExecutor = new TestExecutor();

    private static final List<String> badTestCases = new ArrayList<>();
    private static HyracksVirtualCluster cluster;

    @BeforeClass
    public static void setUp() throws Exception {
        // Create actual-results output directory.
        File outDir = new File(ACTUAL_RESULTS_DIR);
        outDir.mkdirs();

        // Remove any instance data from previous runs.
        File instanceDir = new File(INSTANCE_DIR);
        if (instanceDir.isDirectory()) {
            FileUtils.deleteDirectory(instanceDir);
        }

        // HDFSCluster requires the input directory to end with a file separator.
        HDFSCluster.getInstance().setup(ASTERIX_APP_DIR + File.separator);

        cluster = new HyracksVirtualCluster(new File(APP_HOME), new File(ASTERIX_APP_DIR));
        cluster.addNCService(
                new File(CONF_DIR, "ncservice1.conf"),
                new File(LOG_DIR, "ncservice1.log"));
        cluster.addNCService(
                new File(CONF_DIR, "ncservice2.conf"),
                new File(LOG_DIR, "ncservice2.log"));

        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        // Start CC
        cluster.start(
                new File(CONF_DIR, "cc.conf"),
                new File(LOG_DIR, "cc.log"));

        LOGGER.info("Sleeping while cluster comes online...");
        try {
            Thread.sleep(6000);
        } catch (InterruptedException ignored) {
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        File outdir = new File(ACTUAL_RESULTS_DIR);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
        cluster.stop();
        HDFSCluster.getInstance().cleanup();
        if (!badTestCases.isEmpty()) {
            System.out.println("The following test cases left some data");
            for (String testCase : badTestCases) {
                System.out.println(testCase);
            }
        }
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(TESTS_DIR))) {
            if (!skip(ctx)) {
                testArgs.add(new Object[] { ctx });
            }
        }
        return testArgs;
    }

    private static boolean skip(TestCaseContext tcCtx) {
        // For now we skip feeds tests, external-library, and api tests.
        for (TestGroup group : tcCtx.getTestGroups()) {
            if (group.getName().startsWith("external-")
                    || group.getName().equals("feeds")
                    || group.getName().equals("api")) {
                LOGGER.info("Skipping test: " + tcCtx.toString());
                return true;
            }
        }
        return false;
    }

    public NCServiceExecutionIT(TestCaseContext ctx) {
        this.tcCtx = ctx;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(ACTUAL_RESULTS_DIR, tcCtx, null, false);
        testExecutor.cleanup(tcCtx.toString(), badTestCases);
    }
}
