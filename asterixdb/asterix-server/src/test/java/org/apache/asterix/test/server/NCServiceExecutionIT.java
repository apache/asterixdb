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
package org.apache.asterix.test.server;

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.HDFSCluster;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.test.server.process.HyracksCCProcess;
import org.apache.hyracks.test.server.process.HyracksNCServiceProcess;
import org.apache.hyracks.test.server.process.HyracksVirtualCluster;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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
    public static final String TARGET_DIR = joinPath("..", "asterix-server", "target");

    // Directory where the NCs create and store all data, as configured by
    // src/test/resources/NCServiceExecutionIT/cc.conf.
    public static final String INSTANCE_DIR = joinPath(TARGET_DIR, "tmp");

    // The log directory, where all CC, NCService, and NC logs are written. CC and
    // NCService logs are configured on the HyracksVirtualCluster below. NC logs
    // are configured in src/test/resources/NCServiceExecutionIT/ncservice*.conf.
    public static final String LOG_DIR = joinPath(TARGET_DIR, "failsafe-reports");

    // Directory where *.conf files are located.
    public static final String CONF_DIR = joinPath(TARGET_DIR, "test-classes", "NCServiceExecutionIT");

    // The app.home specified for HyracksVirtualCluster. The NCService expects
    // to find the NC startup script in ${app.home}/bin.
    public static final File APP_HOME = new File(TARGET_DIR, "appassembler");

    // Path to the asterix-app directory. This is used as the current working
    // directory for the CC and NCService processes, which allows relative file
    // paths in "load" statements in test queries to find the right data. It is
    // also used for HDFSCluster.
    public static final File ASTERIX_APP_DIR = new File(joinPath("..", "asterix-app"));

    // Path to the actual AQL test files, which we borrow from asterix-app. This is
    // passed to TestExecutor.
    protected static final File TESTS_DIR =
            new File(ASTERIX_APP_DIR, joinPath("src", "test", "resources", "runtimets"));

    // Path that actual results are written to. We create and clean this directory
    // here, and also pass it to TestExecutor which writes the test output there.
    public static final File ACTUAL_RESULTS_DIR = new File(TARGET_DIR, "ittest");

    private static final Logger LOGGER = LogManager.getLogger();
    private static boolean startHdfs;

    @AfterClass
    public static void checkLogFiles() {
        NCServiceExecutionIT.checkLogFiles(new File(TARGET_DIR, NCServiceExecutionIT.class.getSimpleName()),
                "asterix_nc1", "asterix_nc2");
    }

    public static void checkLogFiles(File logsPath, String... ncNames) {
        LOGGER.info("verifying server log files");
        Assert.assertTrue("logs dir (" + logsPath + ") missing!", logsPath.exists());
        List<String> allFiles = new ArrayList<>();
        Stream.of(ncNames).map(NCServiceExecutionIT::addNcFiles).forEach(allFiles::addAll);
        allFiles.add("access.log");
        allFiles.add("cc.log");
        allFiles.forEach(file -> {
            Assert.assertTrue(file + " missing!", new File(logsPath, file).exists());
        });
    }

    private static List<String> addNcFiles(String ncName) {
        return Arrays.asList("access-" + ncName + ".log", "nc-" + ncName + ".log", "trace-" + ncName + ".log");
    }

    enum KillCommand {
        CC,
        NC1,
        NC2;

        @Override
        public String toString() {
            return "<kill " + name().toLowerCase() + ">";
        }
    }

    private static HyracksCCProcess cc;
    private static HyracksNCServiceProcess nc1;
    private static HyracksNCServiceProcess nc2;

    private final TestCaseContext tcCtx;
    private static final TestExecutor testExecutor = new TestExecutor();

    private static final List<String> badTestCases = new ArrayList<>();
    private static HyracksVirtualCluster cluster;
    private final KillCommand killType;
    private static boolean clusterActive = false;

    @BeforeClass
    public static void setUp() throws Exception {
        setUp(false);
    }

    public static void setUp(boolean startHdfs) throws Exception {
        NCServiceExecutionIT.startHdfs = startHdfs;
        // Create actual-results output directory.
        ACTUAL_RESULTS_DIR.mkdirs();

        // Remove any instance data from previous runs.
        File instanceDir = new File(INSTANCE_DIR);
        if (instanceDir.isDirectory()) {
            FileUtils.deleteDirectory(instanceDir);
        }

        if (startHdfs) {
            HDFSCluster.getInstance().setup(ASTERIX_APP_DIR);
        }

        cluster = new HyracksVirtualCluster(APP_HOME, ASTERIX_APP_DIR);
        nc1 = cluster.addNCService(new File(CONF_DIR, "ncservice1.conf"), null);

        nc2 = cluster.addNCService(new File(CONF_DIR, "ncservice2.conf"), null);

        // Start CC
        cc = cluster.start(new File(CONF_DIR, "cc.conf"), null);

        testExecutor.waitForClusterActive(30, TimeUnit.SECONDS);
        clusterActive = true;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        File[] files = ACTUAL_RESULTS_DIR.listFiles();
        if (files == null || files.length == 0) {
            ACTUAL_RESULTS_DIR.delete();
        }
        cluster.stop();
        if (startHdfs) {
            HDFSCluster.getInstance().cleanup();
        }
        if (!badTestCases.isEmpty()) {
            System.out.println("The following test cases left some data");
            for (String testCase : badTestCases) {
                System.out.println(testCase);
            }
        }
    }

    @Parameters(name = "NCServiceExecutionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<>();
        Random random = getRandom();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(TESTS_DIR)) {
            if (!skip(ctx)) {
                testArgs.add(new Object[] { ctx, ctx, null });
            }
            // let's kill something every 50 tests
            if (testArgs.size() % 50 == 0) {
                final KillCommand killCommand = KillCommand.values()[random.nextInt(KillCommand.values().length)];
                testArgs.add(new Object[] { killCommand, null, killCommand });
            }
        }
        return testArgs;
    }

    private static Random getRandom() {
        Random random;
        if (System.getProperty("random.seed") == null) {
            random = new Random() {
                @Override
                public synchronized void setSeed(long seed) {
                    super.setSeed(seed);
                    System.err.println("using generated seed: " + seed + "; use -Drandom.seed to use specific seed");
                }
            };
        } else {
            final long seed = Long.getLong("random.seed");
            System.err.println("using provided seed (-Drandom.seed): " + seed);
            random = new Random(seed);
        }
        return random;
    }

    private static boolean skip(TestCaseContext tcCtx) {
        // For now we skip feeds tests, external-library, and api tests.
        for (TestGroup group : tcCtx.getTestGroups()) {
            if (group.getName().startsWith("external-") || group.getName().equals("feeds")
                    || group.getName().equals("api")) {
                LOGGER.info("Skipping test: " + tcCtx.toString());
                return true;
            }
        }
        return false;
    }

    public NCServiceExecutionIT(Object description, TestCaseContext tcCtx, KillCommand killType) {
        this.tcCtx = tcCtx;
        this.killType = killType;
    }

    @Before
    public void before() {
        if (!clusterActive) {
            Assert.fail("Skipping test since cluster is not ACTIVE");
        }
    }

    @Test
    public void test() throws Exception {
        if (tcCtx != null) {
            testExecutor.executeTest(ACTUAL_RESULTS_DIR.getPath(), tcCtx, null, false);
            testExecutor.cleanup(tcCtx.toString(), badTestCases);
        } else {
            switch (killType) {
                case CC:
                    LOGGER.info("Killing CC...");
                    cc.stop(true);
                    cc.start();
                    break;

                case NC1:
                    LOGGER.info("Killing NC1...");
                    nc1.stop(); // we can't kill due to ASTERIXDB-1941
                    testExecutor.waitForClusterState("UNUSABLE", 90, TimeUnit.SECONDS); // wait for missed heartbeats...
                    nc1.start(); // this restarts the NC service
                    testExecutor.startNC("asterix_nc1");
                    break;

                case NC2:
                    LOGGER.info("Killing NC2...");
                    nc2.stop(); // we can't kill due to ASTERIXDB-1941
                    testExecutor.waitForClusterState("UNUSABLE", 90, TimeUnit.SECONDS); // wait for missed heartbeats...
                    nc2.start(); // this restarts the NC service
                    testExecutor.startNC("asterix_nc2");
                    break;

                default:
                    Assert.fail("killType: " + killType);
            }
            try {
                testExecutor.waitForClusterActive(90, TimeUnit.SECONDS);
            } catch (Exception e) {
                // stop executing the rest of the tests since the cluster is not ACTIVE
                LOGGER.log(Level.ERROR, "Cannot continue since cluster is not ACTIVE", e);
                clusterActive = false;
                Assert.fail("Cluster is not ACTIVE");
            }
        }
    }
}
