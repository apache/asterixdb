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

import static org.apache.asterix.test.server.NCServiceExecutionIT.*;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.test.base.RetainLogsRule;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.test.server.process.HyracksVirtualCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ReplicationIT {

    private static final String PATH_BASE = joinPath("src", "test", "resources", "integrationts", "replication");
    private static final String CONF_DIR = joinPath(TARGET_DIR, "test-classes", "ReplicationIT");
    private static final String PATH_ACTUAL = joinPath("target", "ittest");
    private static final Logger LOGGER = LogManager.getLogger();
    private static File reportPath = new File("target", "failsafe-reports");
    private static final TestExecutor testExecutor = new TestExecutor();
    private static HyracksVirtualCluster cluster;

    static {
        final Map<String, InetSocketAddress> ncEndPoints = new HashMap<>();
        final Map<String, InetSocketAddress> replicationAddress = new HashMap<>();
        final String ip = InetAddress.getLoopbackAddress().getHostAddress();
        ncEndPoints.put("asterix_nc1", InetSocketAddress.createUnresolved(ip, 19004));
        ncEndPoints.put("asterix_nc2", InetSocketAddress.createUnresolved(ip, 19005));
        replicationAddress.put("asterix_nc1", InetSocketAddress.createUnresolved(ip, 2001));
        replicationAddress.put("asterix_nc2", InetSocketAddress.createUnresolved(ip, 2002));
        testExecutor.setNcEndPoints(ncEndPoints);
        testExecutor.setNcReplicationAddress(replicationAddress);
    }

    private TestCaseContext tcCtx;

    public ReplicationIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Rule
    public TestRule retainLogs = new RetainLogsRule(ASTERIX_APP_DIR, reportPath, this);

    @Before
    public void before() throws Exception {
        LOGGER.info("Creating new instance...");
        File instanceDir = new File(INSTANCE_DIR);
        if (instanceDir.isDirectory()) {
            FileUtils.deleteDirectory(instanceDir);
        }

        cluster = new HyracksVirtualCluster(APP_HOME, ASTERIX_APP_DIR);
        cluster.addNCService(new File(CONF_DIR, "ncservice1.conf"), null);
        cluster.addNCService(new File(CONF_DIR, "ncservice2.conf"), null);

        // Start CC
        cluster.start(new File(CONF_DIR, "cc.conf"), null);
        LOGGER.info("Instance created.");
        testExecutor.waitForClusterActive(30, TimeUnit.SECONDS);
        LOGGER.info("Instance is in ACTIVE state.");
    }

    @After
    public void after() {
        LOGGER.info("Destroying instance...");
        cluster.stop();
        LOGGER.info("Instance destroyed.");
    }

    @AfterClass
    public static void checkLogFiles() {
        NCServiceExecutionIT.checkLogFiles(new File(TARGET_DIR, ReplicationIT.class.getSimpleName()), "asterix_nc1",
                "asterix_nc2");
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }

    @Parameterized.Parameters(name = "ReplicationIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = buildTestsInXml(TestCaseContext.DEFAULT_TESTSUITE_XML_NAME);
        if (testArgs.size() == 0) {
            testArgs = buildTestsInXml(TestCaseContext.DEFAULT_TESTSUITE_XML_NAME);
        }
        return testArgs;
    }

    private static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }
}
