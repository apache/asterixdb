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
package org.apache.asterix.test.runtime;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SslReplicationExecutionTest {
    protected static final String TEST_CONFIG_FILE_NAME = "src/test/resources/cc-rep-ssl.conf";
    private static final TestExecutor testExecutor = new TestExecutor();
    private static boolean configured = false;

    @BeforeClass
    public static void setUp() {
        LangExecutionUtil.setCheckStorageDistribution(false);
    }

    @Before
    public void before() throws Exception {
        TestUtils.redirectLoggingToConsole();
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor);
        if (!configured) {
            final NodeControllerService[] ncs = ExecutionTestUtil.integrationUtil.ncs;
            Map<String, InetSocketAddress> ncEndPoints = new HashMap<>();
            Map<String, InetSocketAddress> replicationAddress = new HashMap<>();
            final String ip = InetAddress.getLoopbackAddress().getHostAddress();
            for (NodeControllerService nc : ncs) {
                final String nodeId = nc.getId();
                final INcApplicationContext appCtx = (INcApplicationContext) nc.getApplicationContext();
                int apiPort = appCtx.getExternalProperties().getNcApiPort();
                int replicationPort =
                        (int) appCtx.getServiceContext().getAppConfig().get(NCConfig.Option.REPLICATION_LISTEN_PORT);
                ncEndPoints.put(nodeId, InetSocketAddress.createUnresolved(ip, apiPort));
                replicationAddress.put(nodeId, InetSocketAddress.createUnresolved(ip, replicationPort));
            }
            testExecutor.setNcEndPoints(ncEndPoints);
            testExecutor.setNcReplicationAddress(replicationAddress);
            configured = true;
        }
    }

    @After
    public void after() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "SslReplicationExecutionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("replication.xml", "replication.xml");
    }

    protected TestCaseContext tcCtx;

    public SslReplicationExecutionTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }
}
