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
package org.apache.asterix.test.podman;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.DockerClient;

/**
 * Runs the Python UDF tests within a container using domain sockets.
 */
@RunWith(Parameterized.class)
public class PodmanPythonFunctionIT {
    public static final DockerImageName ASTERIX_IMAGE = DockerImageName.parse("asterixdb/socktest");
    @ClassRule
    public static GenericContainer<?> asterix = new GenericContainer(ASTERIX_IMAGE).withExposedPorts(19004, 5006, 19002)
            .withFileSystemBind("../asterix-app/", "/var/tmp/asterix-app/", BindMode.READ_WRITE);
    protected static final String TEST_CONFIG_FILE_NAME = "../asterix-app/src/test/resources/cc.conf";
    private static final boolean cleanupOnStop = true;

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new TestExecutor(
                List.of(InetSocketAddress.createUnresolved(asterix.getHost(), asterix.getMappedPort(19002))));
        asterix.execInContainer("/opt/setup.sh");
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor, false, true, new PodmanUDFLibrarian(asterix));
        setEndpoints(testExecutor);
        testExecutor.waitForClusterActive(60, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
        } finally {
            ExecutionTestUtil.tearDown(cleanupOnStop);
            DockerClient dc = DockerClientFactory.instance().client();
            dc.removeImageCmd(ASTERIX_IMAGE.asCanonicalNameString()).withForce(true).exec();
        }
    }

    @Parameters(name = "PodmanPythonFunctionIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_sqlpp.xml", "testsuite_it_python.xml",
                "../asterix-app/src/test/resources/runtimets");
    }

    protected TestCaseContext tcCtx;

    public PodmanPythonFunctionIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }

    private static void setEndpoints(TestExecutor testExecutor) {
        final Map<String, InetSocketAddress> ncEndPoints = new HashMap<>();
        final String ip = asterix.getHost();
        final String nodeId = "asterix_nc";
        int apiPort = asterix.getMappedPort(19004);
        ncEndPoints.put(nodeId, InetSocketAddress.createUnresolved(ip, apiPort));
        testExecutor.setNcEndPoints(ncEndPoints);
    }
}
