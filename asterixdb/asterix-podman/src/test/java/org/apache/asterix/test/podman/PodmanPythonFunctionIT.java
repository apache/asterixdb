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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
 * Runs the Python UDF tests across two containers: one running AsterixDB, and a separate, fenced
 * one running only the s6-ipcserver-wrapped UDF interpreter. The two only ever share a UDF domain
 * socket and the installed-library directory, both bind-mounted from the host - the sandbox
 * container never sees AsterixDB's storage, transaction log, or credentials, and AsterixDB never
 * runs a line of the UDF code itself.
 */
@RunWith(Parameterized.class)
public class PodmanPythonFunctionIT {
    public static final DockerImageName ASTERIX_IMAGE = DockerImageName.parse("asterixdb/socktest");
    public static final DockerImageName SANDBOX_IMAGE = DockerImageName.parse("asterixdb/udf-sandbox");

    // host directories bind-mounted into both containers: only the UDF socket and the installed
    // library code cross the boundary between them
    private static final Path SHARED_SOCK_DIR = Path.of("target/podman-shared/sock");
    private static final Path SHARED_APPS_DIR = Path.of("target/podman-shared/applications");

    public static GenericContainer<?> sandbox;
    public static GenericContainer<?> asterix;

    protected static final String TEST_CONFIG_FILE_NAME = "../asterix-app/src/test/resources/cc.conf";
    private static final boolean cleanupOnStop = true;

    @BeforeClass
    public static void setUp() throws Exception {
        Files.createDirectories(SHARED_SOCK_DIR);
        Files.createDirectories(SHARED_APPS_DIR);

        sandbox = new GenericContainer(SANDBOX_IMAGE)
                .withFileSystemBind(SHARED_SOCK_DIR.toString(), "/mnt/udfsock", BindMode.READ_WRITE)
                .withFileSystemBind(SHARED_APPS_DIR.toString(), "/opt/apache-asterixdb/data/applications",
                        BindMode.READ_WRITE)
                .withFileSystemBind("../asterix-app/", "/var/tmp/asterix-app/", BindMode.READ_WRITE);
        sandbox.start();

        asterix = new GenericContainer(ASTERIX_IMAGE).withExposedPorts(19004, 5006, 19002)
                .withStartupTimeout(Duration.ofMinutes(2))
                .withFileSystemBind(SHARED_SOCK_DIR.toString(), "/mnt/udfsock", BindMode.READ_WRITE)
                .withFileSystemBind(SHARED_APPS_DIR.toString(), "/opt/apache-asterixdb/data/applications",
                        BindMode.READ_WRITE)
                .withFileSystemBind("../asterix-app/", "/var/tmp/asterix-app/", BindMode.READ_WRITE);
        asterix.start();

        final TestExecutor testExecutor = new TestExecutor(
                List.of(InetSocketAddress.createUnresolved(asterix.getHost(), asterix.getMappedPort(19002))));
        sandbox.execInContainer("/opt/setup-python.sh");
        asterix.execInContainer("/opt/setup-data.sh");
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor, false, true, new PodmanUDFLibrarian(asterix));
        setEndpoints(testExecutor);
        testExecutor.waitForClusterActive(60, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
        } finally {
            ExecutionTestUtil.tearDown(cleanupOnStop);
            if (asterix != null) {
                asterix.stop();
            }
            if (sandbox != null) {
                sandbox.stop();
            }
            DockerClient dc = DockerClientFactory.instance().client();
            dc.removeImageCmd(ASTERIX_IMAGE.asCanonicalNameString()).withForce(true).exec();
            dc.removeImageCmd(SANDBOX_IMAGE.asCanonicalNameString()).withForce(true).exec();
        }
    }

    @Parameters(name = "PodmanPythonFunctionIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_sqlpp.xml", "testsuite_it_python_fenced.xml",
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
