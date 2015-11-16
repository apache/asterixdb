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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class AsterixClusterLifeCycleIT {

    private static final String PATH_BASE = StringUtils
            .join(new String[] { "src", "test", "resources", "integrationts", "lifecycle" }, File.separator);
    private static final String CLUSTER_BASE = StringUtils
            .join(new String[] { "src", "test", "resources", "clusterts" }, File.separator);
    private static final String PATH_ACTUAL = "ittest" + File.separator;
    private static String managixFolderName;
    private static final Logger LOGGER = Logger.getLogger(AsterixClusterLifeCycleIT.class.getName());
    private static List<TestCaseContext> testCaseCollection;
    private static File asterixProjectDir = new File(System.getProperty("user.dir"));
    private final TestExecutor testExecutor = new TestExecutor();

    @BeforeClass
    public static void setUp() throws Exception {
        // testcase setup
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        testCaseCollection = b.build(new File(PATH_BASE));
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        // vagrant setup
        File installerTargetDir = new File(asterixProjectDir, "target");
        System.out.println(managixFolderName);
        managixFolderName = installerTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }

        })[0];
        invoke("cp", "-r", installerTargetDir.toString() + "/" + managixFolderName,
                asterixProjectDir + "/" + CLUSTER_BASE);

        logOutput(remoteInvoke("cp -r /vagrant/" + managixFolderName + " /tmp/asterix").getInputStream());

        logOutput(managixInvoke("configure").getInputStream());
        logOutput(managixInvoke("validate").getInputStream());

        Process p = managixInvoke("create -n vagrant-ssh -c /vagrant/cluster.xml");
        String pout = processOut(p);
        LOGGER.info(pout);
        Assert.assertTrue(checkOutput(pout, "ACTIVE"));
        // TODO: I should check for 'WARNING' here, but issue 764 stops this
        // from being reliable
        LOGGER.info("Test start active cluster instance PASSED");

        Process stop = managixInvoke("stop -n vagrant-ssh");
        Assert.assertTrue(checkOutput(stop.getInputStream(), "Stopped Asterix instance"));
        LOGGER.info("Test stop active cluster instance PASSED");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Process p = managixInvoke("delete -n vagrant-ssh");
        Assert.assertTrue(checkOutput(p.getInputStream(), "Deleted Asterix instance"));
        remoteInvoke("rm -rf /vagrant/managix-working");
        LOGGER.info("Test delete active instance PASSED");
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        return testArgs;
    }

    public static boolean checkOutput(InputStream input, String requiredSubString) {
        // right now im just going to look at the output, which is wholly
        // inadequate
        // TODO: try using cURL to actually poke the instance to see if it is
        // more alive
        String candidate;
        try {
            candidate = IOUtils.toString(input, StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            LOGGER.warning("Could not check output of subprocess");
            return false;
        }
        return candidate.contains(requiredSubString);
    }

    public static boolean checkOutput(String candidate, String requiredSubString) {
        return candidate.contains(requiredSubString);
    }

    public static String processOut(Process p) throws IOException {
        InputStream input = p.getInputStream();
        return IOUtils.toString(input, StandardCharsets.UTF_8.name());
    }

    public static void logOutput(InputStream input) {
        try {
            LOGGER.info(IOUtils.toString(input, StandardCharsets.UTF_8.name()));
        } catch (IOException e) {
            LOGGER.warning("Could not print output of subprocess");
        }
    }

    private static Process invoke(String... args) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        return p;
    }

    private static Process remoteInvoke(String cmd) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("vagrant", "ssh", "cc", "-c", "MANAGIX_HOME=/tmp/asterix/ " + cmd);
        File cwd = new File(asterixProjectDir.toString() + "/" + CLUSTER_BASE);
        pb.directory(cwd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        return p;
    }

    private static Process managixInvoke(String cmd) throws Exception {
        return remoteInvoke("/tmp/asterix/bin/managix " + cmd);
    }

    @Test
    public void StartStopActiveInstance() throws Exception {
        // TODO: is the instance actually live?
        // TODO: is ZK still running?
        try {
            Process start = managixInvoke("start -n vagrant-ssh");
            Assert.assertTrue(checkOutput(start.getInputStream(), "ACTIVE"));
            Process stop = managixInvoke("stop -n vagrant-ssh");
            Assert.assertTrue(checkOutput(stop.getInputStream(), "Stopped Asterix instance"));
            LOGGER.info("Test start/stop active cluster instance PASSED");
        } catch (Exception e) {
            throw new Exception("Test start/stop FAILED!", e);
        }
    }

    public void test() throws Exception {
        for (TestCaseContext testCaseCtx : testCaseCollection) {
            testExecutor.executeTest(PATH_ACTUAL, testCaseCtx, null, false);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
            new AsterixClusterLifeCycleIT().test();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.severe("TEST CASE(S) FAILED");
        } finally {
            tearDown();
        }
    }

}
