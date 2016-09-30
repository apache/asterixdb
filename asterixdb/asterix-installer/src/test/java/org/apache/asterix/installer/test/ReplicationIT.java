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
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.plexus.util.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ReplicationIT {

    private static final String PATH_BASE = StringUtils
            .join(new String[] { "src", "test", "resources", "integrationts", "replication" }, File.separator);
    private static final String CLUSTER_BASE = StringUtils
            .join(new String[] { "src", "test", "resources", "clusterts" }, File.separator);
    private static final String PATH_ACTUAL = "target" + File.separator + "repliationtest" + File.separator;
    private static String managixFolderName;
    private static final Logger LOGGER = Logger.getLogger(ReplicationIT.class.getName());
    private static File asterixProjectDir = new File(System.getProperty("user.dir"));
    private static final String CLUSTER_CC_ADDRESS = "10.10.0.2";
    private static final int CLUSTER_CC_API_PORT = 19002;
    private static ProcessBuilder pb;
    private static Map<String, String> env;
    private final static TestExecutor testExecutor = new TestExecutor(CLUSTER_CC_ADDRESS, CLUSTER_CC_API_PORT);
    private static String SCRIPT_HOME = "/vagrant/scripts/";
    private static String MANAGIX_HOME = "/tmp/asterix/bin/managix ";
    private static final String INSTANCE_NAME = "asterix";
    protected TestCaseContext tcCtx;

    public ReplicationIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        // vagrant setup
        File installerTargetDir = new File(asterixProjectDir, "target");
        String[] installerFiles = installerTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }
        });

        if (installerFiles == null || installerFiles.length == 0) {
            throw new Exception("Couldn't find installer binaries");
        }

        managixFolderName = installerFiles[0];

        //copy tests data
        FileUtils.copyDirectoryStructure(
                new File(StringUtils.join(
                        new String[] { "..", "asterix-replication", "src", "test", "resources", "data" },
                        File.separator)),
                new File(StringUtils.join(new String[] { "src", "test", "resources", "clusterts", "data" },
                        File.separator)));

        //copy tests scripts
        FileUtils.copyDirectoryStructure(
                new File(StringUtils.join(
                        new String[] { "..", "asterix-replication", "src", "test", "resources", "scripts" },
                        File.separator)),
                new File(StringUtils.join(new String[] { "src", "test", "resources", "clusterts", "scripts" },
                        File.separator)));

        invoke("cp", "-r", installerTargetDir.toString() + "/" + managixFolderName,
                asterixProjectDir + "/" + CLUSTER_BASE);

        remoteInvoke("cp -r /vagrant/" + managixFolderName + " /tmp/asterix");

        pb = new ProcessBuilder();
        env = pb.environment();
        env.put("SCRIPT_HOME", SCRIPT_HOME);
        env.put("MANAGIX_HOME", MANAGIX_HOME);
        File cwd = new File(asterixProjectDir.toString() + "/" + CLUSTER_BASE);
        pb.directory(cwd);
        pb.redirectErrorStream(true);

        //make scripts executable
        String chmodScriptsCmd = "chmod -R +x " + SCRIPT_HOME;
        remoteInvoke(chmodScriptsCmd, "cc");
        remoteInvoke(chmodScriptsCmd, "nc1");
        remoteInvoke(chmodScriptsCmd, "nc2");

        //managix configure
        logOutput(managixInvoke("configure").getInputStream());

        //managix validate
        String validateOutput = IOUtils.toString(managixInvoke("validate").getInputStream(),
                StandardCharsets.UTF_8.name());
        if (validateOutput.contains("ERROR")) {
            throw new Exception("Managix validate error: " + validateOutput);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        //remove files
        remoteInvoke("rm -rf /vagrant/asterix");
    }

    @Before
    public void beforeTest() throws Exception {
        //create instance
        managixInvoke("create -n " + INSTANCE_NAME + " -c /vagrant/cluster_with_replication.xml").getInputStream();
    }

    @After
    public void afterTest() throws Exception {
        //stop instance
        managixInvoke("stop -n " + INSTANCE_NAME);

        //verify that all processes have been stopped
        String killProcesses = "kill_cc_and_nc.sh";
        executeVagrantScript("cc", killProcesses);
        executeVagrantScript("nc1", killProcesses);
        executeVagrantScript("nc2", killProcesses);

        //delete storage
        String deleteStorage = "delete_storage.sh";
        executeVagrantScript("cc", deleteStorage);
        executeVagrantScript("nc1", deleteStorage);
        executeVagrantScript("nc2", deleteStorage);

        //delete instance
        managixInvoke("delete -n " + INSTANCE_NAME);
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, pb, false);
    }

    @Parameters(name = "ReplicationIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = buildTestsInXml(TestCaseContext.DEFAULT_TESTSUITE_XML_NAME);
        if (testArgs.size() == 0) {
            testArgs = buildTestsInXml(TestCaseContext.DEFAULT_TESTSUITE_XML_NAME);
        }
        return testArgs;
    }

    protected static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    public static boolean checkOutput(InputStream input, String requiredSubString) {
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
        p.waitFor();
        return p;
    }

    private static Process remoteInvoke(String cmd, String node) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("vagrant", "ssh", node, "--", cmd);
        File cwd = new File(asterixProjectDir.toString() + "/" + CLUSTER_BASE);
        pb.directory(cwd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        p.waitFor();
        return p;
    }

    private static Process managixInvoke(String cmd) throws Exception {
        return remoteInvoke(MANAGIX_HOME + cmd);
    }

    private static String executeVagrantScript(String node, String scriptName) throws Exception {
        pb.command("vagrant", "ssh", node, "--", SCRIPT_HOME + scriptName);
        Process p = pb.start();
        p.waitFor();
        InputStream input = p.getInputStream();
        return IOUtils.toString(input, StandardCharsets.UTF_8.name());
    }
}
