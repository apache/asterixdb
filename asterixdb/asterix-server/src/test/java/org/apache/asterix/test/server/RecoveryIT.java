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
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.test.base.RetainLogsRule;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.HDFSCluster;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class RecoveryIT {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final String PATH_ACTUAL = joinPath("target", "rttest");
    private static final String PATH_BASE = "src/test/resources/transactionts/";
    private static final File HDFS_BASE = new File("..", "asterix-app");
    private TestCaseContext tcCtx;
    private static File asterixInstallerPath;
    private static File installerTargetPath;
    private static String ncServiceHomeDirName;
    private static String ncServiceHomePath;
    private static String ncServiceSubDirName;
    private static String ncServiceSubPath;
    private static String scriptHomePath;
    private static String reportPath;
    private static ProcessBuilder pb;
    private static Map<String, String> env;
    private final TestExecutor testExecutor = new TestExecutor();

    @Rule
    public TestRule retainLogs = new RetainLogsRule(ncServiceHomePath, reportPath, this);

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        File externalTestsJar = Objects.requireNonNull(new File(joinPath("..", "asterix-external-data", "target"))
                .listFiles((dir, name) -> name.matches("asterix-external-data-.*-tests.jar")))[0];

        asterixInstallerPath = new File(System.getProperty("user.dir"));
        installerTargetPath = new File(new File(asterixInstallerPath.getParentFile(), "asterix-server"), "target");
        reportPath = new File(installerTargetPath, "failsafe-reports").getAbsolutePath();
        ncServiceSubDirName = Objects.requireNonNull(
                installerTargetPath.list((dir, name) -> name.matches("asterix-server.*binary-assembly")))[0];
        ncServiceSubPath = new File(installerTargetPath, ncServiceSubDirName).getAbsolutePath();
        ncServiceHomeDirName = Objects.requireNonNull(
                new File(ncServiceSubPath).list(((dir, name) -> name.matches("apache-asterixdb.*"))))[0];
        ncServiceHomePath = new File(ncServiceSubPath, ncServiceHomeDirName).getAbsolutePath();

        LOGGER.info("NCSERVICE_HOME=" + ncServiceHomePath);

        FileUtils.copyFile(externalTestsJar, new File(ncServiceHomePath + "/repo", externalTestsJar.getName()));

        pb = new ProcessBuilder();
        env = pb.environment();
        env.put("NCSERVICE_HOME", ncServiceHomePath);
        env.put("JAVA_HOME", System.getProperty("java.home"));
        scriptHomePath =
                joinPath(asterixInstallerPath.getPath(), "src", "test", "resources", "transactionts", "scripts");
        env.put("SCRIPT_HOME", scriptHomePath);

        TestExecutor.executeScript(pb, joinPath(scriptHomePath, "setup_teardown", "configure_and_validate.sh"));
        TestExecutor.executeScript(pb, joinPath(scriptHomePath, "setup_teardown", "stop_and_delete.sh"));
        HDFSCluster.getInstance().setup(HDFS_BASE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        FileUtils.deleteDirectory(outdir);
        File dataCopyDir = new File(joinPath(ncServiceHomePath, "..", "..", "data"));
        FileUtils.deleteDirectory(dataCopyDir);
        TestExecutor.executeScript(pb, joinPath(scriptHomePath, "setup_teardown", "stop_and_delete.sh"));
        HDFSCluster.getInstance().cleanup();
    }

    @Parameters(name = "RecoveryIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE))) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    public RecoveryIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, pb, false);
    }

}
