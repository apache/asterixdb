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
package org.apache.asterix.installer.transaction;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DmlRecoveryIT {

    // variable to indicate whether this test will be executed

    private static final Logger LOGGER = Logger.getLogger(RecoveryIT.class.getName());
    private static final String PATH_ACTUAL = "rttest/";

    private static final String TESTSUITE_PATH_BASE = "../asterix-app/src/test/resources/runtimets/";

    private TestCaseContext tcCtx;
    private static File asterixInstallerPath;
    private static File installerTargetPath;
    private static String managixHomeDirName;
    private static String managixHomePath;
    private static String scriptHomePath;
    private static ProcessBuilder pb;
    private static Map<String, String> env;
    private final TestExecutor testExecutor = new TestExecutor();

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        asterixInstallerPath = new File(System.getProperty("user.dir"));
        installerTargetPath = new File(asterixInstallerPath, "target");
        managixHomeDirName = installerTargetPath.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }
        })[0];
        managixHomePath = new File(installerTargetPath, managixHomeDirName).getAbsolutePath();
        LOGGER.info("MANAGIX_HOME=" + managixHomePath);

        pb = new ProcessBuilder();
        env = pb.environment();
        env.put("MANAGIX_HOME", managixHomePath);
        scriptHomePath = asterixInstallerPath + File.separator + "src" + File.separator + "test" + File.separator
                + "resources" + File.separator + "transactionts" + File.separator + "scripts";
        env.put("SCRIPT_HOME", scriptHomePath);

        TestExecutor.executeScript(pb,
                scriptHomePath + File.separator + "dml_recovery" + File.separator + "configure_and_validate.sh");
        TestExecutor.executeScript(pb,
                scriptHomePath + File.separator + "dml_recovery" + File.separator + "stop_and_delete.sh");

        TestExecutor.executeScript(pb,
                scriptHomePath + File.separator + "dml_recovery" + File.separator + "create_and_start.sh");

    }

    @AfterClass
    public static void tearDown() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        FileUtils.deleteDirectory(outdir);
        TestExecutor.executeScript(pb,
                scriptHomePath + File.separator + "dml_recovery" + File.separator + "stop_and_delete.sh");
        TestExecutor.executeScript(pb,
                scriptHomePath + File.separator + "dml_recovery" + File.separator + "shutdown.sh");

    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {

        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(TESTSUITE_PATH_BASE))) {
            if (ctx.getTestCase().getFilePath().equals("dml"))
                testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    public DmlRecoveryIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, pb, true);
    }
}
