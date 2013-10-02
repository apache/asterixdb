/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.installer.transaction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

@RunWith(Parameterized.class)
public class RecoveryIT {

    private static final Logger LOGGER = Logger.getLogger(RecoveryIT.class.getName());
    private static final String PATH_ACTUAL = "rttest/";
    private static final String PATH_BASE = "src/test/resources/transactionts/";
    private TestCaseContext tcCtx;
    private static File asterixInstallerPath;
    private static File asterixAppPath;
    private static File asterixDBPath;
    private static File installerTargetPath;
    private static String managixHomeDirName;
    private static String managixHomePath;
    private static String scriptHomePath;
    private static ProcessBuilder pb;
    private static Map<String, String> env;

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        asterixInstallerPath = new File(System.getProperty("user.dir"));
        asterixDBPath = new File(asterixInstallerPath.getParent());
        asterixAppPath = new File(asterixDBPath.getAbsolutePath() + File.separator + "asterix-app");
        installerTargetPath = new File(asterixInstallerPath, "target");
        managixHomeDirName = installerTargetPath.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }
        })[0];
        managixHomePath = new File(installerTargetPath, managixHomeDirName).getAbsolutePath();

        String fileListPath = asterixInstallerPath.getAbsolutePath() + File.separator + "src" + File.separator + "test"
                + File.separator + "resources" + File.separator + "transactionts" + File.separator + "data"
                + File.separator + "file_list.txt";
        String srcBasePath = asterixAppPath.getAbsolutePath();
        String destBasePath = managixHomePath + File.separator + "clusters" + File.separator + "local" + File.separator
                + "working_dir";
        prepareDataFiles(fileListPath, srcBasePath, destBasePath);

        pb = new ProcessBuilder();
        env = pb.environment();
        env.put("MANAGIX_HOME", managixHomePath);
        scriptHomePath = asterixInstallerPath + File.separator + "src" + File.separator + "test" + File.separator
                + "resources" + File.separator + "transactionts" + File.separator + "scripts";
        env.put("SCRIPT_HOME", scriptHomePath);
        
        TestsUtils.executeScript(pb, scriptHomePath + File.separator + "setup_teardown" + File.separator
                + "configure_and_validate.sh");
        TestsUtils.executeScript(pb, scriptHomePath + File.separator + "setup_teardown" + File.separator
                + "stop_and_delete.sh");
    }

    private static void prepareDataFiles(String fileListPath, String srcBasePath, String destBasePath)
            throws IOException {
        String line;
        File srcPathFile = null;
        File destPathFile = null;
        BufferedReader br = new BufferedReader(new FileReader(fileListPath));
        while ((line = br.readLine()) != null) {
            srcPathFile = new File(srcBasePath + File.separator + line.trim());
            destPathFile = new File(destBasePath + File.separator + line.trim());
            destPathFile.getParentFile().mkdirs();
            FileUtils.copyFile(srcPathFile, destPathFile);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        FileUtils.deleteDirectory(outdir);
        TestsUtils.executeScript(pb, scriptHomePath + File.separator + "setup_teardown" + File.separator
                + "stop_and_delete.sh");
        TestsUtils.executeScript(pb, scriptHomePath + File.separator + "setup_teardown" + File.separator
                + "shutdown.sh");
    }

    @Parameters
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
        TestsUtils.executeTest(PATH_ACTUAL, tcCtx, pb);
    }
}
