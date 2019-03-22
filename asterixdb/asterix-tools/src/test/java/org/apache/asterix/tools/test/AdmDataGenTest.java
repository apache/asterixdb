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
package org.apache.asterix.tools.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.asterix.test.base.AsterixTestHelper;
import org.apache.asterix.tools.datagen.AdmDataGen;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AdmDataGenTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String SEPARATOR = File.separator;
    private static final String EXTENSION_QUERY = "adg";
    private static final String FILENAME_IGNORE = "ignore.txt";
    private static final String FILENAME_ONLY = "only.txt";
    private static final String PATH_BASE =
            "src" + SEPARATOR + "test" + SEPARATOR + "resources" + SEPARATOR + "adgts" + SEPARATOR;
    private static final String PATH_QUERIES = PATH_BASE + "dgscripts" + SEPARATOR;
    private static final String PATH_EXPECTED = PATH_BASE + "results" + SEPARATOR;
    private static final String PATH_ACTUAL = "adgtest" + SEPARATOR;

    private static final ArrayList<String> ignore = AsterixTestHelper.readTestListFile(FILENAME_IGNORE, PATH_BASE);
    private static final ArrayList<String> only = AsterixTestHelper.readTestListFile(FILENAME_ONLY, PATH_BASE);

    @BeforeClass
    public static void setUp() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        if (outdir.exists()) {
            AsterixTestHelper.deleteRec(outdir);
        }
        outdir.mkdirs();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // _bootstrap.stop();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
    }

    private static void suiteBuild(File dir, Collection<Object[]> testArgs, String path) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory() && !file.getName().startsWith(".")) {
                suiteBuild(file, testArgs, path + file.getName() + SEPARATOR);
            }
            if (file.isFile() && file.getName().endsWith(EXTENSION_QUERY)
            // && !ignore.contains(path + file.getName())
            ) {
                File expectedDir = new File(PATH_EXPECTED + path);
                File actualDir = new File(PATH_ACTUAL + SEPARATOR + path);
                testArgs.add(new Object[] { file, expectedDir, actualDir });
            }
        }
    }

    @Parameters
    public static Collection<Object[]> tests() {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        suiteBuild(new File(PATH_QUERIES), testArgs, "");
        return testArgs;
    }

    private File actualDir;
    private File expectedDir;
    private File scriptFile;

    public AdmDataGenTest(File scriptFile, File expectedDir, File actualDir) {
        this.scriptFile = scriptFile;
        this.expectedDir = expectedDir;
        this.actualDir = actualDir;
    }

    @Test
    public void test() throws Exception {
        String scriptFileShort =
                scriptFile.getPath().substring(PATH_QUERIES.length()).replace(SEPARATOR.charAt(0), '/');
        if (!only.isEmpty()) {
            if (!only.contains(scriptFileShort)) {
                LOGGER.info(
                        "SKIP TEST: \"" + scriptFile.getPath() + "\" \"only.txt\" not empty and not in \"only.txt\".");
            }
            Assume.assumeTrue(only.contains(scriptFileShort));
        }
        if (ignore.contains(scriptFileShort)) {
            LOGGER.info("SKIP TEST: \"" + scriptFile.getPath() + "\" in \"ignore.txt\".");
        }
        Assume.assumeTrue(!ignore.contains(scriptFileShort));

        LOGGER.info("RUN TEST: \"" + scriptFile.getPath() + "\"");

        actualDir.mkdirs();
        AdmDataGen adg = new AdmDataGen(scriptFile, actualDir);
        try {
            adg.init();
            adg.dataGen();
        } catch (Exception e) {
            throw new Exception("Data gen. ERROR for " + scriptFile + ": " + e.getMessage(), e);
        }

        if (!expectedDir.isDirectory()) {
            throw new Exception(expectedDir + " is not a directory.");
        }
        if (!actualDir.isDirectory()) {
            throw new Exception(expectedDir + " is not a directory.");
        }

        File[] expectedFileSet = expectedDir.listFiles(AdmFileFilter.INSTANCE);
        File[] actualFileSet = actualDir.listFiles(AdmFileFilter.INSTANCE);

        if (expectedFileSet.length != actualFileSet.length) {
            throw new Exception("Expecting " + expectedFileSet.length + " files and found " + actualFileSet.length
                    + " files instead.");
        }

        for (File expectedFile : expectedFileSet) {
            if (expectedFile.isHidden()) {
                continue;
            }
            File actualFile = null;
            for (File f : actualFileSet) {
                if (f.getName().equals(expectedFile.getName())) {
                    actualFile = f;
                    break;
                }
            }
            if (actualFile == null) {
                throw new Exception("Could not find file " + expectedFile.getName());
            }

            BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));
            BufferedReader readerActual = new BufferedReader(new FileReader(actualFile));

            String lineExpected, lineActual;
            int num = 1;
            try {
                while ((lineExpected = readerExpected.readLine()) != null) {
                    lineActual = readerActual.readLine();
                    // Assert.assertEquals(lineExpected, lineActual);
                    if (lineActual == null) {
                        throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< "
                                + lineExpected + "\n> ");
                    }
                    if (!lineExpected.equals(lineActual)) {
                        throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< "
                                + lineExpected + "\n> " + lineActual);
                    }
                    ++num;
                }
                lineActual = readerActual.readLine();
                // Assert.assertEquals(null, lineActual);
                if (lineActual != null) {
                    throw new Exception(
                            "Result for " + scriptFile + " changed at line " + num + ":\n< \n> " + lineActual);
                }
            } finally {
                readerExpected.close();
                readerActual.close();
            }
        }
        AsterixTestHelper.deleteRec(actualDir);
    }

    private static class AdmFileFilter implements FileFilter {

        public static final AdmFileFilter INSTANCE = new AdmFileFilter();

        private AdmFileFilter() {
        }

        @Override
        public boolean accept(File path) {
            if (path.isHidden() || !path.isFile()) {
                return false;
            }
            return path.getName().endsWith(".adm");
        }
    }

}
