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
package org.apache.asterix.test.sqlpp;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import org.apache.asterix.test.base.AsterixTestHelper;
import org.apache.asterix.test.common.TestHelper;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SmokeParserTest {

    private static final Logger LOGGER = Logger.getLogger(SmokeParserTest.class.getName());

    private static final String SEPARATOR = File.separator;
    private static final String EXTENSION_QUERY = "sqlpp";
    private static final String EXTENSION_RESULT = "ast";
    private static final String FILENAME_IGNORE = "ignore.txt";
    private static final String FILENAME_ONLY = "only.txt";
    private static final String PATH_BASE = "src" + SEPARATOR + "test" + SEPARATOR + "resources" + SEPARATOR
            + "parserts" + SEPARATOR;
    private static final String PATH_QUERIES = PATH_BASE + "queries_sqlpp" + SEPARATOR;
    private static final String PATH_EXPECTED = PATH_BASE + "results_parser_sqlpp" + SEPARATOR;
    private static final String PATH_ACTUAL = "parserts" + SEPARATOR;

    private static final ArrayList<String> ignore = AsterixTestHelper.readFile(FILENAME_IGNORE, PATH_BASE);
    private static final ArrayList<String> only = AsterixTestHelper.readFile(FILENAME_ONLY, PATH_BASE);

    @BeforeClass
    public static void setUp() throws Exception {
        System.err.println("Starting SQL++ parser smoke tests");
    }

    @AfterClass
    public static void tearDown() throws Exception {
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
            if (file.isFile() && file.getName().endsWith(EXTENSION_QUERY)) {
                String resultFileName = AsterixTestHelper.extToResExt(file.getName(), EXTENSION_RESULT);
                File expectedFile = new File(PATH_EXPECTED + path + resultFileName);
                File actualFile = new File(PATH_ACTUAL + SEPARATOR + path.replace(SEPARATOR, "_") + resultFileName);
                testArgs.add(new Object[] { file, expectedFile, actualFile });
            }
        }
    }

    @Parameters
    public static Collection<Object[]> tests() {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        suiteBuild(new File(PATH_QUERIES), testArgs, "");
        return testArgs;
    }

    private File actualFile;
    private File expectedFile;
    private File queryFile;

    private ParserTestExecutor parserTestExecutor = new ParserTestExecutor();

    public SmokeParserTest(File queryFile, File expectedFile, File actualFile) {
        this.queryFile = queryFile;
        this.expectedFile = expectedFile;
        this.actualFile = actualFile;
    }

    @Test
    public void test() throws Exception {
        try {
            String queryFileShort = queryFile.getPath().substring(PATH_QUERIES.length()).replace(SEPARATOR.charAt(0),
                    '/');
            if (!only.isEmpty()) {
                boolean toRun = TestHelper.isInPrefixList(only, queryFileShort);
                if (!toRun) {
                    LOGGER.info("SKIP TEST: \"" + queryFile.getPath()
                            + "\" \"only.txt\" not empty and not in \"only.txt\".");
                }
                Assume.assumeTrue(toRun);
            }
            boolean skipped = TestHelper.isInPrefixList(ignore, queryFileShort);
            if (skipped) {
                LOGGER.info("SKIP TEST: \"" + queryFile.getPath() + "\" in \"ignore.txt\".");
            }
            Assume.assumeTrue(!skipped);

            LOGGER.info("RUN TEST: \"" + queryFile.getPath() + "\"");
            parserTestExecutor.testSQLPPParser(queryFile, actualFile, expectedFile);
        } catch (Exception e) {
            if (!(e instanceof AssumptionViolatedException)) {
                LOGGER.severe("Test \"" + queryFile.getPath() + "\" FAILED!");
                throw new Exception("Test \"" + queryFile.getPath() + "\" FAILED!", e);
            } else {
                throw e;
            }
        }
    }
}
