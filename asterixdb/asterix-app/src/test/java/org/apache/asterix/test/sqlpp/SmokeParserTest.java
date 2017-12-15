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

import org.apache.asterix.test.base.AsterixTestHelper;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SmokeParserTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String EXTENSION_QUERY = "sqlpp";
    private static final String EXTENSION_RESULT = "ast";
    private static final String FILENAME_IGNORE = "ignore.txt";
    private static final String FILENAME_ONLY = "only.txt";
    private static final String PATH_BASE = FileUtil.joinPath("src", "test", "resources", "parserts");
    private static final String PATH_QUERIES = FileUtil.joinPath(PATH_BASE, "queries_sqlpp");
    private static final String PATH_EXPECTED = FileUtil.joinPath(PATH_BASE, "results_parser_sqlpp");
    private static final String PATH_ACTUAL = FileUtil.joinPath("target", "parserts");

    private static final ArrayList<String> ignore = AsterixTestHelper.readTestListFile(FILENAME_IGNORE, PATH_BASE);
    private static final ArrayList<String> only = AsterixTestHelper.readTestListFile(FILENAME_ONLY, PATH_BASE);

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

    @Parameters(name = "SmokeParserTest {index}: {0}")
    public static Collection<Object[]> tests() {
        Collection<Object[]> testArgs = new ArrayList<>();
        ParserTestUtil.suiteBuild(new File(PATH_QUERIES), testArgs, "", File.separator, EXTENSION_QUERY,
                EXTENSION_RESULT, PATH_EXPECTED, PATH_ACTUAL);
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
        ParserTestUtil.runTest(LOGGER, parserTestExecutor, PATH_QUERIES, queryFile, expectedFile, actualFile, ignore,
                only, File.separator);
    }
}
