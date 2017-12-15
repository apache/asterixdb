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
import java.util.Collection;
import java.util.List;

import org.apache.asterix.test.base.AsterixTestHelper;
import org.apache.asterix.test.common.TestHelper;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.Logger;
import org.junit.Assume;
import org.junit.internal.AssumptionViolatedException;

class ParserTestUtil {

    static void suiteBuild(File file, Collection<Object[]> testArgs, String path, String separator,
            String extensionQuery, String extensionResult, String pathExpected, String pathActual) {
        if (file.isDirectory() && !file.getName().startsWith(".")) {
            for (File innerfile : file.listFiles()) {
                String subdir = innerfile.isDirectory() ? FileUtil.joinPath(path, innerfile.getName()) : path;
                suiteBuild(innerfile, testArgs, subdir, separator, extensionQuery, extensionResult, pathExpected,
                        pathActual);
            }
        }
        if (file.isFile() && file.getName().endsWith(extensionQuery)) {
            String resultFileName = AsterixTestHelper.extToResExt(file.getName(), extensionResult);
            File expectedFile = new File(FileUtil.joinPath(pathExpected, path, resultFileName));
            File actualFile = new File(FileUtil.joinPath(pathActual, path, resultFileName));
            testArgs.add(new Object[] { file, expectedFile, actualFile });
        }
    }

    protected static void runTest(Logger logger, ParserTestExecutor parserTestExecutor, String pathQueries,
            File queryFile, File expectedFile, File actualFile, List<String> ignore, List<String> only,
            String separator) throws Exception {
        final char sep = separator.charAt(0);
        try {
            String queryFileShort = queryFile.getPath().substring(pathQueries.length()).replace(sep, '/');
            if (!only.isEmpty()) {
                boolean toRun = TestHelper.isInPrefixList(only, queryFileShort);
                if (!toRun) {
                    logger.info("SKIP TEST: \"" + queryFile.getPath()
                            + "\" \"only.txt\" not empty and not in \"only.txt\".");
                }
                Assume.assumeTrue(toRun);
            }
            boolean skipped = TestHelper.isInPrefixList(ignore, queryFileShort);
            if (skipped) {
                logger.info("SKIP TEST: \"" + queryFile.getPath() + "\" in \"ignore.txt\".");
            }
            Assume.assumeTrue(!skipped);

            logger.info("RUN TEST: \"" + queryFile.getPath() + "\"");
            parserTestExecutor.testSQLPPParser(queryFile, actualFile, expectedFile);

        } catch (Exception e) {
            if (!(e instanceof AssumptionViolatedException)) {
                final String msg = "Test \"" + queryFile.getPath() + "\" FAILED!";
                logger.error(msg);
                throw new Exception(msg, e);
            } else {
                throw e;
            }
        }
    }
}
