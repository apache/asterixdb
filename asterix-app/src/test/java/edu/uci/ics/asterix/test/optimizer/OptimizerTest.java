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
package edu.uci.ics.asterix.test.optimizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.api.java.AsterixJavaClient;
import edu.uci.ics.asterix.common.config.AsterixPropertiesAccessor;
import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.external.util.IdentitiyResolverFactory;
import edu.uci.ics.asterix.test.base.AsterixTestHelper;
import edu.uci.ics.asterix.test.common.TestHelper;

@RunWith(Parameterized.class)
public class OptimizerTest {

    private static final Logger LOGGER = Logger.getLogger(OptimizerTest.class.getName());

    private static final String SEPARATOR = File.separator;
    private static final String EXTENSION_QUERY = "aql";
    private static final String EXTENSION_RESULT = "plan";
    private static final String FILENAME_IGNORE = "ignore.txt";
    private static final String FILENAME_ONLY = "only.txt";
    private static final String PATH_BASE = "src" + SEPARATOR + "test" + SEPARATOR + "resources" + SEPARATOR
            + "optimizerts" + SEPARATOR;
    private static final String PATH_QUERIES = PATH_BASE + "queries" + SEPARATOR;
    private static final String PATH_EXPECTED = PATH_BASE + "results" + SEPARATOR;
    private static final String PATH_ACTUAL = "opttest" + SEPARATOR;

    private static final ArrayList<String> ignore = AsterixTestHelper.readFile(FILENAME_IGNORE, PATH_BASE);
    private static final ArrayList<String> only = AsterixTestHelper.readFile(FILENAME_ONLY, PATH_BASE);
    private static final String TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";

    private static AsterixTransactionProperties txnProperties;

    @BeforeClass
    public static void setUp() throws Exception {
        // File outdir = new File(PATH_ACTUAL);
        // outdir.mkdirs();

        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        System.setProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY, "19002");
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        AsterixPropertiesAccessor apa = new AsterixPropertiesAccessor();
        txnProperties = new AsterixTransactionProperties(apa);

        deleteTransactionLogs();

        AsterixHyracksIntegrationUtil.init();
        // Set the node resolver to be the identity resolver that expects node names 
        // to be node controller ids; a valid assumption in test environment. 
        System.setProperty(FileSystemBasedAdapter.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());
    }

    private static void deleteTransactionLogs() throws Exception {
        for (String ncId : AsterixHyracksIntegrationUtil.NC_IDS) {
            File log = new File(txnProperties.getLogDirectory(ncId));
            if (log.exists()) {
                FileUtils.deleteDirectory(log);
            }
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // _bootstrap.stop();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }

        deleteTransactionLogs();
    }

    private static void suiteBuild(File dir, Collection<Object[]> testArgs, String path) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory() && !file.getName().startsWith(".")) {
                suiteBuild(file, testArgs, path + file.getName() + SEPARATOR);
            }
            if (file.isFile() && file.getName().endsWith(EXTENSION_QUERY)
            // && !ignore.contains(path + file.getName())
            ) {
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

    public OptimizerTest(File queryFile, File expectedFile, File actualFile) {
        this.queryFile = queryFile;
        this.expectedFile = expectedFile;
        this.actualFile = actualFile;
    }

    @Test
    public void test() throws Exception {
        try {
            String queryFileShort = queryFile.getPath().substring(PATH_QUERIES.length())
                    .replace(SEPARATOR.charAt(0), '/');
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
            Reader query = new BufferedReader(new InputStreamReader(new FileInputStream(queryFile), "UTF-8"));
            PrintWriter plan = new PrintWriter(actualFile);
            AsterixJavaClient asterix = new AsterixJavaClient(
                    AsterixHyracksIntegrationUtil.getHyracksClientConnection(), query, plan);
            try {
                asterix.compile(true, false, false, true, true, false, false);
            } catch (AsterixException e) {
                plan.close();
                query.close();
                throw new Exception("Compile ERROR for " + queryFile + ": " + e.getMessage(), e);
            }
            plan.close();
            query.close();

            BufferedReader readerExpected = new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile),
                    "UTF-8"));
            BufferedReader readerActual = new BufferedReader(new InputStreamReader(new FileInputStream(actualFile),
                    "UTF-8"));

            String lineExpected, lineActual;
            int num = 1;
            try {
                while ((lineExpected = readerExpected.readLine()) != null) {
                    lineActual = readerActual.readLine();
                    // Assert.assertEquals(lineExpected, lineActual);
                    if (lineActual == null) {
                        throw new Exception("Result for " + queryFile + " changed at line " + num + ":\n< "
                                + lineExpected + "\n> ");
                    }
                    if (!lineExpected.equals(lineActual)) {
                        throw new Exception("Result for " + queryFile + " changed at line " + num + ":\n< "
                                + lineExpected + "\n> " + lineActual);
                    }
                    ++num;
                }
                lineActual = readerActual.readLine();
                // Assert.assertEquals(null, lineActual);
                if (lineActual != null) {
                    throw new Exception("Result for " + queryFile + " changed at line " + num + ":\n< \n> "
                            + lineActual);
                }
                LOGGER.info("Test \"" + queryFile.getPath() + "\" PASSED!");
                actualFile.delete();
            } finally {
                readerExpected.close();
                readerActual.close();
            }
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
