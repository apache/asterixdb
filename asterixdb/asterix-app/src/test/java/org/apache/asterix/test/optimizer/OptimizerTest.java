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
package org.apache.asterix.test.optimizer;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.IdentitiyResolverFactory;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.test.base.AsterixTestHelper;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.test.runtime.HDFSCluster;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the optimization tests. The current configuration runs the tests with parallel sort disabled.
 * Note: when adding a new test case and it includes sorting, provide another test case and enable parallel sort in the
 * query by setting the property (append the test case name with "_ps")
 */
@RunWith(Parameterized.class)
public class OptimizerTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String SEPARATOR = File.separator;
    private static final String EXTENSION_SQLPP = "sqlpp";
    private static final String EXTENSION_RESULT = "plan";
    private static final String FILENAME_IGNORE = "ignore.txt";
    private static final String FILENAME_ONLY = "only.txt";
    private static final String PATH_BASE =
            "src" + SEPARATOR + "test" + SEPARATOR + "resources" + SEPARATOR + "optimizerts" + SEPARATOR;
    private static final String PATH_QUERIES = PATH_BASE + "queries" + SEPARATOR;
    private static final String PATH_EXPECTED = PATH_BASE + "results" + SEPARATOR;
    protected static final String PATH_ACTUAL = "target" + File.separator + "opttest" + SEPARATOR;

    private static final ArrayList<String> ignore = AsterixTestHelper.readTestListFile(FILENAME_IGNORE, PATH_BASE);
    private static final ArrayList<String> only = AsterixTestHelper.readTestListFile(FILENAME_ONLY, PATH_BASE);
    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static final ILangCompilationProvider sqlppCompilationProvider = new SqlppCompilationProvider();
    protected static ILangCompilationProvider extensionLangCompilationProvider = null;
    protected static IStatementExecutorFactory statementExecutorFactory = new DefaultStatementExecutorFactory();
    protected static IStorageComponentProvider storageComponentProvider = new StorageComponentProvider();

    protected static AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    private static final String PATTERN_VAR_ID_PREFIX = "\\$\\$";
    private static final Pattern PATTERN_VAR_ID = Pattern.compile(PATTERN_VAR_ID_PREFIX + "(\\d+)");

    @BeforeClass
    public static void setUp() throws Exception {
        final File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        HDFSCluster.getInstance().setup();

        integrationUtil.init(true, TEST_CONFIG_FILE_NAME);
        // Set the node resolver to be the identity resolver that expects node names
        // to be node controller ids; a valid assumption in test environment.
        System.setProperty(ExternalDataConstants.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }

        HDFSCluster.getInstance().cleanup();

        integrationUtil.deinit(true);
    }

    private static void suiteBuildPerFile(File file, Collection<Object[]> testArgs, String path) {
        if (file.isDirectory() && !file.getName().startsWith(".")) {
            File[] files = file.listFiles();
            Arrays.sort(files);
            for (File innerfile : files) {
                String subdir = innerfile.isDirectory() ? path + innerfile.getName() + SEPARATOR : path;
                suiteBuildPerFile(innerfile, testArgs, subdir);
            }
        }
        if (file.isFile() && file.getName().endsWith(EXTENSION_SQLPP)) {
            String resultFileName = AsterixTestHelper.extToResExt(file.getName(), EXTENSION_RESULT);
            File expectedFile = new File(PATH_EXPECTED + path + resultFileName);
            File actualFile = new File(PATH_ACTUAL + SEPARATOR + path + resultFileName);
            testArgs.add(new Object[] { file, expectedFile, actualFile });
        }
    }

    @Parameters(name = "OptimizerTest {index}: {0}")
    public static Collection<Object[]> tests() {
        Collection<Object[]> testArgs = new ArrayList<>();
        if (only.isEmpty()) {
            suiteBuildPerFile(new File(PATH_QUERIES), testArgs, "");
        } else {
            for (String path : only) {
                suiteBuildPerFile(new File(PATH_QUERIES + path), testArgs,
                        path.lastIndexOf(SEPARATOR) < 0 ? "" : path.substring(0, path.lastIndexOf(SEPARATOR) + 1));
            }
        }
        return testArgs;
    }

    private final File actualFile;
    private final File expectedFile;
    private final File queryFile;

    public OptimizerTest(final File queryFile, final File expectedFile, final File actualFile) {
        this.queryFile = queryFile;
        this.expectedFile = expectedFile;
        this.actualFile = actualFile;
    }

    @Test
    public void test() throws Exception {
        try {
            String queryFileShort =
                    queryFile.getPath().substring(PATH_QUERIES.length()).replace(SEPARATOR.charAt(0), '/');
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
            String query = FileUtils.readFileToString(queryFile, StandardCharsets.UTF_8);
            Map<String, IAObject> queryParams = TestHelper.readStatementParameters(query);

            LOGGER.info("ACTUAL RESULT FILE: " + actualFile.getAbsolutePath());

            // Forces the creation of actualFile.
            actualFile.getParentFile().mkdirs();

            ILangCompilationProvider provider = sqlppCompilationProvider;
            if (extensionLangCompilationProvider != null) {
                provider = extensionLangCompilationProvider;
            }
            IHyracksClientConnection hcc = integrationUtil.getHyracksClientConnection();
            try (PrintWriter plan = new PrintWriter(actualFile)) {
                AsterixJavaClient asterix = new AsterixJavaClient(
                        (ICcApplicationContext) integrationUtil.cc.getApplicationContext(), hcc,
                        new StringReader(query), plan, provider, statementExecutorFactory, storageComponentProvider);
                asterix.setStatementParameters(queryParams);
                asterix.compile(true, false, false, true, true, false, false);
            } catch (AlgebricksException e) {
                throw new Exception("Compile ERROR for " + queryFile + ": " + e.getMessage(), e);
            }

            List<String> linesExpected = Files.readAllLines(expectedFile.toPath(), StandardCharsets.UTF_8);
            List<String> linesActual = Files.readAllLines(actualFile.toPath(), StandardCharsets.UTF_8);

            int varBaseExpected = findBaseVarId(linesExpected);
            int varBaseActual = findBaseVarId(linesActual);

            Iterator<String> readerExpected = linesExpected.iterator();
            Iterator<String> readerActual = linesActual.iterator();
            String lineExpected, lineActual;
            int num = 1;
            while (readerExpected.hasNext()) {
                lineExpected = readerExpected.next();
                if (!readerActual.hasNext()) {
                    throw new Exception(
                            "Result for " + queryFile + " changed at line " + num + ":\n< " + lineExpected + "\n> ");
                }
                lineActual = readerActual.next();

                if (!planLineEquals(lineExpected, varBaseExpected, lineActual, varBaseActual)) {
                    throw new Exception("Result for " + queryFile + " changed at line " + num + ":\n< " + lineExpected
                            + "\n> " + lineActual);
                }
                ++num;
            }
            if (readerActual.hasNext()) {
                throw new Exception(
                        "Result for " + queryFile + " changed at line " + num + ":\n< \n> " + readerActual.next());
            }
            LOGGER.info("Test \"" + queryFile.getPath() + "\" PASSED!");
            actualFile.delete();
        } catch (Exception e) {
            if (!(e instanceof AssumptionViolatedException)) {
                LOGGER.error("Test \"" + queryFile.getPath() + "\" FAILED!");
                throw new Exception("Test \"" + queryFile.getPath() + "\" FAILED!", e);
            } else {
                throw e;
            }
        }
    }

    private boolean planLineEquals(String lineExpected, int varIdBaseExpected, String lineActual, int varIdBaseActual) {
        String lineExpectedNorm = normalizePlanLine(lineExpected, varIdBaseExpected);
        String lineActualNorm = normalizePlanLine(lineActual, varIdBaseActual);
        return lineExpectedNorm.equals(lineActualNorm);
    }

    // rewrite variable ids in given plan line: $$varId -> $$(varId-varIdBase)
    private String normalizePlanLine(String line, int varIdBase) {
        if (varIdBase == Integer.MAX_VALUE) {
            // plan did not contain any variables -> no rewriting necessary
            return line;
        }
        Matcher m = PATTERN_VAR_ID.matcher(line);
        StringBuffer sb = new StringBuffer(line.length());
        while (m.find()) {
            int varId = Integer.parseInt(m.group(1));
            int newVarId = varId - varIdBase;
            m.appendReplacement(sb, PATTERN_VAR_ID_PREFIX + newVarId);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private int findBaseVarId(Collection<String> plan) {
        int varIdBase = Integer.MAX_VALUE;
        Matcher m = PATTERN_VAR_ID.matcher("");
        for (String line : plan) {
            m.reset(line);
            while (m.find()) {
                int varId = Integer.parseInt(m.group(1));
                varIdBase = Math.min(varIdBase, varId);
            }
        }
        return varIdBase;
    }
}
