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

package org.apache.asterix.test.jsonplan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.IdentitiyResolverFactory;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.test.base.AsterixTestHelper;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.test.runtime.HDFSCluster;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionConfig.PlanFormat;
import org.apache.commons.io.FileUtils;
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(Parameterized.class)
public class JsonLogicalPlanTest {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        OBJECT_MAPPER.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    protected static final String SEPARATOR = File.separator;
    private static final String EXTENSION_AQL = "aql";
    private static final String EXTENSION_SQLPP = "sqlpp";
    private static final String EXTENSION_RESULT = "plan.json";
    private static final String FILENAME_IGNORE = "ignore.txt";
    private static final String FILENAME_ONLY = "only.txt";
    private static final String PATH_BASE =
            "src" + SEPARATOR + "test" + SEPARATOR + "resources" + SEPARATOR + "optimizerts" + SEPARATOR;
    private static final String PATH_QUERIES = PATH_BASE + "queries" + SEPARATOR;
    protected static String PATH_ACTUAL = "target" + File.separator + "jplantest" + SEPARATOR;
    protected static boolean optimized = false;

    private static final ArrayList<String> ignore = AsterixTestHelper.readTestListFile(FILENAME_IGNORE, PATH_BASE);
    private static final ArrayList<String> only = AsterixTestHelper.readTestListFile(FILENAME_ONLY, PATH_BASE);
    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static final ILangCompilationProvider aqlCompilationProvider = new AqlCompilationProvider();
    private static final ILangCompilationProvider sqlppCompilationProvider = new SqlppCompilationProvider();
    protected static ILangCompilationProvider extensionLangCompilationProvider = null;
    protected static IStatementExecutorFactory statementExecutorFactory = new DefaultStatementExecutorFactory();
    protected static IStorageComponentProvider storageComponentProvider = new StorageComponentProvider();

    protected static AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
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
            for (File innerfile : file.listFiles()) {
                String subdir = innerfile.isDirectory() ? path + innerfile.getName() + SEPARATOR : path;
                suiteBuildPerFile(innerfile, testArgs, subdir);
            }
        }
        if (file.isFile() && (file.getName().endsWith(EXTENSION_AQL) || file.getName().endsWith(EXTENSION_SQLPP))) {
            String resultFileName = AsterixTestHelper.extToResExt(file.getName(), EXTENSION_RESULT);
            File actualFile = new File(PATH_ACTUAL + SEPARATOR + path + resultFileName);
            testArgs.add(new Object[] { file, actualFile });
        }
    }

    @Parameters(name = "JsonLogicalPlanTest {index}: {0}")
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
    private final File queryFile;

    public JsonLogicalPlanTest(final File queryFile, final File actualFile) {
        this.queryFile = queryFile;
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

            // Forces the creation of actualFile.
            actualFile.getParentFile().mkdirs();

            ILangCompilationProvider provider =
                    queryFile.getName().endsWith("aql") ? aqlCompilationProvider : sqlppCompilationProvider;
            if (extensionLangCompilationProvider != null) {
                provider = extensionLangCompilationProvider;
            }
            IHyracksClientConnection hcc = integrationUtil.getHyracksClientConnection();
            String planStr;
            try (PrintWriter plan = new PrintWriter(actualFile)) {
                AsterixJavaClient asterix = new AsterixJavaClient(
                        (ICcApplicationContext) integrationUtil.cc.getApplicationContext(), hcc,
                        new StringReader(query), plan, provider, statementExecutorFactory, storageComponentProvider);
                asterix.setStatementParameters(queryParams);
                asterix.compile(true, false, !optimized, optimized, false, false, false, PlanFormat.JSON);
                ExecutionPlans executionPlans = asterix.getExecutionPlans();
                planStr = optimized ? executionPlans.getOptimizedLogicalPlan() : executionPlans.getLogicalPlan();
                plan.write(planStr);
            } catch (AsterixException e) {
                throw new Exception("Compile ERROR for " + queryFile + ": " + e.getMessage(), e);
            }

            BufferedReader readerActual =
                    new BufferedReader(new InputStreamReader(new FileInputStream(actualFile), "UTF-8"));
            String lineActual, objectActual = "";
            boolean firstPlan = false;
            while ((lineActual = readerActual.readLine()) != null) {
                if (lineActual.contains("--")) {
                    if (firstPlan) {
                        break;
                    }
                    firstPlan = true;

                } else {
                    objectActual = objectActual + lineActual;
                }
            }

            try {
                JsonNode jsonNode = OBJECT_MAPPER.readTree(planStr);
                if (jsonNode == null || !jsonNode.isObject()) {
                    throw new Exception("ERROR: No JSON plan or plan is malformed!");
                }
            } finally {
                readerActual.close();
            }

        } catch (Exception e) {
            if (!(e instanceof AssumptionViolatedException)) {
                LOGGER.error("Test \"" + queryFile.getPath() + "\" FAILED!");
                throw new Exception("Test \"" + queryFile.getPath() + "\" FAILED!", e);
            } else {
                throw e;
            }
        }
    }
}
