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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.metadata.NamespaceResolver;
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
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractOptimizerTest {

    private static final Logger LOGGER = LogManager.getLogger();

    protected static final String SEPARATOR = File.separator;
    protected static final String EXTENSION_SQLPP = "sqlpp";
    protected static String EXTENSION_RESULT;
    protected static final String FILENAME_IGNORE = "ignore.txt";
    protected static final String FILENAME_ONLY = "only.txt";
    protected static final String PATH_BASE =
            "src" + SEPARATOR + "test" + SEPARATOR + "resources" + SEPARATOR + "optimizerts" + SEPARATOR;
    protected static final String PATH_QUERIES = PATH_BASE + "queries" + SEPARATOR;
    protected static String PATH_ACTUAL;

    protected static final ArrayList<String> ignore = AsterixTestHelper.readTestListFile(FILENAME_IGNORE, PATH_BASE);
    protected static final ArrayList<String> only = AsterixTestHelper.readTestListFile(FILENAME_ONLY, PATH_BASE);
    protected static String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    protected static final ILangCompilationProvider sqlppCompilationProvider =
            new SqlppCompilationProvider(new NamespaceResolver(false));
    protected static ILangCompilationProvider extensionLangCompilationProvider = null;
    protected static IStatementExecutorFactory statementExecutorFactory = new DefaultStatementExecutorFactory();
    protected static IStorageComponentProvider storageComponentProvider = new StorageComponentProvider();
    protected static AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    private static final String REPEAT_EXEC = "repeat_exec:(.*)";
    private static final Pattern PATTERN_REPEAT_EXEC = Pattern.compile(REPEAT_EXEC);
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
            File[] files = file.listFiles();
            Arrays.sort(files);
            for (File innerfile : files) {
                String subdir = innerfile.isDirectory() ? path + innerfile.getName() + SEPARATOR : path;
                suiteBuildPerFile(innerfile, testArgs, subdir);
            }
        }
        if (file.isFile() && file.getName().endsWith(EXTENSION_SQLPP)) {
            String resultFileName = AsterixTestHelper.extToResExt(file.getName(), EXTENSION_RESULT);
            File actualFile = new File(PATH_ACTUAL + SEPARATOR + path + resultFileName);
            testArgs.add(new Object[] { file, path + resultFileName, actualFile });
        }
    }

    protected static Collection<Object[]> tests() {
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

    protected final File actualFile;
    protected final File queryFile;

    public AbstractOptimizerTest(final File queryFile, final File actualFile) {
        this.queryFile = queryFile;
        this.actualFile = actualFile;
    }

    protected abstract void runAndCompare(String query, ILangCompilationProvider provider,
            Map<String, IAObject> queryParams, IHyracksClientConnection hcc) throws Exception;

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
            Matcher matcher = PATTERN_REPEAT_EXEC.matcher(query);
            boolean repeat = false;
            String placeholder = null;
            JsonNode substitutions = null;
            if (matcher.find()) {
                repeat = true;
                String placeholderSubstitutions = matcher.group(1);
                String[] split = placeholderSubstitutions.split("=", 2);
                placeholder = split[0].trim();
                substitutions = MAPPER.readTree(split[1]);
            }

            LOGGER.info("ACTUAL RESULT FILE: " + actualFile.getAbsolutePath());

            // Forces the creation of actualFile.
            actualFile.getParentFile().mkdirs();

            ILangCompilationProvider provider = sqlppCompilationProvider;
            if (extensionLangCompilationProvider != null) {
                provider = extensionLangCompilationProvider;
            }
            IHyracksClientConnection hcc = integrationUtil.getHyracksClientConnection();
            if (repeat) {
                runAndRepeat(placeholder, substitutions, query, provider, queryParams, hcc);
            } else {
                runAndCompare(query, provider, queryParams, hcc);
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

    private void runAndRepeat(String placeholder, JsonNode substitutions, String query,
            ILangCompilationProvider provider, Map<String, IAObject> queryParams, IHyracksClientConnection hcc)
            throws Exception {
        if (substitutions.isArray()) {
            for (int i = 0, size = substitutions.size(); i < size; i++) {
                String substitute = substitutions.get(i).asText();
                String newQuery = query.replaceAll(placeholder, substitute);
                runAndCompare(newQuery, provider, queryParams, hcc);
            }
        } else {
            runAndCompare(query, provider, queryParams, hcc);
        }
    }
}
