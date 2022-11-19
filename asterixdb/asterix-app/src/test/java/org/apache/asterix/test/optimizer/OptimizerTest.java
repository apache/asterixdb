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
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.test.common.TestHelper;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the optimization tests. The current configuration runs the tests with parallel sort disabled.
 * Note: when adding a new test case and it includes sorting, provide another test case and enable parallel sort in the
 * query by setting the property (append the test case name with "_ps")
 */
@RunWith(Parameterized.class)
public class OptimizerTest extends AbstractOptimizerTest {

    protected static final String PATH_EXPECTED = PATH_BASE + "results" + SEPARATOR;
    protected String expectedFilePath;

    static {
        TEST_CONFIG_FILE_NAME = "src/main/resources/cc_no_cbo.conf";
        EXTENSION_RESULT = "plan";
        PATH_ACTUAL = "target" + SEPARATOR + "opttest" + SEPARATOR;
    }

    @Parameters(name = "OptimizerTest {index}: {0}")
    public static Collection<Object[]> tests() {
        return AbstractOptimizerTest.tests();
    }

    public OptimizerTest(File queryFile, String expectedFilePath, File actualFile) {
        super(queryFile, actualFile);
        this.expectedFilePath = expectedFilePath;
    }

    @Test
    public void test() throws Exception {
        super.test();
    }

    @Override
    protected void runAndCompare(String query, ILangCompilationProvider provider, Map<String, IAObject> queryParams,
            IHyracksClientConnection hcc) throws Exception {
        FileUtils.writeStringToFile(actualFile, "", StandardCharsets.UTF_8);
        try (PrintWriter plan = new PrintWriter(actualFile)) {
            AsterixJavaClient asterix = new AsterixJavaClient(
                    (ICcApplicationContext) integrationUtil.cc.getApplicationContext(), hcc, new StringReader(query),
                    plan, provider, statementExecutorFactory, storageComponentProvider);
            asterix.setStatementParameters(queryParams);
            asterix.compile(true, false, false, true, true, false, false);
        } catch (AlgebricksException e) {
            throw new Exception("Compile ERROR for " + queryFile + ": " + e.getMessage(), e);
        }

        List<String> linesActual = Files.readAllLines(actualFile.toPath(), StandardCharsets.UTF_8);
        List<String> linesExpected = getExpectedLines();
        TestHelper.comparePlans(linesExpected, linesActual, queryFile);
    }

    protected List<String> getExpectedLines() throws IOException {
        return Files.readAllLines(Path.of(PATH_EXPECTED, expectedFilePath), StandardCharsets.UTF_8);
    }
}
