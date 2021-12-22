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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.om.base.IAObject;
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

    private static final String PATTERN_VAR_ID_PREFIX = "\\$\\$";
    private static final Pattern PATTERN_VAR_ID = Pattern.compile(PATTERN_VAR_ID_PREFIX + "(\\d+)");

    static {
        EXTENSION_RESULT = "plan";
        PATH_ACTUAL = "target" + File.separator + "opttest" + SEPARATOR;
    }

    @Parameters(name = "OptimizerTest {index}: {0}")
    public static Collection<Object[]> tests() {
        return AbstractOptimizerTest.tests();
    }

    public OptimizerTest(final File queryFile, final File expectedFile, final File actualFile) {
        super(queryFile, expectedFile, actualFile);
    }

    @Test
    public void test() throws Exception {
        super.test();
    }

    @Override
    protected void runAndCompare(String query, ILangCompilationProvider provider, Map<String, IAObject> queryParams,
            IHyracksClientConnection hcc, List<String> linesExpected) throws Exception {
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
    }

    @Override
    protected List<String> getExpectedLines() throws IOException {
        return Files.readAllLines(expectedFile.toPath(), StandardCharsets.UTF_8);
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
