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
import java.util.Collection;
import java.util.Map;

import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.test.optimizer.AbstractOptimizerTest;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.SessionConfig.PlanFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(Parameterized.class)
public class JsonLogicalPlanTest extends AbstractOptimizerTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        OBJECT_MAPPER.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    protected static boolean optimized = false;

    static {
        EXTENSION_RESULT = "plan.json";
        PATH_ACTUAL = "target" + SEPARATOR + "jplantest" + SEPARATOR;
    }

    @Parameters(name = "JsonLogicalPlanTest {index}: {0}")
    public static Collection<Object[]> tests() {
        return AbstractOptimizerTest.tests();
    }

    public JsonLogicalPlanTest(File queryFile, String expectedFilePath, File actualFile) {
        super(queryFile, actualFile);
    }

    @Test
    public void test() throws Exception {
        super.test();
    }

    @Override
    protected void runAndCompare(String query, ILangCompilationProvider provider, Map<String, IAObject> queryParams,
            IHyracksClientConnection hcc) throws Exception {
        FileUtils.writeStringToFile(actualFile, "", StandardCharsets.UTF_8);
        String planStr;
        try (PrintWriter plan = new PrintWriter(actualFile)) {
            AsterixJavaClient asterix = new AsterixJavaClient(
                    (ICcApplicationContext) integrationUtil.cc.getApplicationContext(), hcc, new StringReader(query),
                    plan, provider, statementExecutorFactory, storageComponentProvider);
            asterix.setStatementParameters(queryParams);
            asterix.compile(true, false, !optimized, optimized, false, false, false, PlanFormat.JSON);
            ExecutionPlans executionPlans = asterix.getExecutionPlans();
            planStr = optimized ? executionPlans.getOptimizedLogicalPlan() : executionPlans.getLogicalPlan();
            plan.write(planStr);
        } catch (AsterixException e) {
            throw new Exception("Compile ERROR for " + queryFile + ": " + e.getMessage(), e);
        }

        BufferedReader readerActual =
                new BufferedReader(new InputStreamReader(new FileInputStream(actualFile), StandardCharsets.UTF_8));
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
    }
}
