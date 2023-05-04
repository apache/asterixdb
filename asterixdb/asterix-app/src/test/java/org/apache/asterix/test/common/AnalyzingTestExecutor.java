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
package org.apache.asterix.test.common;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AnalyzingTestExecutor extends TestExecutor {

    private Pattern loadPattern = Pattern.compile("(load)\\s+(dataset|collection)\\s+([a-zA-z0-9\\.`]+)\\s+using",
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    private Pattern upsertPattern = Pattern.compile("^(upsert|insert)\\s+into\\s+([a-zA-z0-9\\.`]+)\\s*(\\(|as)?",
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    private Pattern usePattern = Pattern.compile("use\\s+(dataverse\\s+)?([a-zA-z0-9\\.`]+)\\s*;",
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

    public AnalyzingTestExecutor() {
        super("results_cbo");
    }

    @Override
    public ExtractedResult executeSqlppUpdateOrDdl(String statement, TestCaseContext.OutputFormat outputFormat)
            throws Exception {
        Matcher dvMatcher = usePattern.matcher(statement);
        String dv = "";
        if (dvMatcher.find()) {
            dv = dvMatcher.group(2) + ".";
        }
        Matcher dsMatcher = loadPattern.matcher(statement);
        Matcher upsertMatcher = upsertPattern.matcher(statement);
        ExtractedResult res = super.executeUpdateOrDdl(statement, outputFormat, getQueryServiceUri(SQLPP));
        analyzeFromRegex(dsMatcher, dv, 3);
        analyzeFromRegex(upsertMatcher, dv, 2);
        return res;
    }

    private void analyzeFromRegex(Matcher m, String dv, int pos) throws Exception {
        while (m.find()) {
            String ds = m.group(pos);
            StringBuilder analyzeStmt = new StringBuilder();
            analyzeStmt.append("ANALYZE DATASET ");
            if (!ds.contains(".")) {
                analyzeStmt.append(dv);
            }
            analyzeStmt.append(ds);
            analyzeStmt.append(" WITH {\"sample-seed\": \"1000\"}");
            analyzeStmt.append(";");
            InputStream resultStream = executeQueryService(analyzeStmt.toString(), getQueryServiceUri(SQLPP),
                    TestCaseContext.OutputFormat.CLEAN_JSON);
            String resultStr = IOUtils.toString(resultStream, UTF_8);
            JsonNode result = RESULT_NODE_READER.<ObjectNode> readValue(resultStr).get("status");
            if (!"success".equals(result.asText())) {
                JsonNode error = RESULT_NODE_READER.<ObjectNode> readValue(resultStr).get("errors");
                throw new IllegalStateException("ANALYZE DATASET failed with error: " + error);
            }
        }
    }

}
