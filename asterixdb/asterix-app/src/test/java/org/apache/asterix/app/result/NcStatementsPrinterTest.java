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
package org.apache.asterix.app.result;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.asterix.app.result.fields.NcStatementsPrinter;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.StatementInfo;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The {@value NcStatementsPrinter#FIELD_NAME} array is written field by field rather than serialized, so its braces and
 * separators are the printer's own doing. These tests pin that what it writes parses, and describes each statement.
 */
public class NcStatementsPrinterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void everyStatementIsReportedInOrder() throws Exception {
        StatementInfo first = statement(1, Statement.Kind.QUERY);
        StatementInfo second = statement(2, Statement.Kind.CREATE_DATASET);
        JsonNode statements = print(List.of(first, second));

        Assert.assertEquals(statements.toString(), 2, statements.size());
        Assert.assertEquals(statements.toString(), 1, statements.get(0).get("statement").asInt());
        Assert.assertEquals(statements.toString(), 2, statements.get(1).get("statement").asInt());
        Assert.assertEquals(statements.toString(), "success", statements.get(0).get("status").asText());
        Assert.assertNotNull(statements.toString(), statements.get(0).get("kind"));
        Assert.assertNotNull(statements.toString(), statements.get(0).get("metrics"));
    }

    /** A failed statement is reported with its own error and outcome, alongside the ones that ran. */
    @Test
    public void aFailedStatementReportsItsError() throws Exception {
        StatementInfo succeeded = statement(1, Statement.Kind.QUERY);
        StatementInfo failed = statement(2, Statement.Kind.QUERY);
        failed.setError(new CompilationException(ErrorCode.PARSE_ERROR, "bad statement"));
        JsonNode statements = print(List.of(succeeded, failed));

        Assert.assertEquals(statements.toString(), "success", statements.get(0).get("status").asText());
        Assert.assertNull(statements.toString(), statements.get(0).get("errors"));
        Assert.assertEquals(statements.toString(), "fatal", statements.get(1).get("status").asText());
        Assert.assertNotNull(statements.toString(), statements.get(1).get("errors"));
    }

    /** The request's own dataverse declaration is not reported and does not shift the numbering. */
    @Test
    public void theRequestsOwnStatementIsNotReported() throws Exception {
        StatementInfo synthetic = statement(0, Statement.Kind.DATAVERSE_DECL);
        StatementInfo sent = statement(1, Statement.Kind.QUERY);
        JsonNode statements = print(List.of(synthetic, sent));

        Assert.assertEquals(statements.toString(), 1, statements.size());
        Assert.assertEquals(statements.toString(), 1, statements.get(0).get("statement").asInt());
    }

    /** An extension statement is named by its name, since its kind alone does not identify it. */
    @Test
    public void anExtensionStatementIsReportedByName() throws Exception {
        StatementInfo extension = new StatementInfo(1, Statement.Kind.EXTENSION, "DESCRIBE LINK");
        extension.setStats(new Stats());
        JsonNode statements = print(List.of(extension));

        Assert.assertEquals(statements.toString(), "DESCRIBE LINK", statements.get(0).get("kind").asText());
    }

    private static StatementInfo statement(int position, Statement.Kind kind) {
        StatementInfo statementInfo = new StatementInfo(position, kind, null);
        statementInfo.setStats(new Stats());
        return statementInfo;
    }

    /** Prints the statements and returns the array that was written. */
    private static JsonNode print(List<StatementInfo> statements) throws Exception {
        StringWriter out = new StringWriter();
        PrintWriter pw = new PrintWriter(out);
        SessionOutput sessionOutput = new SessionOutput(new SessionConfig(SessionConfig.OutputFormat.CLEAN_JSON), pw);
        new NcStatementsPrinter(null, statements, null, ResultDelivery.IMMEDIATE, sessionOutput, StandardCharsets.UTF_8,
                false, "test-request").print(pw);
        pw.flush();
        JsonNode response = OBJECT_MAPPER.readTree("{\n" + out + "\n}");
        JsonNode statementsField = response.get(NcStatementsPrinter.FIELD_NAME);
        Assert.assertNotNull(out.toString(), statementsField);
        return statementsField;
    }
}
