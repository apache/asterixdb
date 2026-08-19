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
package org.apache.asterix.api.http.server;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.app.message.ExecuteStatementResponseMessage;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.fields.NcResultPrinter;
import org.apache.asterix.app.result.fields.NcStatementsPrinter;
import org.apache.asterix.app.result.fields.SignaturePrinter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.ResultMetadata;
import org.apache.asterix.translator.IStatementExecutor.ResultSetInfo;
import org.apache.asterix.translator.IStatementExecutor.StatementInfo;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.HttpServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A request whose rows fail part way - the job of a cancelled request is gone, its result with it - has already had
 * part of its response sent. What the client receives must still be the whole response: the failure reported in it,
 * and the object closed. Each shape a response takes is served here and read as a client reads it. The failure is
 * injected rather than raced for, the window that produces it being a few instructions wide.
 */
public class FailedResultPrintResponseTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String BASE_PATH = "/test/failed-result";
    /** The response a request takes when it reports itself as a whole, the one the cancelled request took. */
    private static final String ROWS_PATH = BASE_PATH + "/rows";
    /** The response a request takes when it reports its statements one by one. */
    private static final String STATEMENTS_PATH = BASE_PATH + "/statements";
    /** The response a request takes when several of its statements returned rows. */
    private static final String RESULT_SETS_PATH = BASE_PATH + "/result-sets";
    /** Rows enough to exceed the 4KB response chunk, so that the header is on the wire before the failure. */
    private static final String ROWS = String.join(",", Collections.nCopies(20000, "1"));

    private static final AsterixHyracksIntegrationUtil INTEGRATION_UTIL = new AsterixHyracksIntegrationUtil() {
        @Override
        protected ICCApplication createCCApplication() {
            return new FailingResultCCApplication();
        }
    };

    @BeforeClass
    public static void setUp() throws Exception {
        INTEGRATION_UTIL.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        INTEGRATION_UTIL.deinit(true);
    }

    /**
     * The reported failure: rows streamed, the header with them, and then the result gone. The response must be
     * finished rather than cut off, and what was written must not be written again by the print that reports the
     * failure.
     */
    @Test
    public void rowsThatFailAfterTheHeaderIsSentStillFinishTheResponse() throws Exception {
        String body = post(ROWS_PATH);
        JsonNode response = failedResponse(body);

        Assert.assertEquals(body, 1, StringUtils.countMatches(body, "\"signature\""));
        Assert.assertEquals(body, 0, StringUtils.countMatches(body, "\"results-0\""));
        Assert.assertNotNull(body, response.get("results"));
    }

    /** The same failure where the request reports its statements one by one: the array must be closed. */
    @Test
    public void aStatementWhoseRowsFailStillFinishesTheResponse() throws Exception {
        String body = post(STATEMENTS_PATH);
        JsonNode response = failedResponse(body);

        Assert.assertTrue(body, response.get(NcStatementsPrinter.FIELD_NAME).isArray());
    }

    /** And where several statements returned rows: a set that cannot be read must leave no separator behind. */
    @Test
    public void aResultSetThatCannotBeReadStillFinishesTheResponse() throws Exception {
        String body = post(RESULT_SETS_PATH);
        JsonNode response = failedResponse(body);

        Assert.assertNotNull(body, response.get("results"));
    }

    /** Parses the response - which it is only if the object was closed - and pins that it reports the failure. */
    private static JsonNode failedResponse(String body) throws Exception {
        JsonNode response = OBJECT_MAPPER.readTree(body);
        Assert.assertNotNull(body, response.get("errors"));
        Assert.assertEquals(body, "fatal", response.get("status").asText());
        Assert.assertEquals(body, 1, response.get("metrics").get("errorCount").asLong());
        return response;
    }

    private static String post(String path) throws Exception {
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("statement", "select 1;"));
        HttpPost request = new HttpPost("http://localhost:19002" + path);
        request.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = httpClient.execute(request)) {
            return IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
        }
    }

    /** What a request queues to print, in place of the result it would have delivered. */
    @FunctionalInterface
    private interface FailingPrinter {
        IResponseFieldPrinter of(IApplicationContext appCtx, SessionOutput sessionOutput) throws Exception;
    }

    /** Serves the responses that fail while printing, alongside the ones a cluster serves anyway. */
    private static class FailingResultCCApplication extends CCApplication {
        @Override
        protected HttpServer setupJSONAPIServer(ExternalProperties externalProperties) throws Exception {
            HttpServer jsonAPIServer = super.setupJSONAPIServer(externalProperties);
            // the CC prints a result as it delivers it, so its failure is the request's to report
            addFailingServlet(jsonAPIServer, ROWS_PATH, true, (ctx, out) -> rowsThenFailure());
            // the NC queues what it prints and the request's own tail prints it, so the failure is the tail's
            addFailingServlet(jsonAPIServer, STATEMENTS_PATH, false,
                    FailingResultCCApplication::statementWhoseRowsFail);
            addFailingServlet(jsonAPIServer, RESULT_SETS_PATH, false,
                    FailingResultCCApplication::resultSetThatCannotBeRead);
            return jsonAPIServer;
        }

        private void addFailingServlet(HttpServer server, String path, boolean printsItsOwnResult,
                FailingPrinter printer) {
            server.addServlet(new FailingResultQueryServiceServlet(server.ctx(), new String[] { path }, appCtx,
                    printsItsOwnResult, printer));
        }

        /**
         * Writes a complete field and fails, as a result printer whose rows stop arriving does: it closes the array it
         * opened in a finally before it rethrows, so what reaches the client is a field followed by nothing.
         */
        private static IResponseFieldPrinter rowsThenFailure() {
            return new IResponseFieldPrinter() {
                @Override
                public void print(PrintWriter pw) throws HyracksDataException {
                    pw.print("\t\"results\": [ ");
                    pw.print(ROWS);
                    pw.print(" ]");
                    // as a streamed result does at the end of a page: this is what puts the header on the wire
                    pw.flush();
                    throw HyracksDataException.create(new Exception("the result of this request is gone"));
                }

                @Override
                public String getName() {
                    return "results";
                }
            };
        }

        /** A statement whose rows cannot be read, reported among the statements of its request. */
        private static IResponseFieldPrinter statementWhoseRowsFail(IApplicationContext appCtx,
                SessionOutput sessionOutput) throws Exception {
            StatementInfo statement = new StatementInfo(1, Statement.Kind.QUERY, null);
            statement.setStats(new Stats());
            statement.setResultSet(new ResultSetInfo(new JobId(1), new ResultSetId(1), null));
            return new NcStatementsPrinter(appCtx, List.of(statement), unreadableResultSet(), ResultDelivery.IMMEDIATE,
                    sessionOutput, StandardCharsets.UTF_8, false, "a-request");
        }

        /** Two result sets of one request where the second cannot be read, so the first is followed by nothing. */
        private static IResponseFieldPrinter resultSetThatCannotBeRead(IApplicationContext appCtx,
                SessionOutput sessionOutput) throws Exception {
            ResultMetadata metadata = new ResultMetadata();
            metadata.getResultSets().add(new ResultSetInfo(new JobId(1), new ResultSetId(1), null));
            metadata.getResultSets().add(new ResultSetInfo(new JobId(2), new ResultSetId(2), null));
            ExecuteStatementResponseMessage responseMsg =
                    new ExecuteStatementResponseMessage(1, "a-client", "a-request");
            responseMsg.setMetadata(metadata);
            return new NcResultPrinter(appCtx, responseMsg, emptyThenUnreadableResultSet(), ResultDelivery.IMMEDIATE,
                    sessionOutput, new Stats());
        }

        private static IResultSet unreadableResultSet() throws HyracksDataException {
            IResultSet resultSet = Mockito.mock(IResultSet.class);
            Mockito.when(resultSet.createReader(Mockito.any(), Mockito.any()))
                    .thenThrow(HyracksDataException.create(new Exception("the result of this request is gone")));
            return resultSet;
        }

        private static IResultSet emptyThenUnreadableResultSet() throws HyracksDataException {
            IResultSet resultSet = Mockito.mock(IResultSet.class);
            Mockito.when(resultSet.createReader(Mockito.any(), Mockito.any()))
                    .thenReturn(Mockito.mock(org.apache.hyracks.api.result.IResultSetReader.class))
                    .thenThrow(HyracksDataException.create(new Exception("the result of this request is gone")));
            return resultSet;
        }
    }

    /**
     * The query service, with a result that fails while it is printed. Everything the response is finished with - the
     * error, the footers, the closing of the object - is the service's own.
     */
    private static class FailingResultQueryServiceServlet extends QueryServiceServlet {

        /** Whether the result is printed as it is delivered, as the CC does, or queued for the tail, as the NC does. */
        private final boolean printsItsOwnResult;
        private final FailingPrinter printer;

        FailingResultQueryServiceServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
                boolean printsItsOwnResult, FailingPrinter printer) {
            super(ctx, paths, appCtx, ILangExtension.Language.SQLPP, null, null, null, null);
            this.printsItsOwnResult = printsItsOwnResult;
            this.printer = printer;
        }

        @Override
        protected void executeStatement(IServletRequest request, IRequestReference requestReference,
                String statementsText, SessionOutput sessionOutput, ResultProperties resultProperties,
                IStatementExecutor.StatementProperties statementProperties, IStatementExecutor.Stats stats,
                QueryServiceRequestParameters param, RequestExecutionState executionState,
                Map<String, String> optionalParameters, Map<String, byte[]> statementParameters,
                ResponsePrinter responsePrinter, List<Warning> warnings) throws Exception {
            responsePrinter.addResultPrinter(SignaturePrinter.INSTANCE);
            responsePrinter.addResultPrinter(printer.of(appCtx, sessionOutput));
            if (printsItsOwnResult) {
                responsePrinter.printResults();
            }
        }
    }
}
