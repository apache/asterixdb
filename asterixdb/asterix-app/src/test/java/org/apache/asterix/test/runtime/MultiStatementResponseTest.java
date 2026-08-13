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
package org.apache.asterix.test.runtime;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * AsterixDB's endpoints report a request as a whole however many statements it carries - see
 * {@code NCQueryServiceServlet#useMultiStatementResponse}. These tests pin that on the CC and on an NC, which a
 * runtimets fixture cannot: a flat response carrying two queries repeats {@code signature}, and its JSON reader
 * rejects duplicate keys.
 */
public class MultiStatementResponseTest {

    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
    private static final String CC_QUERY_SERVICE = "http://localhost:19002/query/service";
    private static final String NC_QUERY_SERVICE = "http://localhost:19004/query/service";
    private static final String TWO_STATEMENTS = "select 1; select 2;";

    @BeforeClass
    public static void setUp() throws Exception {
        integrationUtil.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void twoStatementsAreOneResponseOnCc() throws Exception {
        assertOneFlatResponseForTwoStatements(CC_QUERY_SERVICE);
    }

    @Test
    public void twoStatementsAreOneResponseOnNc() throws Exception {
        assertOneFlatResponseForTwoStatements(NC_QUERY_SERVICE);
    }

    /**
     * Both statements run, in order, into one response: their results, one {@code status} and one {@code metrics}
     * counting the rows of the request. No {@code statements} array.
     */
    private void assertOneFlatResponseForTwoStatements(String endpoint) throws Exception {
        Response response = post(endpoint, "{\"statement\": \"" + TWO_STATEMENTS + "\", \"multi-statement\": true}");
        Assert.assertEquals(response.body(), 200, response.statusCode());
        Assert.assertFalse("expected no statements array in: " + response.body(),
                response.body().contains("\"statements\""));
        Assert.assertTrue("expected the first statement's rows in: " + response.body(),
                response.body().contains("{\"$1\":1}"));
        Assert.assertTrue("expected the second statement's rows in: " + response.body(),
                response.body().contains("{\"$1\":2}"));
        Assert.assertEquals("expected the first statement's results: " + response.body(), 1,
                StringUtils.countMatches(response.body(), "\"results\""));
        Assert.assertEquals(
                "expected the second statement's results, named as the response has always"
                        + " named a second result set: " + response.body(),
                1, StringUtils.countMatches(response.body(), "\"results-0\""));
        Assert.assertEquals("expected one status for the request: " + response.body(), 1,
                StringUtils.countMatches(response.body(), "\"status\""));
        Assert.assertEquals("expected one metrics for the request: " + response.body(), 1,
                StringUtils.countMatches(response.body(), "\"metrics\""));
        Assert.assertTrue("expected the rows of both statements to be counted: " + response.body(),
                response.body().contains("\"resultCount\": 2"));
    }

    /** A request whose statements produce no output still reports its status and metrics. */
    @Test
    public void statementWithoutOutputStillReportsStatus() throws Exception {
        Response response = post(CC_QUERY_SERVICE, "{\"statement\": \"use Default;\", \"multi-statement\": true}");
        Assert.assertEquals(response.body(), 200, response.statusCode());
        Assert.assertTrue("expected a status in: " + response.body(),
                response.body().contains("\"status\": \"success\""));
        Assert.assertTrue("expected metrics in: " + response.body(), response.body().contains("\"metrics\""));
    }

    /**
     * A warning is reported by the request that carried it: the engine attributes warnings per statement, and a
     * response reporting the request as a whole must still report them.
     */
    @Test
    public void warningsOfEveryStatementAreReported() throws Exception {
        String statements = "select 1; select /*+ nosuchhint */ 2;";
        // none are reported by default
        Response response = post(CC_QUERY_SERVICE,
                "{\"statement\": \"" + statements + "\", \"multi-statement\": true, \"max-warnings\": 5}");
        Assert.assertEquals(response.body(), 200, response.statusCode());
        Assert.assertTrue("expected the second statement's warning in: " + response.body(),
                response.body().contains("Unexpected hint: nosuchhint"));
        Assert.assertTrue("expected the warning to be counted for the request: " + response.body(),
                response.body().contains("\"warningCount\": 1"));
    }

    /**
     * Async answers with a handle before the statement has run, so several statements are rejected (see the
     * api/multi-statement fixtures). Deferred runs them in order, so it stays supported: one handle per statement.
     */
    @Test
    public void deferredSupportedForMultipleStatements() throws Exception {
        Response response = post(CC_QUERY_SERVICE,
                "{\"statement\": \"" + TWO_STATEMENTS + "\", \"multi-statement\": true, \"mode\": \"deferred\"}");
        Assert.assertEquals(response.body(), 200, response.statusCode());
        Assert.assertTrue("expected a handle per statement in: " + response.body(),
                StringUtils.countMatches(response.body(), "\"handle\"") == 2);
    }

    private static Response post(String endpoint, String body) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost request = new HttpPost(endpoint);
            request.setHeader("Content-Type", "application/json");
            request.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                return new Response(response.getStatusLine().getStatusCode(),
                        EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
            }
        }
    }

    private record Response(int statusCode, String body) {
    }
}
