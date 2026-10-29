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

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.test.common.TestHelper;
import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ApiAuthenticationTest {

    private static final AsterixHyracksIntegrationUtil INTEGRATION_UTIL = new AsterixHyracksIntegrationUtil();
    private static final String LEGACY_WEB_UI = "http://localhost:19001/";
    private static final String CC_QUERY_SERVICE = "http://localhost:19002/query/service";
    private static final String NC_QUERY_SERVICE = "http://localhost:19004/query/service";

    @BeforeClass
    public static void setUp() throws Exception {
        INTEGRATION_UTIL.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        INTEGRATION_UTIL.deinit(true);
    }

    @Test
    public void legacyWebUiChallengesUnauthenticatedRequests() throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = httpClient.execute(new HttpGet(LEGACY_WEB_UI))) {
            Assert.assertEquals(401, response.getStatusLine().getStatusCode());
            Assert.assertTrue("a browser only prompts for credentials on a Basic challenge",
                    response.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE).getValue().startsWith("Basic "));
        }
    }

    @Test
    public void legacyWebUiDoesNotRunUnauthenticatedQueries() throws Exception {
        HttpPost post = new HttpPost(LEGACY_WEB_UI);
        post.setEntity(new UrlEncodedFormEntity(Collections.singletonList(new BasicNameValuePair("query", "select 1;")),
                StandardCharsets.UTF_8));
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = httpClient.execute(post)) {
            Assert.assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void legacyWebUiAcceptsAuthenticatedRequests() throws Exception {
        try (CloseableHttpClient httpClient = TestHelper.createAuthenticatedHttpClient();
                CloseableHttpResponse response = httpClient.execute(new HttpGet(LEGACY_WEB_UI))) {
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void ccApiDoesNotChallengeUnauthenticatedRequests() throws Exception {
        assertRejectedWithoutChallenge(CC_QUERY_SERVICE);
    }

    @Test
    public void ncApiDoesNotChallengeUnauthenticatedRequests() throws Exception {
        assertRejectedWithoutChallenge(NC_QUERY_SERVICE);
    }

    private static void assertRejectedWithoutChallenge(String uri) throws Exception {
        HttpPost post = new HttpPost(uri);
        post.setEntity(new UrlEncodedFormEntity(
                Collections.singletonList(new BasicNameValuePair("statement", "select 1;")), StandardCharsets.UTF_8));
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = httpClient.execute(post)) {
            Assert.assertEquals(401, response.getStatusLine().getStatusCode());
            Assert.assertNull(response.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE));
        }
    }
}
