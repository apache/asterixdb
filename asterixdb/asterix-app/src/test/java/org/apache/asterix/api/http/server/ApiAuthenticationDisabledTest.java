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
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ApiAuthenticationDisabledTest {

    private static final AsterixHyracksIntegrationUtil INTEGRATION_UTIL = new AsterixHyracksIntegrationUtil();
    private static final String CC_QUERY_SERVICE = "http://localhost:19002/query/service";
    private static final String NC_QUERY_SERVICE = "http://localhost:19004/query/service";
    private static final String NC_UDF = "http://localhost:19004/admin/udf/Default/lib";
    private static final String MISSING_CREDENTIAL_FILE = "target/no-such-dir/passwd";

    @BeforeClass
    public static void setUp() throws Exception {
        INTEGRATION_UTIL.addOption(ExternalProperties.Option.API_AUTHENTICATION_ENABLED, false);
        // with authentication off a missing credential file must not keep the cluster from starting
        INTEGRATION_UTIL.addOption(CCConfig.Option.CREDENTIAL_FILE, MISSING_CREDENTIAL_FILE);
        INTEGRATION_UTIL.addOption(NCConfig.Option.CREDENTIAL_FILE, MISSING_CREDENTIAL_FILE);
        INTEGRATION_UTIL.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        INTEGRATION_UTIL.deinit(true);
        INTEGRATION_UTIL.clearOptions();
    }

    @Test
    public void ccQueryServiceAcceptsUnauthenticatedRequests() throws Exception {
        Assert.assertEquals(200, execute(query(CC_QUERY_SERVICE)));
    }

    @Test
    public void ncQueryServiceAcceptsUnauthenticatedRequests() throws Exception {
        Assert.assertEquals(200, execute(query(NC_QUERY_SERVICE)));
    }

    @Test
    public void udfEndpointStillRequiresAuthentication() throws Exception {
        Assert.assertEquals(401, execute(new HttpDelete(NC_UDF)));
    }

    private static HttpPost query(String uri) {
        HttpPost post = new HttpPost(uri);
        post.setEntity(new UrlEncodedFormEntity(
                Collections.singletonList(new BasicNameValuePair("statement", "select 1;")), StandardCharsets.UTF_8));
        return post;
    }

    private static int execute(HttpUriRequest request) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = httpClient.execute(request)) {
            return response.getStatusLine().getStatusCode();
        }
    }
}
