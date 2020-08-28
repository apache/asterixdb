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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.hyracks.control.nc.result.ResultPartitionReader;
import org.apache.hyracks.util.Span;
import org.apache.hyracks.util.ThreadDumpUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ResultStreamingFailureTest {

    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @Before
    public void setUp() throws Exception {
        integrationUtil.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void resultStreamingFailureTest() throws Exception {
        queryAndDropConnection();
        // allow result sender to terminate and ensure no leaks
        Span timeout = Span.start(30, TimeUnit.SECONDS);
        while (!timeout.elapsed()) {
            String threadDump = ThreadDumpUtil.takeDumpString();
            if (!threadDump.contains(ResultPartitionReader.class.getName())) {
                return;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        throw new AssertionError("found leaking senders in:\n" + ThreadDumpUtil.takeDumpString());
    }

    private void queryAndDropConnection() throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault();) {
            final List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("statement", "select * from range(1, 10000000) r;"));
            HttpPost request = new HttpPost("http://localhost:19004/query/service");
            request.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
            CloseableHttpResponse response = httpClient.execute(request);
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());
            // close connection without streaming the result
        }
    }
}
