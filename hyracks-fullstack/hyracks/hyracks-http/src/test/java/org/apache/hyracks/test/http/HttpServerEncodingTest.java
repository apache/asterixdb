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
package org.apache.hyracks.test.http;

import static org.apache.hyracks.util.string.UTF8StringSample.STRING_NEEDS_2_JAVA_CHARS_1;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_NEEDS_2_JAVA_CHARS_2;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.HttpServerConfig;
import org.apache.hyracks.http.server.HttpServerConfigBuilder;
import org.apache.hyracks.http.server.WebManager;
import org.apache.hyracks.test.http.servlet.CompliantEchoServlet;
import org.apache.hyracks.test.string.EncodingUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HttpServerEncodingTest {
    private static final int PORT = 9898;
    private static final String HOST = "localhost";
    private static final String PROTOCOL = "http";
    private static final String PATH = "/";
    private static WebManager webMgr;
    private static CloseableHttpClient client;
    private static URI uri;

    @Parameters(name = "HttpServerEncodingTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        List<Object[]> tests = new ArrayList<>();
        Stream.of("encoding is hard", "中文字符", "لا يوجد ترجمة لكُ", STRING_NEEDS_2_JAVA_CHARS_1,
                STRING_NEEDS_2_JAVA_CHARS_2).forEach(input -> {
                    Set<Charset> legalCharsets = EncodingUtils.getLegalCharsetsFor(input);
                    legalCharsets.forEach(charsetIn -> legalCharsets.forEach(charsetOut -> tests
                            .add(new Object[] { input + ":" + charsetIn.displayName() + "->" + charsetOut.displayName(),
                                    input, charsetIn, charsetOut })));
                });
        return tests;
    }

    @Parameter(0)
    public String testName;

    @Parameter(1)
    public String input;

    @Parameter(2)
    public Charset inputCharset;

    @Parameter(3)
    public Charset outputCharset;

    @BeforeClass
    public static void setup() throws Exception {
        int numExecutors = 16;
        int serverQueueSize = 16;
        webMgr = new WebManager();
        final HttpServerConfig config = HttpServerConfigBuilder.custom().setThreadCount(numExecutors)
                .setRequestQueueSize(serverQueueSize).build();
        HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, config);
        CompliantEchoServlet servlet = new CompliantEchoServlet(server.ctx(), PATH);
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        client = HttpClients.custom().build();
        uri = new URI(PROTOCOL, null, HOST, PORT, PATH, null, null);

    }

    @AfterClass
    public static void teardown() throws Exception {
        client.close();
        webMgr.stop();
    }

    @Test
    public void testAcceptCharset() throws Exception {
        RequestBuilder builder = RequestBuilder.post(uri);
        builder.setEntity(new StringEntity(input, inputCharset));
        builder.setCharset(inputCharset);
        builder.setHeader(HttpHeaders.ACCEPT_CHARSET, inputCharset.name());
        try (CloseableHttpResponse response = client.execute(builder.build())) {
            HttpEntity entity = response.getEntity();
            Assert.assertEquals(String.valueOf(inputCharset), ContentType.getOrDefault(entity).getCharset(),
                    inputCharset);
            Assert.assertEquals(String.valueOf(inputCharset), input, EntityUtils.toString(entity));
        }
    }

}
