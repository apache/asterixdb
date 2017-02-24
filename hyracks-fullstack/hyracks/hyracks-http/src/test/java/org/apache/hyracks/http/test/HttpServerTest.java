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
package org.apache.hyracks.http.test;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;
import org.apache.hyracks.http.servlet.SlowServlet;
import org.junit.Assert;
import org.junit.Test;

import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpServerTest {
    static final int PORT = 9898;
    static final int NUM_EXECUTOR_THREADS = 16;
    static final int SERVER_QUEUE_SIZE = 16;
    static final int NUM_OF_REQUESTS = 48;
    static final String HOST = "localhost";
    static final String PROTOCOL = "http";
    static final String PATH = "/";
    static final AtomicInteger SUCCESS_COUNT = new AtomicInteger();
    static final AtomicInteger UNAVAILABLE_COUNT = new AtomicInteger();
    static final AtomicInteger OTHER_COUNT = new AtomicInteger();
    static final List<Thread> THREADS = new ArrayList<>();

    @Test
    public void testOverloadingServer() throws Exception {
        WebManager webMgr = new WebManager();
        HttpServer server =
                new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, NUM_EXECUTOR_THREADS, SERVER_QUEUE_SIZE);
        SlowServlet servlet = new SlowServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        try {
            request(NUM_OF_REQUESTS);
            for (Thread thread : THREADS) {
                thread.join();
            }
            Assert.assertEquals(32, SUCCESS_COUNT.get());
            Assert.assertEquals(16, UNAVAILABLE_COUNT.get());
            Assert.assertEquals(0, OTHER_COUNT.get());
        } finally {
            webMgr.stop();
        }
    }

    private void request(int count) {
        for (int i = 0; i < count; i++) {
            Thread next = new Thread(() -> {
                try {
                    HttpUriRequest request = request();
                    HttpResponse response = executeHttpRequest(request);
                    if (response.getStatusLine().getStatusCode() == HttpResponseStatus.OK.code()) {
                        SUCCESS_COUNT.incrementAndGet();
                    } else if (response.getStatusLine().getStatusCode() == HttpResponseStatus.SERVICE_UNAVAILABLE
                            .code()) {
                        UNAVAILABLE_COUNT.incrementAndGet();
                    } else {
                        OTHER_COUNT.incrementAndGet();
                    }
                    InputStream responseStream = response.getEntity().getContent();
                    IOUtils.closeQuietly(responseStream);
                } catch (Throwable th) {
                    th.printStackTrace();
                }
            });
            THREADS.add(next);
            next.start();
        }
    }

    protected HttpResponse executeHttpRequest(HttpUriRequest method) throws Exception {
        HttpClient client = HttpClients.custom().setRetryHandler(StandardHttpRequestRetryHandler.INSTANCE).build();
        try {
            return client.execute(method);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    protected HttpUriRequest request() throws URISyntaxException {
        URI uri = new URI(PROTOCOL, null, HOST, PORT, PATH, null, null);
        RequestBuilder builder = RequestBuilder.get(uri);
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }
}
