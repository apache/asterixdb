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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.Socket;
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

    @Test
    public void testMalformedString() throws Exception {
        WebManager webMgr = new WebManager();
        HttpServer server =
                new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, NUM_EXECUTOR_THREADS, SERVER_QUEUE_SIZE);
        SlowServlet servlet = new SlowServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        try {
            StringBuilder response = new StringBuilder();
            try (Socket s = new Socket(InetAddress.getLocalHost(), PORT)) {
                PrintWriter pw = new PrintWriter(s.getOutputStream());
                pw.println("GET /?handle=%7B%22handle%22%3A%5B0%2C%200%5D%7 HTTP/1.1");
                pw.println("Host: 127.0.0.1");
                pw.println();
                pw.flush();
                BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
                String line;
                while ((line = br.readLine()) != null) {
                    response.append(line).append('\n');
                }
                br.close();
            }
            String output = response.toString();
            Assert.assertTrue(output.contains(HttpResponseStatus.BAD_REQUEST.toString()));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            webMgr.stop();
        }
    }

    public static void setPrivateField(Object obj, String filedName, Object value) throws Exception {
        Field f = obj.getClass().getDeclaredField(filedName);
        f.setAccessible(true);
        f.set(obj, value);
    }

    private void request(int count) {
        for (int i = 0; i < count; i++) {
            Thread next = new Thread(() -> {
                try {
                    HttpUriRequest request = request(null);
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

    protected HttpUriRequest request(String query) throws URISyntaxException {
        URI uri = new URI(PROTOCOL, null, HOST, PORT, PATH, query, null);
        RequestBuilder builder = RequestBuilder.get(uri);
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }
}
