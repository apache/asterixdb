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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.http.servlet.ChattyServlet;
import org.apache.hyracks.http.servlet.SleepyServlet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpServerTest {
    static final boolean PRINT_TO_CONSOLE = false;
    static final int PORT = 9898;
    static final String HOST = "localhost";
    static final String PROTOCOL = "http";
    static final String PATH = "/";
    static final AtomicInteger SUCCESS_COUNT = new AtomicInteger();
    static final AtomicInteger UNAVAILABLE_COUNT = new AtomicInteger();
    static final AtomicInteger OTHER_COUNT = new AtomicInteger();
    static final AtomicInteger EXCEPTION_COUNT = new AtomicInteger();
    static final List<Future<Void>> FUTURES = new ArrayList<>();
    static final ExecutorService executor = Executors.newCachedThreadPool();

    @Before
    public void setUp() {
        SUCCESS_COUNT.set(0);
        UNAVAILABLE_COUNT.set(0);
        OTHER_COUNT.set(0);
        EXCEPTION_COUNT.set(0);
    }

    @Test
    public void testOverloadingServer() throws Exception {
        WebManager webMgr = new WebManager();
        int numExecutors = 16;
        int serverQueueSize = 16;
        int numRequests = 128;
        HttpServer server =
                new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, numExecutors, serverQueueSize);
        SleepyServlet servlet = new SleepyServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        int expectedSuccess = numExecutors + serverQueueSize;
        int expectedUnavailable = numRequests - expectedSuccess;
        try {
            request(expectedSuccess);
            waitTillQueued(server, serverQueueSize);
            ArrayList<Future<Void>> successSet = started();
            request(expectedUnavailable);
            ArrayList<Future<Void>> rejectedSet = started();
            for (Future<Void> f : rejectedSet) {
                f.get();
            }
            servlet.wakeUp();
            for (Future<Void> f : successSet) {
                f.get();
            }
            Assert.assertEquals(expectedSuccess, SUCCESS_COUNT.get());
            Assert.assertEquals(expectedUnavailable, UNAVAILABLE_COUNT.get() + EXCEPTION_COUNT.get());
            System.err.println("Number of rejections: " + UNAVAILABLE_COUNT.get());
            System.err.println("Number of exceptions: " + EXCEPTION_COUNT.get());
            Assert.assertEquals(0, OTHER_COUNT.get());
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        } finally {
            webMgr.stop();
        }
    }

    private void waitTillQueued(HttpServer server, int expectedQueued) throws Exception {
        int maxAttempts = 15;
        int attempt = 0;
        int queued = server.getWorkQueueSize();
        while (queued != expectedQueued) {
            attempt++;
            if (attempt > maxAttempts) {
                throw new Exception("Number of queued requests (" + queued + ") didn't match the expected number ("
                        + expectedQueued + ")");
            }
            Thread.sleep(1000); // NOSONAR polling is the clean way
            queued = server.getWorkQueueSize();
        }
    }

    @Test
    public void testReleaseRejectedRequest() throws Exception {
        WebManager webMgr = new WebManager();
        int numRequests = 64;
        int numExecutors = 2;
        int serverQueueSize = 2;
        int numPatches = 60;
        HttpServer server =
                new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, numExecutors, serverQueueSize);
        SleepyServlet servlet = new SleepyServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        request(numExecutors + serverQueueSize);
        ArrayList<Future<Void>> stuck = started();
        waitTillQueued(server, serverQueueSize);
        try {
            try {
                for (int i = 0; i < numPatches; i++) {
                    HttpUtil.printMemUsage();
                    request(numRequests);
                    for (Future<Void> f : FUTURES) {
                        f.get();
                    }
                    FUTURES.clear();
                }
            } finally {
                HttpUtil.printMemUsage();
                servlet.wakeUp();
                for (Future<Void> f : stuck) {
                    f.get();
                }
            }
        } finally {
            System.err.println("Number of rejections: " + UNAVAILABLE_COUNT.get());
            System.err.println("Number of exceptions: " + EXCEPTION_COUNT.get());
            webMgr.stop();
            HttpUtil.printMemUsage();
        }
    }

    private ArrayList<Future<Void>> started() {
        ArrayList<Future<Void>> started = new ArrayList<>(FUTURES);
        FUTURES.clear();
        return started;
    }

    @Test
    public void testChattyServer() throws Exception {
        int numRequests = 48;
        int numExecutors = 24;
        int serverQueueSize = 24;
        HttpUtil.printMemUsage();
        WebManager webMgr = new WebManager();
        HttpServer server =
                new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, numExecutors, serverQueueSize);
        ChattyServlet servlet = new ChattyServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        try {
            request(numRequests);
            for (Future<Void> thread : FUTURES) {
                thread.get();
            }
            Assert.assertEquals(numRequests, SUCCESS_COUNT.get());
            Assert.assertEquals(0, UNAVAILABLE_COUNT.get());
            Assert.assertEquals(0, OTHER_COUNT.get());
        } finally {
            HttpUtil.printMemUsage();
            webMgr.stop();
            HttpUtil.printMemUsage();
        }
    }

    @Test
    public void testMalformedString() throws Exception {
        int numExecutors = 16;
        int serverQueueSize = 16;
        WebManager webMgr = new WebManager();
        HttpServer server =
                new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, numExecutors, serverQueueSize);
        SleepyServlet servlet = new SleepyServlet(server.ctx(), new String[] { PATH });
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
            Future<Void> next = executor.submit(() -> {
                try {
                    HttpUriRequest request = post(null);
                    HttpResponse response = executeHttpRequest(request);
                    if (response.getStatusLine().getStatusCode() == HttpResponseStatus.OK.code()) {
                        SUCCESS_COUNT.incrementAndGet();
                    } else if (response.getStatusLine().getStatusCode() == HttpResponseStatus.SERVICE_UNAVAILABLE
                            .code()) {
                        UNAVAILABLE_COUNT.incrementAndGet();
                    } else {
                        OTHER_COUNT.incrementAndGet();
                    }
                    InputStream in = response.getEntity().getContent();
                    if (PRINT_TO_CONSOLE) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            System.out.println(line);
                        }
                    }
                    IOUtils.closeQuietly(in);
                } catch (Throwable th) {
                    // Server closed connection before we complete writing..
                    EXCEPTION_COUNT.incrementAndGet();
                }
                return null;
            });
            FUTURES.add(next);
        }
    }

    public static HttpResponse executeHttpRequest(HttpUriRequest method) throws Exception {
        HttpClient client = HttpClients.custom().setRetryHandler(StandardHttpRequestRetryHandler.INSTANCE).build();
        try {
            return client.execute(method);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static HttpUriRequest get(String protocol, String host, int port, String path, String query)
            throws URISyntaxException {
        URI uri = new URI(protocol, null, host, port, path, query, null);
        RequestBuilder builder = RequestBuilder.get(uri);
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    protected HttpUriRequest post(String query) throws URISyntaxException {
        URI uri = new URI(PROTOCOL, null, HOST, PORT, PATH, query, null);
        RequestBuilder builder = RequestBuilder.post(uri);
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < 2046; i++) {
            str.append("This is a string statement that will be ignored");
            str.append('\n');
        }
        String statement = str.toString();
        builder.setHeader("Content-type", "application/x-www-form-urlencoded");
        builder.addParameter("statement", statement);
        builder.setEntity(new StringEntity(statement, StandardCharsets.UTF_8));
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }
}
