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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.HttpServerConfig;
import org.apache.hyracks.http.server.HttpServerConfigBuilder;
import org.apache.hyracks.http.server.InterruptOnCloseHandler;
import org.apache.hyracks.http.server.WebManager;
import org.apache.hyracks.test.http.servlet.ChattyServlet;
import org.apache.hyracks.test.http.servlet.EchoServlet;
import org.apache.hyracks.test.http.servlet.SleepyServlet;
import org.apache.hyracks.util.StorageUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpServerTest {
    private static final Logger LOGGER = LogManager.getLogger();
    static final boolean PRINT_TO_CONSOLE = false;
    static final int PORT = 9898;
    static final String HOST = "localhost";
    static final String PROTOCOL = "http";
    static final String PATH = "/";
    static final AtomicInteger SUCCESS_COUNT = new AtomicInteger();
    static final AtomicInteger UNAVAILABLE_COUNT = new AtomicInteger();
    static final AtomicInteger OTHER_COUNT = new AtomicInteger();
    static final AtomicInteger EXCEPTION_COUNT = new AtomicInteger();
    static final List<HttpRequestTask> TASKS = new ArrayList<>();
    static final List<Future<Void>> FUTURES = new ArrayList<>();
    static final ExecutorService executor = Executors.newCachedThreadPool();

    @Before
    public void setUp() {
        SUCCESS_COUNT.set(0);
        UNAVAILABLE_COUNT.set(0);
        OTHER_COUNT.set(0);
        EXCEPTION_COUNT.set(0);
        FUTURES.clear();
        TASKS.clear();
    }

    @Test
    public void testOverloadingServer() throws Exception {
        WebManager webMgr = new WebManager();
        int numExecutors = 16;
        int serverQueueSize = 16;
        int numRequests = 128;
        final HttpServerConfig config = HttpServerConfigBuilder.custom().setThreadCount(numExecutors)
                .setRequestQueueSize(serverQueueSize).build();
        HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, config);
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
        final HttpServerConfig config = HttpServerConfigBuilder.custom().setThreadCount(numExecutors)
                .setRequestQueueSize(serverQueueSize).build();
        HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, config);
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
                    HttpTestUtil.printMemUsage();
                    request(numRequests);
                    for (Future<Void> f : FUTURES) {
                        f.get();
                    }
                    FUTURES.clear();
                }
            } finally {
                HttpTestUtil.printMemUsage();
                servlet.wakeUp();
                for (Future<Void> f : stuck) {
                    f.get();
                }
            }
        } finally {
            System.err.println("Number of rejections: " + UNAVAILABLE_COUNT.get());
            System.err.println("Number of exceptions: " + EXCEPTION_COUNT.get());
            webMgr.stop();
            HttpTestUtil.printMemUsage();
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
        HttpTestUtil.printMemUsage();
        WebManager webMgr = new WebManager();
        final HttpServerConfig config = HttpServerConfigBuilder.custom().setThreadCount(numExecutors)
                .setRequestQueueSize(serverQueueSize).build();
        HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, config);
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
            HttpTestUtil.printMemUsage();
            webMgr.stop();
            HttpTestUtil.printMemUsage();
        }
    }

    @Test
    public void testMalformedString() throws Exception {
        int numExecutors = 16;
        int serverQueueSize = 16;
        WebManager webMgr = new WebManager();
        final HttpServerConfig config = HttpServerConfigBuilder.custom().setThreadCount(numExecutors)
                .setRequestQueueSize(serverQueueSize).build();
        HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, config);
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
                while ((line = br.readLine()) != null && !line.isEmpty()) {
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

    @Test
    public void testServerRevival() throws Exception {
        int numExecutors = 16;
        int serverQueueSize = 16;
        int numRequests = 1;
        WebManager webMgr = new WebManager();
        final HttpServerConfig config = HttpServerConfigBuilder.custom().setThreadCount(numExecutors)
                .setRequestQueueSize(serverQueueSize).build();
        HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, config);
        ChattyServlet servlet = new ChattyServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        try {
            // send a request
            request(numRequests);
            for (Future<Void> thread : FUTURES) {
                thread.get();
            }
            Assert.assertEquals(numRequests, SUCCESS_COUNT.get());
            // close the channel
            Field channelField = server.getClass().getDeclaredField("channel");
            channelField.setAccessible(true);
            Field recoveryThreadField = server.getClass().getDeclaredField("recoveryThread");
            recoveryThreadField.setAccessible(true);
            Channel channel = (Channel) channelField.get(server);
            channel.close();
            Thread.sleep(1000);
            final int sleeps = 10;
            for (int i = 0; i < sleeps; i++) {
                Thread thread = (Thread) recoveryThreadField.get(server);
                if (thread == null) {
                    break;
                }
                LOGGER.log(Level.WARN,
                        "Attempt #" + (i + 1) + ". Recovery thread is not null and has id " + thread.getId());
                if (i == sleeps - 1) {
                    throw new Exception("Http server recovery didn't complete after " + sleeps + "s");
                }
                Thread.sleep(1000);
            }
            for (int i = 0; i < sleeps; i++) {
                request(1);
                for (Future<Void> thread : FUTURES) {
                    thread.get();
                }
                if (numRequests + 1 == SUCCESS_COUNT.get()) {
                    break;
                } else if (i == sleeps - 1) {
                    throw new Exception(
                            "Http server couldn't process requests correctly after recovery for " + sleeps + "s");
                }
            }
        } finally {
            webMgr.stop();
        }
    }

    @Test
    public void testInterruptOnClientClose() throws Exception {
        WebManager webMgr = new WebManager();
        int numExecutors = 1;
        int queueSize = 1;
        final HttpServerConfig config =
                HttpServerConfigBuilder.custom().setThreadCount(numExecutors).setRequestQueueSize(queueSize).build();
        HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), new InetSocketAddress(PORT), config,
                InterruptOnCloseHandler.INSTANCE);
        SleepyServlet servlet = new SleepyServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        try {
            request(1);
            synchronized (servlet) {
                while (servlet.getNumSlept() == 0) {
                    servlet.wait();
                }
            }
            request(1);
            waitTillQueued(server, 1);
            FUTURES.remove(0);
            HttpRequestTask request = TASKS.remove(0);
            request.request.abort();
            waitTillQueued(server, 0);
            synchronized (servlet) {
                while (servlet.getNumSlept() == 1) {
                    servlet.wait();
                }
            }
            servlet.wakeUp();
            for (Future<Void> f : FUTURES) {
                f.get();
            }
            FUTURES.clear();
        } finally {
            webMgr.stop();
        }
    }

    @Test
    public void testLargeRequest() throws Exception {
        WebManager webMgr = new WebManager();
        // Server with max allowed request size = 512K
        final int maxRequestSize = StorageUtil.getIntSizeInBytes(512, StorageUtil.StorageUnit.KILOBYTE);
        final HttpServerConfig config = HttpServerConfigBuilder.custom().setMaxRequestSize(maxRequestSize).build();
        HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, config);
        ChattyServlet servlet = new ChattyServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        Exception failure = null;
        try {
            request(1, maxRequestSize + 1);
            for (Future<Void> thread : FUTURES) {
                thread.get();
            }
        } catch (Exception e) {
            failure = e;
        } finally {
            webMgr.stop();
        }
        Assert.assertNotNull(failure);
    }

    @Test
    public void chunkedRequestTest() throws Exception {
        final WebManager webMgr = new WebManager();
        final int serverRequestChunkSize = StorageUtil.getIntSizeInBytes(1, StorageUtil.StorageUnit.KILOBYTE);
        final HttpServerConfig config = HttpServerConfigBuilder.custom().setThreadCount(16).setRequestQueueSize(16)
                .setMaxRequestChunkSize(serverRequestChunkSize).build();
        final HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), PORT, config);
        EchoServlet servlet = new EchoServlet(server.ctx(), PATH);
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            final URI uri = new URI(HttpServerTest.PROTOCOL, null, HttpServerTest.HOST, HttpServerTest.PORT,
                    HttpServerTest.PATH, null, null);
            final HttpPost postRequest = new HttpPost(uri);
            final int requestSize = StorageUtil.getIntSizeInBytes(8, StorageUtil.StorageUnit.KILOBYTE);
            final String requestBody = RandomStringUtils.randomAlphanumeric(requestSize);
            final StringEntity chunkedEntity = new StringEntity(requestBody);
            chunkedEntity.setChunked(true);
            postRequest.setEntity(chunkedEntity);
            try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
                final String responseBody = EntityUtils.toString(response.getEntity());
                Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpResponseStatus.OK.code());
                Assert.assertEquals(responseBody, requestBody);
            }
        } finally {
            webMgr.stop();
        }
    }

    private void request(int count) throws URISyntaxException {
        request(count, 0);
    }

    private void request(int count, int entitySize) throws URISyntaxException {
        for (int i = 0; i < count; i++) {
            HttpRequestTask requestTask = new HttpRequestTask(entitySize);
            Future<Void> next = executor.submit(requestTask);
            FUTURES.add(next);
            TASKS.add(requestTask);
        }
    }
}
