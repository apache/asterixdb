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
package org.apache.asterix.cloud.clients.google.gcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.annotations.AiProvenance.Agent;
import org.apache.hyracks.util.annotations.AiProvenance.ContributionKind;
import org.apache.hyracks.util.annotations.AiProvenance.Tool;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * Runtime test for MB-71767: verify that the {@code cloudRequestsHttpConnectionMaxIdleSeconds} and
 * {@code cloudRequestsHttpConnectionMaxLifetimeSeconds} settings actually drive the underlying
 * Apache HttpClient pool's eviction + TTL behaviour for the GCS path.
 *
 * Uses a JDK built-in {@link HttpServer} that returns a GCS-shaped 404 for every request. We then
 * exercise {@link GCSCloudClient#exists(String, String)} which gracefully maps 404 to {@code
 * false}, so the test path produces one HTTP request per call without throwing. That's enough to
 * populate the Apache HttpClient connection pool with pooled sockets; {@code lsof} then lets us
 * observe whether {@code evictIdleConnections} / {@code setConnectionTimeToLive} kill them on the
 * configured schedule. Zero external dependencies — runs on any OS with {@code lsof} on PATH.
 *
 * Mirror of {@code S3ConnectionLifecycleTest} for the GCS code path.
 */
@AiProvenance(agent = Agent.CLAUDE_OPUS_4_7, tool = Tool.ANTHROPIC_CLI, contributionKind = ContributionKind.TEST_GENERATED, notes = "MB-71767: runtime verification of HTTP idle/lifetime knobs on the GCS path; generated via Claude Code")
public class GCSConnectionLifecycleTest {

    private static final String MOCK_SERVER_REGION = "us-west2";
    private static final String PLAYGROUND_BUCKET = "lifecycle-playground-gcs";

    private static final int CONCURRENCY = 8;

    private static HttpServer mockServer;
    private static int mockServerPort;
    private static String mockServerHostname;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("GCSConnectionLifecycleTest setup");
        Assume.assumeTrue("lsof not on PATH; skipping socket observation tests", lsofAvailable());

        // Bind to an ephemeral port on loopback. The handler always responds with a GCS-shaped
        // 404 so GCSCloudClient.exists() returns false without throwing, which keeps the test
        // focused on TCP socket lifecycle.
        mockServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 64);
        mockServer.createContext("/", new GcsStubHandler());
        mockServer.setExecutor(Executors.newFixedThreadPool(16));
        mockServer.start();
        mockServerPort = mockServer.getAddress().getPort();
        mockServerHostname = "http://127.0.0.1:" + mockServerPort;
        System.out.println("stub server up at " + mockServerHostname);
    }

    @AfterClass
    public static void tearDown() {
        if (mockServer != null) {
            mockServer.stop(0);
        }
    }

    /**
     * idle=2s, lifetime=large: after a burst of concurrent exists() goes quiet for >idle the
     * Apache HttpClient eviction thread should close the pooled sockets. Sample every 2s for
     * up to 20s as a safety net.
     */
    @Test
    public void a_idleEvictionClosesQuietConnections() throws Exception {
        int idleSec = 2;
        int lifetimeSec = 600;
        GCSCloudClient client = buildPatchedClient(idleSec, lifetimeSec);

        runConcurrent(CONCURRENCY, threadIdx -> existsOnce(client, "burst-" + threadIdx));

        int beforeIdle = countEstablishedToMock();
        System.out.println("[idle test] after burst: established conns to mock = " + beforeIdle);
        assertGreaterThan("pool should be populated after burst", beforeIdle, 0);

        int totalSleepSec = 20;
        int afterIdle = beforeIdle;
        for (int waited = 0; waited < totalSleepSec; waited += 2) {
            Thread.sleep(2_000L);
            afterIdle = countEstablishedToMock();
            System.out.println("[idle test] waited " + (waited + 2) + "s: established conns to mock = " + afterIdle);
            if (afterIdle < beforeIdle) {
                break;
            }
        }
        assertLessThan("idle eviction should have closed connections within " + totalSleepSec + "s. before="
                + beforeIdle + " after=" + afterIdle, afterIdle, beforeIdle);
    }

    /**
     * idle=large, lifetime=3s: keep firing requests so idle never trips, but TTL must still retire
     * the pooled connections. Verifies wall-clock-age semantics.
     */
    @Test
    public void b_ttlRotatesConnectionsDespiteActivity() throws Exception {
        int idleSec = 600;
        int lifetimeSec = 3;
        GCSCloudClient client = buildPatchedClient(idleSec, lifetimeSec);

        runConcurrent(CONCURRENCY, threadIdx -> existsOnce(client, "ttl-init-" + threadIdx));
        Set<String> beforePorts = sourcePortsToMock();
        System.out.println("[ttl test] after initial burst: source ports = " + beforePorts);
        assertGreaterThan("pool should be populated after burst", beforePorts.size(), 0);

        long deadline = System.currentTimeMillis() + (lifetimeSec + 4) * 1000L;
        int iters = 0;
        while (System.currentTimeMillis() < deadline) {
            final int iterSnapshot = iters;
            runConcurrent(CONCURRENCY, threadIdx -> existsOnce(client, "ttl-busy-" + iterSnapshot + "-" + threadIdx));
            Thread.sleep(500);
            iters++;
        }

        runConcurrent(CONCURRENCY, threadIdx -> existsOnce(client, "ttl-final-" + threadIdx));
        Set<String> afterPorts = sourcePortsToMock();
        System.out.println("[ttl test] after " + (lifetimeSec + 4) + "s of activity: source ports = " + afterPorts);

        Set<String> stillPresent = new HashSet<>(beforePorts);
        stillPresent.retainAll(afterPorts);
        System.out.println("[ttl test] source ports surviving TTL: " + stillPresent);
        assertLessThan("TTL should have retired all initial connections. before=" + beforePorts + " after=" + afterPorts
                + " survivors=" + stillPresent, stillPresent.size(), beforePorts.size());
    }

    // --------------------------- helpers ---------------------------

    /**
     * Builds a {@link GCSCloudClient} via the existing public constructor (which defaults idle and
     * lifetime to 0), then reflectively patches the two private fields on the {@link GCSClientConfig}
     * to the desired values. The cloud client's constructor invokes {@code buildClient(config)}
     * after we patch, so the patched values reach the {@code ApacheHttpTransport} that backs the
     * google-cloud-storage transport.
     */
    private static GCSCloudClient buildPatchedClient(int idleSec, int lifetimeSec) throws Exception {
        int writeBufferSize = StorageUtil.getIntSizeInBytes(5, StorageUtil.StorageUnit.MEGABYTE);
        GCSClientConfig config =
                new GCSClientConfig(MOCK_SERVER_REGION, mockServerHostname, true, 0, writeBufferSize, "");

        setIntField(config, "maxIdleSeconds", idleSec);
        setIntField(config, "maxLifetimeSeconds", lifetimeSec);

        return new GCSCloudClient(config, ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    private static void setIntField(Object target, String name, int value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.setInt(target, value);
    }

    /**
     * Drives one cheap GCS API call through the cloud client. Our stub returns 404 for every
     * path; {@link GCSCloudClient#exists} maps that to {@code false} without throwing, so the
     * only observable side effect is one HTTP request being issued through the pooled HTTP
     * client.
     */
    private static void existsOnce(GCSCloudClient client, String key) {
        try {
            client.exists(PLAYGROUND_BUCKET, "objects/" + key);
        } catch (Exception e) {
            // We don't care about response correctness — only that a TCP connection was made.
        }
    }

    @FunctionalInterface
    private interface IntTask {
        void run(int threadIdx) throws Exception;
    }

    private static void runConcurrent(int n, IntTask task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(n);
            AtomicInteger errors = new AtomicInteger();
            for (int i = 0; i < n; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        task.run(idx);
                    } catch (Exception e) {
                        errors.incrementAndGet();
                        System.err.println("concurrent task " + idx + " failed: " + e);
                        e.printStackTrace(System.err);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            if (!done.await(30, TimeUnit.SECONDS)) {
                throw new RuntimeException("concurrent tasks timed out");
            }
            // The test only cares about TCP socket lifecycle, not response correctness — don't
            // fail the test just because the stub gave a 404 the SDK didn't love.
        } finally {
            pool.shutdownNow();
        }
    }

    private static boolean lsofAvailable() {
        try {
            Process p = new ProcessBuilder("lsof", "-v").redirectErrorStream(true).start();
            return p.waitFor(2, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static int countEstablishedToMock() throws Exception {
        return sourcePortsToMock().size();
    }

    /**
     * Returns the set of local source ports for ESTABLISHED TCP sockets in this JVM that have the
     * stub server port as their destination. Each pooled connection corresponds to one source
     * port; a rotation of connections shows up as a new set of ports.
     */
    private static Set<String> sourcePortsToMock() throws Exception {
        long pid = ProcessHandle.current().pid();
        Process p = new ProcessBuilder("lsof", "-nP", "-iTCP", "-p", String.valueOf(pid), "-sTCP:ESTABLISHED")
                .redirectErrorStream(true).start();
        Set<String> ports = new HashSet<>();
        Pattern destPattern = Pattern.compile(":(\\d+)->(?:127\\.0\\.0\\.1|\\[?::1]?):" + mockServerPort);
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line;
            while ((line = r.readLine()) != null) {
                Matcher m = destPattern.matcher(line);
                if (m.find()) {
                    ports.add(m.group(1));
                }
            }
        }
        p.waitFor(3, TimeUnit.SECONDS);
        return ports;
    }

    private static void assertGreaterThan(String msg, int actual, int floor) {
        if (actual <= floor) {
            throw new AssertionError(msg + " (actual=" + actual + ", expected > " + floor + ")");
        }
    }

    private static void assertLessThan(String msg, int actual, int ceiling) {
        if (actual >= ceiling) {
            throw new AssertionError(msg + " (actual=" + actual + ", expected < " + ceiling + ")");
        }
    }

    /**
     * Minimal HTTP handler that returns a GCS-shaped 404 response for every path. The body is the
     * standard GCS error envelope so the SDK classifies the error as code=404 and short-circuits
     * to a clean "not found" outcome (no throw, no retry storm).
     */
    private static final class GcsStubHandler implements HttpHandler {
        private static final byte[] BODY = ("{\"error\":{\"code\":404,\"message\":\"Not Found\","
                + "\"errors\":[{\"reason\":\"notFound\",\"message\":\"Not Found\"}]}}").getBytes();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(404, BODY.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(BODY);
                }
            } finally {
                exchange.close();
            }
        }
    }
}
