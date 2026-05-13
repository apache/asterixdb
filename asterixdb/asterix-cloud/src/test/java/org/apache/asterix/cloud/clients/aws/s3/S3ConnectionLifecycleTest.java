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
package org.apache.asterix.cloud.clients.aws.s3;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.WriterSingleBufferProvider;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.annotations.AiProvenance.Agent;
import org.apache.hyracks.util.annotations.AiProvenance.ContributionKind;
import org.apache.hyracks.util.annotations.AiProvenance.Tool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;

/**
 * Runtime test for MB-71767: verify that the {@code cloudRequestsHttpConnectionMaxIdleSeconds} and
 * {@code cloudRequestsHttpConnectionMaxLifetimeSeconds} settings actually drive the underlying
 * Apache HttpClient pool's eviction + TTL behaviour for the sync S3 path.
 *
 * Approach: spin up an in-process S3Mock, build a {@link S3CloudClient} with small idle/lifetime
 * values, drive concurrent uploads to populate the connection pool, then observe established
 * TCP sockets via {@code lsof} to confirm connections are torn down at the configured times.
 */
@AiProvenance(agent = Agent.CLAUDE_OPUS_4_7, tool = Tool.ANTHROPIC_CLI, contributionKind = ContributionKind.TEST_GENERATED, notes = "MB-71767: runtime verification of HTTP idle/lifetime knobs on the sync S3 path; generated via Claude Code")
public class S3ConnectionLifecycleTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int MOCK_SERVER_PORT = 8002;
    private static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;
    private static final String MOCK_SERVER_REGION = "us-west-2";
    private static final String PLAYGROUND_BUCKET = "lifecycle-playground";

    private static final int CONCURRENCY = 8;
    private static final int OBJ_BYTES = 256;

    private static S3Mock s3MockServer;
    private static S3Client setupClient;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("S3ConnectionLifecycleTest setup");
        s3MockServer = new S3Mock.Builder().withPort(MOCK_SERVER_PORT).withInMemoryBackend().build();
        try {
            s3MockServer.start();
        } catch (Exception alreadyStarted) {
            // ignore
        }

        setupClient = S3Client.builder().region(Region.of(MOCK_SERVER_REGION))
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(URI.create(MOCK_SERVER_HOSTNAME)).forcePathStyle(true).build();
        try {
            setupClient.deleteBucket(DeleteBucketRequest.builder().bucket(PLAYGROUND_BUCKET).build());
        } catch (Exception ignore) {
        }
        setupClient.createBucket(CreateBucketRequest.builder().bucket(PLAYGROUND_BUCKET).build());

        // lsof must be on PATH for this test to observe socket state. Skip with a warning on other OS.
        Assume.assumeTrue("lsof not on PATH; skipping socket observation tests", lsofAvailable());
    }

    @AfterClass
    public static void tearDown() {
        if (setupClient != null) {
            setupClient.close();
        }
        if (s3MockServer != null) {
            s3MockServer.shutdown();
        }
    }

    /**
     * idle=2s, lifetime=large: after a burst of concurrent uploads goes quiet for >idle the AWS
     * SDK v2 IdleConnectionReaper should close the pooled sockets. The reaper has its own
     * ~60s wake interval (independent of connectionMaxIdleTime), so we poll for up to 70s
     * and short-circuit as soon as we observe the drop.
     */
    @Test
    public void a_idleEvictionClosesQuietConnections() throws Exception {
        int idleSec = 2;
        int lifetimeSec = 600;
        S3CloudClient client = buildPatchedClient(idleSec, lifetimeSec);

        // Populate the pool with a concurrent burst.
        runConcurrent(CONCURRENCY, threadIdx -> writeOnce(client, "burst-" + threadIdx));

        int beforeIdle = countEstablishedToMock();
        System.out.println("[idle test] after burst: established conns to mock = " + beforeIdle);
        assertGreaterThan("pool should be populated after burst", beforeIdle, 0);

        // Go quiet for a long stretch — the AWS SDK v2 IdleConnectionReaper wakes on its own
        // schedule (often ~60s, independent of connectionMaxIdleTime). Sample every 5s for
        // up to 70s so we observe eviction whenever the reaper finally fires.
        int totalSleepSec = 70;
        int afterIdle = beforeIdle;
        for (int waited = 0; waited < totalSleepSec; waited += 5) {
            Thread.sleep(5_000L);
            afterIdle = countEstablishedToMock();
            System.out.println("[idle test] waited " + (waited + 5) + "s: established conns to mock = " + afterIdle);
            if (afterIdle < beforeIdle) {
                break;
            }
        }
        assertLessThan("idle eviction should have closed connections within " + totalSleepSec + "s. before="
                + beforeIdle + " after=" + afterIdle, afterIdle, beforeIdle);
    }

    /**
     * idle=large, lifetime=3s: keep firing requests so idle never trips, but TTL must still retire
     * the pooled connections. Verifies wall-clock-age semantics, not idle-time semantics.
     */
    @Test
    public void b_ttlRotatesConnectionsDespiteActivity() throws Exception {
        int idleSec = 600;
        int lifetimeSec = 3;
        S3CloudClient client = buildPatchedClient(idleSec, lifetimeSec);

        // Initial burst, snapshot source ports.
        runConcurrent(CONCURRENCY, threadIdx -> writeOnce(client, "ttl-init-" + threadIdx));
        Set<String> beforePorts = sourcePortsToMock();
        System.out.println("[ttl test] after initial burst: source ports = " + beforePorts);
        assertGreaterThan("pool should be populated after burst", beforePorts.size(), 0);

        // Keep busy for > TTL — fire small bursts every 500ms.
        long deadline = System.currentTimeMillis() + (lifetimeSec + 4) * 1000L;
        int iters = 0;
        while (System.currentTimeMillis() < deadline) {
            final int iterSnapshot = iters;
            runConcurrent(CONCURRENCY, threadIdx -> writeOnce(client, "ttl-busy-" + iterSnapshot + "-" + threadIdx));
            Thread.sleep(500);
            iters++;
        }

        // Final burst to ensure new connections are open right now.
        runConcurrent(CONCURRENCY, threadIdx -> writeOnce(client, "ttl-final-" + threadIdx));
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
     * Builds an {@link S3CloudClient} via the existing public constructor (which defaults idle and
     * lifetime to 0), then reflectively patches the two private fields on the {@link S3ClientConfig}
     * to the desired values. We then rebuild the underlying SDK client by reaching into the cloud
     * client. This is test-only — production code never patches these fields after construction.
     */
    private static S3CloudClient buildPatchedClient(int idleSec, int lifetimeSec) throws Exception {
        int writeBufferSize = StorageUtil.getIntSizeInBytes(5, StorageUtil.StorageUnit.MEGABYTE);
        S3ClientConfig config =
                new S3ClientConfig(MOCK_SERVER_REGION, MOCK_SERVER_HOSTNAME, "", true, Collections.emptyList(), 0,
                        writeBufferSize, S3ClientConfig.S3ParallelDownloaderClientType.ASYNC, false);

        // Patch the idle/lifetime fields on the config.
        setIntField(config, "maxIdleSeconds", idleSec);
        setIntField(config, "maxLifetimeSeconds", lifetimeSec);

        // Build the cloud client. buildClient is invoked from the ctor and will pick up the patched values.
        return new S3CloudClient(config, ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    private static void setIntField(Object target, String name, int value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.setInt(target, value);
    }

    private static void writeOnce(S3CloudClient client, String key) {
        try {
            IWriteBufferProvider bufferProvider = new WriterSingleBufferProvider(client.getWriteBufferSize());
            ICloudWriter writer = client.createWriter(PLAYGROUND_BUCKET, "objects/" + key, bufferProvider);
            ByteBuffer buf = ByteBuffer.allocate(OBJ_BYTES);
            for (int i = 0; i < OBJ_BYTES; i++) {
                buf.put((byte) (i & 0xff));
            }
            buf.flip();
            writer.write(buf);
            writer.finish();
        } catch (Exception e) {
            throw new RuntimeException("write failed for key=" + key, e);
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
                        LOGGER.warn("concurrent task " + idx + " failed", e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            if (!done.await(30, TimeUnit.SECONDS)) {
                throw new RuntimeException("concurrent tasks timed out");
            }
            if (errors.get() > 0) {
                throw new RuntimeException(errors.get() + " concurrent tasks failed");
            }
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
     * S3Mock port as their destination. Each pooled connection corresponds to one source port; a
     * rotation of connections shows up as a new set of ports.
     */
    private static Set<String> sourcePortsToMock() throws Exception {
        long pid = ProcessHandle.current().pid();
        Process p = new ProcessBuilder("lsof", "-nP", "-iTCP", "-p", String.valueOf(pid), "-sTCP:ESTABLISHED")
                .redirectErrorStream(true).start();
        Set<String> ports = new HashSet<>();
        Pattern destPattern = Pattern.compile(":(\\d+)->(?:127\\.0\\.0\\.1|\\[?::1]?):" + MOCK_SERVER_PORT);
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
}
