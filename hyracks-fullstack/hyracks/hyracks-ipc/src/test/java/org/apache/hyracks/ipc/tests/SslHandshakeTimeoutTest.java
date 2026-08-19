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
package org.apache.hyracks.ipc.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.hyracks.ipc.sockets.SslHandshake;
import org.apache.hyracks.ipc.sockets.SslSocketChannel;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Test;

/**
 * The IPC layer hands {@link SslHandshake} a non-blocking channel. A peer which accepts the connection
 * and then says nothing must therefore make the handshake wait, and give up at its deadline, rather than
 * retry the zero-length read in a tight loop.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "handshake deadline regression")
public class SslHandshakeTimeoutTest {

    /**
     * The bound is read once at class-init, so it cannot be set from here; surefire supplies it for this
     * module. Read it back rather than assuming, so the assertions track whatever the build configured.
     */
    private static final int TIMEOUT_SEC = Integer.getInteger(SslHandshake.HANDSHAKE_TIMEOUT_SEC_KEY, 60);

    @Test
    public void unresponsivePeerFailsAtDeadlineWithoutSpinning() throws Exception {
        assertTrue("expected " + SslHandshake.HANDSHAKE_TIMEOUT_SEC_KEY + " to be shortened for tests, but it is "
                + TIMEOUT_SEC + "s; check the surefire configuration in this module's pom", TIMEOUT_SEC <= 10);
        try (ServerSocketChannel server = ServerSocketChannel.open()) {
            server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try (SocketChannel client = SocketChannel.open(server.getLocalAddress());
                    // accepted, but deliberately never written to
                    SocketChannel accepted = server.accept()) {
                client.configureBlocking(false);
                final SSLEngine engine = SSLContext.getDefault().createSSLEngine();
                engine.setUseClientMode(true);
                final SslSocketChannel sslChannel = new SslSocketChannel(client, engine);

                final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
                final long cpuStart = threads.getCurrentThreadCpuTime();
                final long start = System.nanoTime();
                final boolean handshakeSucceeded = sslChannel.handshake();
                final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                final long cpuMillis = TimeUnit.NANOSECONDS.toMillis(threads.getCurrentThreadCpuTime() - cpuStart);

                assertFalse("handshake should fail against a peer that never replies", handshakeSucceeded);
                assertTrue("gave up after " + elapsedMillis + "ms, before the " + TIMEOUT_SEC + "s deadline",
                        elapsedMillis >= TimeUnit.SECONDS.toMillis(TIMEOUT_SEC) - 250);
                assertTrue("still waiting after " + elapsedMillis + "ms, well past the deadline",
                        elapsedMillis < TimeUnit.SECONDS.toMillis(TIMEOUT_SEC) * 4);
                // the point of the fix: the wait must be idle, not a retry loop on the non-blocking channel
                assertTrue("handshake burned " + cpuMillis + "ms of CPU over " + elapsedMillis
                        + "ms of waiting; it is spinning rather than waiting", cpuMillis < elapsedMillis / 4);
            }
        }
    }
}
