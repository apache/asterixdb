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
package org.apache.hyracks.net.protocols.muxdemux;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class MuxDemuxIncomingConnectionTest {

    private static final long TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "MuxDemuxIncomingConnectionTest Handshake thread");
        t.setDaemon(true);
        return t;
    });

    @After
    public void shutdownExecutor() {
        executor.shutdownNow();
    }

    /**
     * An accepted connection must be registered under the MuxDemux monitor, as every other access to the
     * connection maps is; otherwise getState() can iterate the incoming map while it is being modified.
     */
    @Test
    public void testAcceptedConnectionRegisteredUnderLock() throws Exception {
        MuxDemux server = createMuxDemux();
        server.start();
        MuxDemux client = createMuxDemux();
        client.start();
        Map<InetSocketAddress, MultiplexedConnection> incoming = getIncomingConnectionMap(server);

        synchronized (server) {
            client.connect(server.getLocalAddress());
            long deadline = System.nanoTime() + TIMEOUT_NANOS;
            // wait for the server's IO thread to reach the registration; with the lock held it must block on the
            // monitor rather than modify the map underneath us
            while (!isThreadBlockedOn(server)) {
                Assert.assertTrue("accepted connection was registered without holding the MuxDemux lock",
                        incoming.isEmpty());
                Assert.assertTrue("timed out waiting for the server to accept the connection",
                        System.nanoTime() < deadline);
                Thread.sleep(10);
            }
            Assert.assertTrue(incoming.isEmpty());
        }

        long deadline = System.nanoTime() + TIMEOUT_NANOS;
        JsonNode state = server.getState();
        while (state.get("incomingConnections").size() == 0) {
            Assert.assertTrue("timed out waiting for the incoming connection to be registered",
                    System.nanoTime() < deadline);
            Thread.sleep(10);
            state = server.getState();
        }
        Assert.assertEquals(1, state.get("incomingConnections").size());
    }

    private static boolean isThreadBlockedOn(Object monitor) {
        int identity = System.identityHashCode(monitor);
        for (ThreadInfo info : ManagementFactory.getThreadMXBean().dumpAllThreads(false, false)) {
            LockInfo lock = info.getLockInfo();
            if (info.getThreadState() == Thread.State.BLOCKED && lock != null
                    && lock.getClassName().equals(monitor.getClass().getName())
                    && lock.getIdentityHashCode() == identity) {
                return true;
            }
        }
        return false;
    }

    private MuxDemux createMuxDemux() {
        return new MuxDemux(new InetSocketAddress("127.0.0.1", 0), channel -> {
            // This test only exercises connection registration.
        }, 1, 5, FullFrameChannelInterfaceFactory.INSTANCE, PlainSocketChannelFactory.INSTANCE, executor);
    }

    @SuppressWarnings("unchecked")
    private static Map<InetSocketAddress, MultiplexedConnection> getIncomingConnectionMap(MuxDemux muxDemux)
            throws Exception {
        Field field = MuxDemux.class.getDeclaredField("incomingConnectionMap");
        field.setAccessible(true);
        return (Map<InetSocketAddress, MultiplexedConnection>) field.get(muxDemux);
    }
}
