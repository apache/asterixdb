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
package org.apache.asterix.external.library;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.functions.IHttpEgressPolicy;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HttpFunctionConnectionManagerTest {

    private static final long LONG_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final long INTERRUPT_AFTER_MILLIS = 300;
    // well under the timeout: the call ended because of the interrupt, not because it ran out of time
    private static final long MAX_CANCEL_MILLIS = TimeUnit.SECONDS.toMillis(5);

    // accepts connections and never answers, as an endpoint that hangs does
    private ServerSocket server;
    private final List<Socket> accepted = new ArrayList<>();
    private final AtomicInteger connections = new AtomicInteger();
    private CloseableHttpClient client;
    private String url;

    @Before
    public void setUp() throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread acceptor = new Thread(() -> {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    synchronized (accepted) {
                        accepted.add(socket);
                    }
                    connections.incrementAndGet();
                } catch (IOException e) {
                    return;
                }
            }
        });
        acceptor.setDaemon(true);
        acceptor.start();
        client = HttpFunctionConnectionManager.createClient(IHttpEgressPolicy.ALLOW_ALL);
        url = "http://127.0.0.1:" + server.getLocalPort() + "/hang";
    }

    @After
    public void tearDown() throws IOException {
        client.close();
        server.close();
        synchronized (accepted) {
            for (Socket socket : accepted) {
                socket.close();
            }
        }
    }

    @Test
    public void interruptAbortsCallBlockedOnResponse() throws Exception {
        HttpGet request = new HttpGet(url);
        AtomicBoolean stillInterrupted = new AtomicBoolean();
        CompletableFuture<Throwable> failure = new CompletableFuture<>();
        Thread caller = new Thread(() -> {
            try {
                HttpFunctionConnectionManager.execute(client, request, LONG_TIMEOUT_MILLIS, response -> null);
                failure.complete(null);
            } catch (Throwable e) {
                stillInterrupted.set(Thread.currentThread().isInterrupted());
                failure.complete(e);
            }
        });
        long start = System.nanoTime();
        caller.start();
        Thread.sleep(INTERRUPT_AFTER_MILLIS);
        caller.interrupt();
        Throwable e = failure.get(MAX_CANCEL_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < MAX_CANCEL_MILLIS);
        assertTrue(String.valueOf(e), e instanceof HyracksDataException);
        assertTrue(ExceptionUtils.causedByInterrupt(e, true));
        assertTrue(stillInterrupted.get());
        assertTrue(request.isAborted());
    }

    @Test
    public void interruptedCallerSendsNothing() throws Exception {
        CompletableFuture<Throwable> failure = new CompletableFuture<>();
        Thread caller = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                HttpFunctionConnectionManager.execute(client, new HttpGet(url), LONG_TIMEOUT_MILLIS, response -> null);
                failure.complete(null);
            } catch (Throwable e) {
                failure.complete(e);
            }
        });
        caller.start();
        Throwable e = failure.get(MAX_CANCEL_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(ExceptionUtils.causedByInterrupt(e, true));
        assertEquals(0, connections.get());
    }

    @Test
    public void deadlineStillAbortsCall() throws Exception {
        HttpGet request = new HttpGet(url);
        Throwable e = null;
        try {
            HttpFunctionConnectionManager.execute(client, request, INTERRUPT_AFTER_MILLIS, response -> null);
        } catch (IOException thrown) {
            e = thrown;
        }
        assertTrue(String.valueOf(e), e != null && !(e instanceof HyracksDataException));
        assertTrue(request.isAborted());
        assertFalse(Thread.currentThread().isInterrupted());
    }
}
