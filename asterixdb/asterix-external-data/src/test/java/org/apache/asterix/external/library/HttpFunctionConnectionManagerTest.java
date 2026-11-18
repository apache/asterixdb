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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.IHttpEgressPolicy;
import org.apache.http.HttpStatus;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
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
    private static final long STALE_AFTER_MILLIS = 200;
    private static final byte[] REQUEST_HEADERS_END = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] KEEP_ALIVE_RESPONSE =
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"
                    .getBytes(StandardCharsets.US_ASCII);
    private static final String UNREADABLE_CERTIFICATE =
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";

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
                execute(request, LONG_TIMEOUT_MILLIS, response -> null);
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
                execute(new HttpGet(url), LONG_TIMEOUT_MILLIS, response -> null);
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
        try {
            execute(request, INTERRUPT_AFTER_MILLIS, response -> null);
            fail("expected the call to time out");
        } catch (HyracksDataException e) {
            assertEquals(ErrorCode.EXTERNAL_HTTP_FUNCTION_TIMEOUT.intValue(), e.getErrorCode());
        }
        assertTrue(request.isAborted());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    public void resendsGetWhoseIdleConnectionTheServerClosed() throws Exception {
        try (ServerSocket closing = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            AtomicInteger closingConnections = new AtomicInteger();
            Thread acceptor = new Thread(() -> answerOnceThenClose(closing, closingConnections));
            acceptor.setDaemon(true);
            acceptor.start();
            String closingUrl = "http://127.0.0.1:" + closing.getLocalPort() + "/ok";
            assertEquals(HttpStatus.SC_OK, get(closingUrl));
            // well under the pool's validation interval, so that the closed connection is handed out unchecked
            Thread.sleep(STALE_AFTER_MILLIS);
            assertEquals(HttpStatus.SC_OK, get(closingUrl));
            assertEquals(2, closingConnections.get());
        }
    }

    @Test
    public void unreadableCertificateIsAnInvalidOption() {
        try {
            HttpFunctionConnectionManager.createContext(UNREADABLE_CERTIFICATE, null);
            fail("expected the certificate to be refused");
        } catch (HyracksDataException e) {
            assertEquals(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_OPTION.intValue(), e.getErrorCode());
        }
    }

    @Test
    public void sendsThroughTheProxyOfThePolicy() throws Exception {
        try (ServerSocket proxy = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            CompletableFuture<String> requestLine = new CompletableFuture<>();
            Thread acceptor = new Thread(() -> answerOnce(proxy, requestLine));
            acceptor.setDaemon(true);
            acceptor.start();
            ProxyPolicy policy = new ProxyPolicy(URI.create("http://127.0.0.1:" + proxy.getLocalPort()), null);
            // never resolved here: the proxy resolves it, and the policy refuses every address it is asked about
            String target = "http://target.invalid:8080/x";
            try (CloseableHttpClient proxied = HttpFunctionConnectionManager.createClient(policy)) {
                assertEquals(HttpStatus.SC_OK, (int) execute(proxied, new HttpGet(target), LONG_TIMEOUT_MILLIS,
                        response -> response.getStatusLine().getStatusCode()));
            }
            assertEquals("GET " + target + " HTTP/1.1", requestLine.get(MAX_CANCEL_MILLIS, TimeUnit.MILLISECONDS));
            assertEquals(List.of("target.invalid via " + policy.proxy), policy.checked);
        }
    }

    @Test
    public void tunnelsHttpsThroughTheProxyOfThePolicy() throws Exception {
        try (ServerSocket proxy = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            CompletableFuture<String> requestLine = new CompletableFuture<>();
            Thread acceptor = new Thread(() -> answerOnce(proxy, requestLine));
            acceptor.setDaemon(true);
            acceptor.start();
            ProxyPolicy policy = new ProxyPolicy(URI.create("http://127.0.0.1:" + proxy.getLocalPort()), null);
            try (CloseableHttpClient proxied = HttpFunctionConnectionManager.createClient(policy)) {
                // the stand-in proxy opens the tunnel, then closes it before TLS can start
                execute(proxied, new HttpGet("https://target.invalid/x"), LONG_TIMEOUT_MILLIS, response -> null);
                fail("expected the TLS handshake to fail");
            } catch (HyracksDataException e) {
                assertEquals(ErrorCode.EXTERNAL_HTTP_FUNCTION_REQUEST_FAILED.intValue(), e.getErrorCode());
            }
            assertEquals("CONNECT target.invalid:443 HTTP/1.1",
                    requestLine.get(MAX_CANCEL_MILLIS, TimeUnit.MILLISECONDS));
            assertEquals(List.of("target.invalid via " + policy.proxy), policy.checked);
        }
    }

    @Test
    public void refusedRequestIsNotSent() throws Exception {
        HyracksDataException refusal = new HyracksDataException("refused");
        ProxyPolicy policy = new ProxyPolicy(null, refusal);
        try (CloseableHttpClient refusing = HttpFunctionConnectionManager.createClient(policy)) {
            execute(refusing, new HttpGet(url), LONG_TIMEOUT_MILLIS, response -> null);
            fail("expected the policy to refuse the request");
        } catch (HyracksDataException e) {
            assertSame(refusal, e);
        }
        assertEquals(List.of("127.0.0.1 via null"), policy.checked);
        assertEquals(0, connections.get());
    }

    @Test
    public void refusedProxiedRequestIsNotSent() throws Exception {
        HyracksDataException refusal = new HyracksDataException("refused");
        // the hanging server stands in for the proxy
        ProxyPolicy policy = new ProxyPolicy(URI.create("http://127.0.0.1:" + server.getLocalPort()), refusal);
        try (CloseableHttpClient refusing = HttpFunctionConnectionManager.createClient(policy)) {
            execute(refusing, new HttpGet("http://target.invalid/x"), LONG_TIMEOUT_MILLIS, response -> null);
            fail("expected the policy to refuse the request");
        } catch (HyracksDataException e) {
            assertSame(refusal, e);
        }
        assertEquals(0, connections.get());
    }

    private int get(String url) throws HyracksDataException {
        return execute(new HttpGet(url), LONG_TIMEOUT_MILLIS, response -> response.getStatusLine().getStatusCode());
    }

    private <T> T execute(HttpGet request, long timeoutMillis, ResponseHandler<T> handler) throws HyracksDataException {
        return execute(client, request, timeoutMillis, handler);
    }

    private static <T> T execute(CloseableHttpClient client, HttpGet request, long timeoutMillis,
            ResponseHandler<T> handler) throws HyracksDataException {
        return HttpFunctionConnectionManager.execute(client, request, HttpClientContext.create(),
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis), handler, null,
                request.getURI().toString(), (int) TimeUnit.MILLISECONDS.toSeconds(timeoutMillis));
    }

    // answers one request on each connection, announcing that it keeps it open, then closes it without a word, as
    // a server whose keep-alive timeout has passed does
    private static void answerOnceThenClose(ServerSocket server, AtomicInteger connections) {
        while (!server.isClosed()) {
            try (Socket socket = server.accept()) {
                connections.incrementAndGet();
                InputStream in = socket.getInputStream();
                int matched = 0;
                while (matched < REQUEST_HEADERS_END.length) {
                    int b = in.read();
                    if (b < 0) {
                        break;
                    }
                    matched = b == REQUEST_HEADERS_END[matched] ? matched + 1 : (b == REQUEST_HEADERS_END[0] ? 1 : 0);
                }
                socket.getOutputStream().write(KEEP_ALIVE_RESPONSE);
                socket.getOutputStream().flush();
            } catch (IOException e) {
                return;
            }
        }
    }

    // answers the first request of the first connection, handing over its request line
    private static void answerOnce(ServerSocket server, CompletableFuture<String> requestLine) {
        try (Socket socket = server.accept()) {
            InputStream in = socket.getInputStream();
            StringBuilder head = new StringBuilder();
            while (head.indexOf("\r\n\r\n") < 0) {
                int b = in.read();
                if (b < 0) {
                    break;
                }
                head.append((char) b);
            }
            requestLine.complete(head.substring(0, Math.max(0, head.indexOf("\r\n"))));
            socket.getOutputStream().write(KEEP_ALIVE_RESPONSE);
            socket.getOutputStream().flush();
        } catch (IOException e) {
            requestLine.completeExceptionally(e);
        }
    }

    // sends through a proxy, records each request it is asked about, and refuses every address it is asked about
    private static final class ProxyPolicy implements IHttpEgressPolicy {

        private final URI proxy;
        private final HyracksDataException refusal;
        private final List<String> checked = new CopyOnWriteArrayList<>();

        private ProxyPolicy(URI proxy, HyracksDataException refusal) {
            this.proxy = proxy;
            this.refusal = refusal;
        }

        @Override
        public void checkAddresses(String host, InetAddress[] addresses) throws HyracksDataException {
            throw new HyracksDataException("no direct connection to " + host);
        }

        @Override
        public URI getProxy() {
            return proxy;
        }

        @Override
        public void checkRequest(String host, URI proxy) throws HyracksDataException {
            checked.add(host + " via " + proxy);
            if (refusal != null) {
                throw refusal;
            }
        }
    }
}
