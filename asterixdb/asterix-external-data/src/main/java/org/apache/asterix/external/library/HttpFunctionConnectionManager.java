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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.functions.IHttpEgressPolicy;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Creates the HTTP client that the external HTTP functions of a node share, and bounds each of their calls.
 */
public final class HttpFunctionConnectionManager {

    private static final String SCHEME_HTTP = "http";
    private static final String SCHEME_HTTPS = "https";
    private static final int MAX_CONNECTIONS = 256;
    private static final int MAX_CONNECTIONS_PER_ROUTE = 64;
    // bounds how long a connection opened to an address the egress policy allowed keeps being reused
    private static final long CONNECTION_TIME_TO_LIVE_SECONDS = 60;
    private static final long MAX_IDLE_SECONDS = 60;
    // how often a call is checked for its deadline and for an interrupt: bounds how late either aborts it
    private static final long WATCH_INTERVAL_MILLIS = 100;
    private static final String DEADLINE_THREAD_NAME = "external-http-function-deadline";
    private static final ScheduledThreadPoolExecutor DEADLINE_TIMER = createDeadlineTimer();

    private HttpFunctionConnectionManager() {
    }

    /**
     * Creates a pooled client that connects only to addresses {@code policy} allows, follows no redirect, keeps no
     * cookie, retries nothing and ignores the JVM's proxy settings.
     *
     * @param policy the policy every address a host name resolves to is checked against, before a connection is
     *        opened to it
     */
    public static CloseableHttpClient createClient(IHttpEgressPolicy policy) {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
                RegistryBuilder.<ConnectionSocketFactory> create()
                        .register(SCHEME_HTTP, PlainConnectionSocketFactory.getSocketFactory())
                        .register(SCHEME_HTTPS, SSLConnectionSocketFactory.getSocketFactory()).build(),
                null, null, new PolicyDnsResolver(policy), CONNECTION_TIME_TO_LIVE_SECONDS, TimeUnit.SECONDS);
        connectionManager.setMaxTotal(MAX_CONNECTIONS);
        connectionManager.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);
        return HttpClients.custom().setConnectionManager(connectionManager).disableRedirectHandling()
                .disableCookieManagement().disableAutomaticRetries()
                .evictIdleConnections(MAX_IDLE_SECONDS, TimeUnit.SECONDS).build();
    }

    /**
     * Sends a request and hands its response to {@code handler}, aborting the request if the whole call, reading
     * the response included, takes longer than {@code timeoutMillis}, or if the calling thread is interrupted, as
     * a cancelled query's are. A socket timeout alone cannot bound a server that keeps sending a byte at a time, and
     * a blocked socket does not notice an interrupt.
     *
     * @return what {@code handler} returns
     * @throws HyracksDataException if the client's egress policy refused the host, {@code handler} threw it, or the
     *         calling thread was interrupted, in which case it is caused by an {@link InterruptedException}
     * @throws IOException if the request failed. {@link HttpRequestBase#isAborted()} tells whether it was because
     *         the call ran out of time.
     */
    static <T> T execute(CloseableHttpClient client, HttpRequestBase request, long timeoutMillis,
            ResponseHandler<? extends T> handler) throws IOException {
        Thread caller = Thread.currentThread();
        if (caller.isInterrupted()) {
            throw HyracksDataException.create(new InterruptedException());
        }
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        ScheduledFuture<?> watch = DEADLINE_TIMER.scheduleWithFixedDelay(() -> {
            if (caller.isInterrupted() || System.nanoTime() - deadlineNanos >= 0) {
                request.abort();
            }
        }, WATCH_INTERVAL_MILLIS, WATCH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        try {
            return client.execute(request, handler);
        } catch (EgressRefusedException e) {
            throw e.getRefusal();
        } catch (IOException e) {
            if (caller.isInterrupted()) {
                // the interrupt stays set, so that the job is reported as cancelled rather than as failed
                InterruptedException interrupted = new InterruptedException();
                interrupted.addSuppressed(e);
                throw HyracksDataException.create(interrupted);
            }
            throw e;
        } finally {
            watch.cancel(false);
        }
    }

    private static ScheduledThreadPoolExecutor createDeadlineTimer() {
        ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, DEADLINE_THREAD_NAME);
            thread.setDaemon(true);
            return thread;
        });
        // nearly every call ends before its deadline, whose task would otherwise stay queued until it expires
        timer.setRemoveOnCancelPolicy(true);
        return timer;
    }

    private static final class PolicyDnsResolver implements DnsResolver {

        private final IHttpEgressPolicy policy;

        private PolicyDnsResolver(IHttpEgressPolicy policy) {
            this.policy = policy;
        }

        @Override
        public InetAddress[] resolve(String host) throws UnknownHostException {
            InetAddress[] addresses = SystemDefaultDnsResolver.INSTANCE.resolve(host);
            try {
                policy.checkAddresses(host, addresses);
            } catch (HyracksDataException e) {
                throw new EgressRefusedException(host, e);
            }
            return addresses;
        }
    }

    /**
     * Carries a refusal of the egress policy out of the client, whose resolver may throw only
     * {@link UnknownHostException}.
     */
    private static final class EgressRefusedException extends UnknownHostException {

        private static final long serialVersionUID = 1L;

        private EgressRefusedException(String host, HyracksDataException refusal) {
            super(host);
            initCause(refusal);
        }

        private HyracksDataException getRefusal() {
            return (HyracksDataException) getCause();
        }
    }
}
