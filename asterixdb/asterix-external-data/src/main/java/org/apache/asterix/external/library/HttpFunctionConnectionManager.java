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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.HttpFunctionConstants;
import org.apache.asterix.common.functions.IHttpEgressPolicy;
import org.apache.asterix.external.util.TlsUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.routing.RouteInfo;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.protocol.HttpContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

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
    // a server may close an idle connection just after the pool has checked it and handed it out
    private static final int STALE_CONNECTION_RETRIES = 1;
    // how often a call is checked for its deadline and for an interrupt: bounds how late either aborts it
    private static final long WATCH_INTERVAL_MILLIS = 100;
    private static final String DEADLINE_THREAD_NAME = "external-http-function-deadline";
    private static final String SOCKET_FACTORY_ATTRIBUTE = "external-http-function.socket-factory";
    private static final String TLS_PROTOCOL = "TLS";
    private static final ScheduledThreadPoolExecutor DEADLINE_TIMER = createDeadlineTimer();

    private HttpFunctionConnectionManager() {
    }

    /**
     * Creates a pooled client that sends only the requests {@code policy} allows, directly or through the proxy it
     * names, connects directly only to addresses it allows, follows no redirect, keeps no cookie and ignores the
     * JVM's proxy settings. It resends a request once when its pooled connection turns out to be closed, unless the
     * request has a body that may have been sent, and never after a timeout, an abort, or a failure to resolve the
     * host, connect to it or set up TLS.
     *
     * @param policy the policy each request is checked against before it is sent, and every address a host name
     *        resolves to before a connection is opened directly to it
     */
    public static CloseableHttpClient createClient(IHttpEgressPolicy policy) {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
                RegistryBuilder.<ConnectionSocketFactory> create()
                        .register(SCHEME_HTTP, PlainConnectionSocketFactory.getSocketFactory())
                        .register(SCHEME_HTTPS, new ContextTrustSocketFactory()).build(),
                null, null, new PolicyDnsResolver(policy), CONNECTION_TIME_TO_LIVE_SECONDS, TimeUnit.SECONDS);
        connectionManager.setMaxTotal(MAX_CONNECTIONS);
        connectionManager.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);
        return HttpClients.custom().setConnectionManager(connectionManager)
                .setRoutePlanner(new PolicyRoutePlanner(policy)).disableRedirectHandling().disableCookieManagement()
                .setRetryHandler(new DefaultHttpRequestRetryHandler(STALE_CONNECTION_RETRIES, false))
                .evictIdleConnections(MAX_IDLE_SECONDS, TimeUnit.SECONDS).build();
    }

    /**
     * Sends a request and hands its response to {@code handler}, aborting the request if it has not ended, reading
     * the response included, by {@code deadlineNanos}, or if the calling thread is interrupted, as a cancelled
     * query's are. A socket timeout alone cannot bound a server that keeps sending a byte at a time, and a blocked
     * socket does not notice an interrupt.
     *
     * @param context a context from {@link #createContext(String, SourceLocation)}
     * @param deadlineNanos the {@link System#nanoTime()} by which the call that sends the request must end
     * @param displayUrl the request's URL as errors show it
     * @param timeoutSeconds the timeout of the call, as errors show it
     * @return what {@code handler} returns
     * @throws HyracksDataException if the client's egress policy refused the host, {@code handler} threw it, the
     *         deadline passed, the request failed, or the calling thread was interrupted, in which case it is caused
     *         by an {@link InterruptedException}
     */
    static <T> T execute(CloseableHttpClient client, HttpRequestBase request, HttpContext context, long deadlineNanos,
            ResponseHandler<? extends T> handler, SourceLocation sourceLoc, String displayUrl, int timeoutSeconds)
            throws HyracksDataException {
        Thread caller = Thread.currentThread();
        if (caller.isInterrupted()) {
            throw HyracksDataException.create(new InterruptedException());
        }
        ScheduledFuture<?> watch = DEADLINE_TIMER.scheduleWithFixedDelay(() -> {
            if (caller.isInterrupted() || System.nanoTime() - deadlineNanos >= 0) {
                request.abort();
            }
        }, WATCH_INTERVAL_MILLIS, WATCH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        try {
            return client.execute(request, handler, context);
        } catch (EgressRefusedException e) {
            throw e.getRefusal();
        } catch (HyracksDataException e) {
            // an error the response raised, already in its final form
            throw e;
        } catch (IOException e) {
            if (e instanceof ClientProtocolException && e.getCause() instanceof RequestRefusedException refused) {
                throw refused.getRefusal();
            }
            if (caller.isInterrupted()) {
                // the interrupt stays set, so that the job is reported as cancelled rather than as failed
                InterruptedException interrupted = new InterruptedException();
                interrupted.addSuppressed(e);
                throw HyracksDataException.create(interrupted);
            }
            if (request.isAborted() || e instanceof SocketTimeoutException || e instanceof ConnectTimeoutException) {
                // not chained: an InterruptedIOException at the root of the cause would report the job as cancelled
                throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_TIMEOUT, sourceLoc, displayUrl,
                        timeoutSeconds);
            }
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_REQUEST_FAILED, e, sourceLoc, displayUrl,
                    ExceptionUtils.getMessage(e));
        } finally {
            watch.cancel(false);
        }
    }

    /**
     * Creates the context of an endpoint's requests.
     *
     * @param certificate the PEM-encoded certificates of the only authorities whose HTTPS servers the requests
     *        trust, or {@code null} to trust those the JVM does
     * @param sourceLoc the location of the call, which an error reports
     * @throws HyracksDataException if {@code certificate} cannot be read
     */
    static HttpClientContext createContext(String certificate, SourceLocation sourceLoc) throws HyracksDataException {
        return createContext(certificate, false, sourceLoc);
    }

    /**
     * Creates the context of an endpoint's requests.
     *
     * @param certificate the PEM-encoded certificates of authorities whose HTTPS servers the requests trust, or
     *        {@code null} to trust those the JVM does
     * @param jvmAuthorities whether the requests also trust the authorities the JVM does when {@code certificate} is
     *        given
     * @param sourceLoc the location of the call, which an error reports
     * @throws HyracksDataException if {@code certificate} cannot be read
     */
    static HttpClientContext createContext(String certificate, boolean jvmAuthorities, SourceLocation sourceLoc)
            throws HyracksDataException {
        HttpClientContext context = HttpClientContext.create();
        if (certificate != null) {
            context.setAttribute(SOCKET_FACTORY_ATTRIBUTE,
                    new SSLConnectionSocketFactory(createSslContext(certificate, jvmAuthorities, sourceLoc)));
            // a pooled connection is reused only by requests of the same token, so never by one trusting others
            context.setUserToken(new Trust(certificate, jvmAuthorities));
        }
        return context;
    }

    /**
     * Creates the configuration of a call's requests, which bounds each wait of a request by the call's timeout.
     */
    static RequestConfig createRequestConfig(int timeoutSeconds) {
        int timeoutMillis = Math.toIntExact(TimeUnit.SECONDS.toMillis(timeoutSeconds));
        return RequestConfig.custom().setConnectTimeout(timeoutMillis).setConnectionRequestTimeout(timeoutMillis)
                .setSocketTimeout(timeoutMillis).build();
    }

    private static SSLContext createSslContext(String certificate, boolean jvmAuthorities, SourceLocation sourceLoc)
            throws HyracksDataException {
        try {
            SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
            sslContext.init(null, TlsUtils.buildTrustManagers(certificate, jvmAuthorities), null);
            return sslContext;
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_OPTION, e, sourceLoc,
                    HttpFunctionConstants.OPTION_CERTIFICATES, ExceptionUtils.getMessage(e));
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
     * Sends each request through the proxy of the egress policy, if it names one, once the policy has allowed the
     * request.
     */
    private static final class PolicyRoutePlanner extends DefaultRoutePlanner {

        private final IHttpEgressPolicy policy;

        private PolicyRoutePlanner(IHttpEgressPolicy policy) {
            super(DefaultSchemePortResolver.INSTANCE);
            this.policy = policy;
        }

        @Override
        protected HttpHost determineProxy(HttpHost target, HttpRequest request, HttpContext context)
                throws HttpException {
            URI proxy;
            try {
                proxy = policy.getProxy();
                policy.checkRequest(target.getHostName(), proxy);
            } catch (HyracksDataException e) {
                throw new RequestRefusedException(e);
            }
            return proxy == null ? null : resolveProxy(proxy);
        }

        // resolved here rather than by the client, whose resolver judges the addresses of the hosts requests go to
        private static HttpHost resolveProxy(URI proxy) throws HttpException {
            HttpHost host = URIUtils.extractHost(proxy);
            if (host == null) {
                throw new HttpException("Invalid proxy " + proxy);
            }
            try {
                InetAddress address = SystemDefaultDnsResolver.INSTANCE.resolve(host.getHostName())[0];
                return new HttpHost(address, host.getHostName(), host.getPort(), host.getSchemeName());
            } catch (UnknownHostException e) {
                throw new HttpException("Cannot resolve proxy " + host.getHostName(), e);
            }
        }
    }

    /**
     * Opens HTTPS connections with the socket factory of the request's context, which trusts the authorities of the
     * request's endpoint, or with the JVM's if it has none. Either verifies the host name. A connection to an HTTPS
     * proxy trusts the authorities the JVM does: those of the endpoint are not the proxy's.
     */
    private static final class ContextTrustSocketFactory implements LayeredConnectionSocketFactory {

        private static final SSLConnectionSocketFactory DEFAULT = SSLConnectionSocketFactory.getSocketFactory();

        @Override
        public Socket createSocket(HttpContext context) throws IOException {
            return select(context).createSocket(context);
        }

        @Override
        public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress,
                InetSocketAddress localAddress, HttpContext context) throws IOException {
            RouteInfo route = HttpClientContext.adapt(context).getHttpRoute();
            SSLConnectionSocketFactory factory =
                    route != null && host.equals(route.getProxyHost()) ? DEFAULT : select(context);
            return factory.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
        }

        @Override
        public Socket createLayeredSocket(Socket socket, String target, int port, HttpContext context)
                throws IOException {
            return select(context).createLayeredSocket(socket, target, port, context);
        }

        private static SSLConnectionSocketFactory select(HttpContext context) {
            Object factory = context.getAttribute(SOCKET_FACTORY_ATTRIBUTE);
            return factory == null ? DEFAULT : (SSLConnectionSocketFactory) factory;
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

    /**
     * Carries a refusal of the egress policy out of the client's route planner, which may throw only
     * {@link HttpException}.
     */
    private static final class RequestRefusedException extends HttpException {

        private static final long serialVersionUID = 1L;

        private RequestRefusedException(HyracksDataException refusal) {
            super(refusal.getMessage(), refusal);
        }

        private HyracksDataException getRefusal() {
            return (HyracksDataException) getCause();
        }
    }

    private record Trust(String certificate, boolean jvmAuthorities) {
    }
}
