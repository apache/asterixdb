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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.HttpOAuthClient;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;

/**
 * Gets the access token of an OAuth client with the client credentials grant (RFC 6749, section 4.4), on the cluster
 * controller. A token is shared by every call in the cluster that uses the same client, until shortly before it
 * expires, and one call at a time fetches the next: a token endpoint may revoke a client's token when it issues the
 * next, so nodes that each fetched their own would revoke each other's.
 */
final class HttpFunctionTokenSource {

    private static final ConcurrentMap<SlotKey, Slot> SLOTS = new ConcurrentHashMap<>();
    // so that the clients of functions that are no longer called, or no longer exist, are not kept until restart
    private static final long IDLE_NANOS = TimeUnit.MINUTES.toNanos(10);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String GRANT_TYPE = "grant_type";
    private static final String CLIENT_CREDENTIALS = "client_credentials";
    private static final String SCOPE = "scope";
    private static final String ACCESS_TOKEN = "access_token";
    private static final String TOKEN_TYPE = "token_type";
    private static final String EXPIRES_IN = "expires_in";
    private static final String BEARER = "bearer";
    // so that a token is not sent in the moments before it expires, while its request is on the way
    private static final long EXPIRY_MARGIN_SECONDS = 30;
    private static final long NEVER_EXPIRES = Long.MAX_VALUE;
    private static final String RESPONSE = "response";
    private static final String NO_ACCESS_TOKEN = "the response has no access token";

    private final HttpOAuthClient oauthClient;
    private final Slot slot;
    private final CloseableHttpClient client;
    private final HttpClientContext context;
    private final RequestConfig requestConfig;
    private final SourceLocation sourceLoc;
    private final int timeoutSeconds;
    private final int maxResponseSize;
    private final String displayUrl;
    private final String clientAuthorization;
    private final HttpEntity grant;

    /**
     * @param certificate the certificate of the function's endpoint, so that the token endpoint is trusted as the
     *        function's endpoint is
     * @param timeoutSeconds the timeout of the calls that get tokens
     * @param maxResponseSize the size of the largest token response read
     */
    HttpFunctionTokenSource(HttpOAuthClient oauthClient, String certificate, CloseableHttpClient client,
            SourceLocation sourceLoc, int timeoutSeconds, int maxResponseSize) throws HyracksDataException {
        this.oauthClient = oauthClient;
        long nowNanos = System.nanoTime();
        SLOTS.values().removeIf(idle -> idle.isIdle(nowNanos));
        // not shared across trust, or whether a call could get a token would depend on which calls came before it
        slot = SLOTS.computeIfAbsent(new SlotKey(oauthClient, certificate), key -> new Slot(nowNanos));
        slot.usedNanos = nowNanos;
        this.client = client;
        // the token endpoint is often a public identity provider, while the certificates are there for the function's
        // endpoint, so it trusts the JVM's authorities as well
        context = HttpFunctionConnectionManager.createContext(certificate, true, sourceLoc);
        requestConfig = HttpFunctionConnectionManager.createRequestConfig(timeoutSeconds);
        this.sourceLoc = sourceLoc;
        this.timeoutSeconds = timeoutSeconds;
        this.maxResponseSize = maxResponseSize;
        displayUrl = HttpFunctionUrlTemplate.toDisplayUrl(oauthClient.tokenUri());
        // the client's credentials are form-encoded before they are joined (RFC 6749, section 2.3.1)
        clientAuthorization = HttpAuthorization.basic(URLEncoder.encode(oauthClient.clientId(), StandardCharsets.UTF_8),
                URLEncoder.encode(oauthClient.clientSecret(), StandardCharsets.UTF_8));
        List<NameValuePair> parameters = new ArrayList<>();
        parameters.add(new BasicNameValuePair(GRANT_TYPE, CLIENT_CREDENTIALS));
        if (oauthClient.scopes() != null) {
            parameters.add(new BasicNameValuePair(SCOPE, oauthClient.scopes()));
        }
        grant = new UrlEncodedFormEntity(parameters, StandardCharsets.UTF_8);
    }

    /**
     * @param deadlineNanos the {@link System#nanoTime()} by which the call that needs the token must end
     * @param rejected a token that an endpoint has rejected, which is fetched again rather than taken from the cache,
     *        or {@code null}
     * @return the client's token
     * @throws HyracksDataException if no token could be got by the deadline
     */
    Token getToken(long deadlineNanos, String rejected) throws HyracksDataException {
        Token token = slot.token;
        if (isUsable(token, rejected)) {
            return token;
        }
        try {
            if (!slot.fetchLock.tryLock(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS)) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_TIMEOUT, sourceLoc, displayUrl,
                        timeoutSeconds);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
        try {
            // the call that held the lock may have fetched one
            token = slot.token;
            if (!isUsable(token, rejected)) {
                token = fetch(deadlineNanos);
                slot.token = token;
            }
            return token;
        } finally {
            slot.fetchLock.unlock();
        }
    }

    static boolean isUsable(Token token, String rejected) {
        return token != null && !token.value().equals(rejected) && token.isFresh(System.nanoTime());
    }

    private Token fetch(long deadlineNanos) throws HyracksDataException {
        HttpPost request = new HttpPost(oauthClient.tokenUri());
        request.setConfig(requestConfig);
        request.setHeader(HttpHeaders.AUTHORIZATION, clientAuthorization);
        request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
        request.setEntity(grant);
        // a lifetime counts from when the token is issued, which the request precedes
        long requestedNanos = System.nanoTime();
        return HttpFunctionConnectionManager.execute(client, request, context, deadlineNanos,
                response -> readToken(response, requestedNanos), sourceLoc, displayUrl, timeoutSeconds);
    }

    private Token readToken(HttpResponse response, long requestedNanos) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status < HttpStatus.SC_OK || status >= HttpStatus.SC_MULTIPLE_CHOICES) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_HTTP_ERROR, sourceLoc, displayUrl, status);
        }
        HttpEntity entity = response.getEntity();
        // one byte past the limit tells a response at the limit from a larger one
        byte[] body = entity == null ? new byte[0] : entity.getContent().readNBytes(maxResponseSize + 1);
        if (body.length > maxResponseSize) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_SIZE_LIMIT_EXCEEDED, sourceLoc, RESPONSE,
                    displayUrl, maxResponseSize);
        }
        JsonNode json = parse(body);
        JsonNode accessToken = json.path(ACCESS_TOKEN);
        if (!accessToken.isTextual() || accessToken.textValue().isEmpty()) {
            throw requestFailed(NO_ACCESS_TOKEN);
        }
        String tokenType = json.path(TOKEN_TYPE).asText(null);
        if (tokenType != null && !BEARER.equalsIgnoreCase(tokenType)) {
            throw requestFailed("unsupported token type '" + tokenType + "'");
        }
        long expiresInSeconds = json.path(EXPIRES_IN).asLong();
        // a token without a positive expiry is used until an endpoint rejects it, and one that lives no longer than
        // the margin keeps half its lifetime, or every call would fetch a token
        long lifetimeNanos = expiresInSeconds <= 0 ? NEVER_EXPIRES
                : TimeUnit.SECONDS.toNanos(expiresInSeconds - Math.min(EXPIRY_MARGIN_SECONDS, expiresInSeconds / 2));
        return new Token(accessToken.textValue(), requestedNanos, lifetimeNanos);
    }

    // not chained: the client is shown the deepest formatted error, which would be the parser's
    private static JsonNode parse(byte[] body) {
        try {
            JsonNode json = OBJECT_MAPPER.readTree(body);
            return json == null ? MissingNode.getInstance() : json;
        } catch (IOException e) {
            return MissingNode.getInstance();
        }
    }

    private HyracksDataException requestFailed(String reason) {
        return new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_REQUEST_FAILED, sourceLoc, displayUrl, reason);
    }

    record Token(String value, long issuedNanos, long lifetimeNanos) {

        private boolean isFresh(long nowNanos) {
            return nowNanos - issuedNanos < lifetimeNanos;
        }

        // a lifetime that another JVM can take on, whose System.nanoTime() has another origin
        long remainingNanos(long nowNanos) {
            return lifetimeNanos - (nowNanos - issuedNanos);
        }
    }

    record SlotKey(HttpOAuthClient oauthClient, String certificate) {
    }

    private static final class Slot {

        private final ReentrantLock fetchLock = new ReentrantLock();
        private volatile Token token;
        private volatile long usedNanos;

        private Slot(long usedNanos) {
            this.usedNanos = usedNanos;
        }

        // a slot dropped while a call still holds it costs at most one extra token request
        private boolean isIdle(long nowNanos) {
            return nowNanos - usedNanos >= IDLE_NANOS && !fetchLock.isLocked();
        }
    }
}
