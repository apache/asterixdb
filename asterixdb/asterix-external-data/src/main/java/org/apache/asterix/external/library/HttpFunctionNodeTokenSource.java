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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.HttpEndpoint;
import org.apache.asterix.common.functions.HttpOAuthClient;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.external.library.HttpFunctionTokenSource.SlotKey;
import org.apache.asterix.external.library.HttpFunctionTokenSource.Token;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Gets the access token of an OAuth client from the cluster controller, whose {@link HttpFunctionTokenSource} shares
 * one token per client with every node. A node's calls share its copy until shortly before it expires or an endpoint
 * rejects it.
 */
final class HttpFunctionNodeTokenSource {

    private static final ConcurrentMap<SlotKey, AtomicReference<Token>> TOKENS = new ConcurrentHashMap<>();
    private static final String CC_UNREACHABLE = "the token request could not be sent to the cluster controller";

    private final AtomicReference<Token> token;
    private final INCServiceContext serviceCtx;
    private final HttpOAuthClient oauthClient;
    private final String certificate;
    private final SourceLocation sourceLoc;
    private final int timeoutSeconds;
    private final String displayUrl;

    /**
     * @param endpoint an endpoint with an OAuth client
     * @param timeoutSeconds the timeout of the calls that get tokens
     */
    HttpFunctionNodeTokenSource(HttpEndpoint endpoint, INCServiceContext serviceCtx, SourceLocation sourceLoc,
            int timeoutSeconds) {
        oauthClient = endpoint.getOAuthClient();
        certificate = endpoint.getCertificate();
        token = TOKENS.computeIfAbsent(new SlotKey(oauthClient, certificate), key -> new AtomicReference<>());
        this.serviceCtx = serviceCtx;
        this.sourceLoc = sourceLoc;
        this.timeoutSeconds = timeoutSeconds;
        displayUrl = HttpFunctionUrlTemplate.toDisplayUrl(oauthClient.tokenUri());
    }

    /**
     * @param deadlineNanos the {@link System#nanoTime()} by which the call that needs the token must end
     * @param rejected a token that an endpoint has rejected, which is replaced rather than taken from the cache, or
     *        {@code null}
     * @return the client's token
     * @throws HyracksDataException if no token could be got by the deadline
     */
    String getToken(long deadlineNanos, String rejected) throws HyracksDataException {
        Token cached = token.get();
        if (HttpFunctionTokenSource.isUsable(cached, rejected)) {
            return cached.value();
        }
        Token fetched = requestToken(deadlineNanos, rejected);
        // a slow answer must not replace a newer token that another call has stored meanwhile
        token.compareAndSet(cached, fetched);
        return fetched.value();
    }

    private Token requestToken(long deadlineNanos, String rejected) throws HyracksDataException {
        INCMessageBroker broker = (INCMessageBroker) serviceCtx.getMessageBroker();
        MessageFuture future = broker.registerMessageFuture();
        try {
            broker.sendMessageToPrimaryCC(
                    new HttpFunctionTokenRequestMessage(serviceCtx.getNodeId(), future.getFutureId(), oauthClient,
                            certificate, rejected, deadlineNanos - System.nanoTime(), sourceLoc, timeoutSeconds));
            return ((HttpFunctionTokenResponseMessage) future.get(deadlineNanos - System.nanoTime(),
                    TimeUnit.NANOSECONDS)).getToken();
        } catch (TimeoutException e) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_TIMEOUT, sourceLoc, displayUrl,
                    timeoutSeconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        } catch (HyracksDataException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_REQUEST_FAILED, e, sourceLoc, displayUrl,
                    CC_UNREACHABLE);
        } finally {
            broker.deregisterMessageFuture(future.getFutureId());
        }
    }
}
