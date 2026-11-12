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

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.functions.HttpOAuthClient;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Asks the cluster controller for the access token of an OAuth client, on behalf of a node's call, and answers with a
 * {@link HttpFunctionTokenResponseMessage}.
 */
final class HttpFunctionTokenRequestMessage implements ICcAddressedMessage {

    private static final long serialVersionUID = 1L;

    private final String nodeId;
    private final long futureId;
    private final HttpOAuthClient oauthClient;
    private final String certificate;
    private final String rejected;
    // what is left of the call's timeout, since System.nanoTime() has another origin on the cluster controller
    private final long remainingNanos;
    private final SourceLocation sourceLoc;
    private final int timeoutSeconds;

    HttpFunctionTokenRequestMessage(String nodeId, long futureId, HttpOAuthClient oauthClient, String certificate,
            String rejected, long remainingNanos, SourceLocation sourceLoc, int timeoutSeconds) {
        this.nodeId = nodeId;
        this.futureId = futureId;
        this.oauthClient = oauthClient;
        this.certificate = certificate;
        this.rejected = rejected;
        this.remainingNanos = remainingNanos;
        this.sourceLoc = sourceLoc;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException {
        long deadlineNanos = System.nanoTime() + remainingNanos;
        HttpFunctionTokenResponseMessage response;
        try {
            HttpFunctionTokenSource source =
                    new HttpFunctionTokenSource(oauthClient, certificate, appCtx.getHttpFunctionClient(), sourceLoc,
                            timeoutSeconds, appCtx.getExternalProperties().getExternalHttpFunctionMaxResponseSize());
            response = new HttpFunctionTokenResponseMessage(futureId, source.getToken(deadlineNanos, rejected), null);
        } catch (Exception e) {
            // whatever went wrong, the node is told, rather than left to wait out its timeout
            response = new HttpFunctionTokenResponseMessage(futureId, null, HyracksDataException.create(e));
        }
        try {
            ((ICCMessageBroker) appCtx.getServiceContext().getMessageBroker()).sendApplicationMessageToNC(response,
                    nodeId);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    // a node sends one whenever its copy of a token is missing, expiring or rejected
    @Override
    public boolean isWhispered() {
        return true;
    }
}
