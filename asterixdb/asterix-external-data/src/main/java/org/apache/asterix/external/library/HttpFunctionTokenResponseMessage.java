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

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.external.library.HttpFunctionTokenSource.Token;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Answers a {@link HttpFunctionTokenRequestMessage} with a token or with why none could be got.
 */
final class HttpFunctionTokenResponseMessage implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;

    private final long futureId;
    private final String token;
    private final long remainingNanos;
    private final HyracksDataException failure;

    HttpFunctionTokenResponseMessage(long futureId, Token token, HyracksDataException failure) {
        this.futureId = futureId;
        this.token = token == null ? null : token.value();
        this.remainingNanos = token == null ? 0 : token.remainingNanos(System.nanoTime());
        this.failure = failure;
    }

    @Override
    public void handle(INcApplicationContext appCtx) {
        MessageFuture future =
                ((INCMessageBroker) appCtx.getServiceContext().getMessageBroker()).deregisterMessageFuture(futureId);
        // null once the call has stopped waiting
        if (future != null) {
            future.complete(this);
        }
    }

    Token getToken() throws HyracksDataException {
        if (failure != null) {
            throw failure;
        }
        return new Token(token, System.nanoTime(), remainingNanos);
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
