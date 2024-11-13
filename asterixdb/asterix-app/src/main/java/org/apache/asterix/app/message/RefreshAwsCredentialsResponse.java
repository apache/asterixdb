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
package org.apache.asterix.app.message;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.messaging.NCMessageBroker;

public class RefreshAwsCredentialsResponse implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final long reqId;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final Throwable failure;

    public RefreshAwsCredentialsResponse(long reqId, String accessKeyId, String secretAccessKey, String sessionToken,
            Throwable failure) {
        this.reqId = reqId;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.failure = failure;
    }

    @Override
    public void handle(INcApplicationContext appCtx) {
        NCMessageBroker mb = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        MessageFuture future = mb.deregisterMessageFuture(reqId);
        if (future != null) {
            future.complete(this);
        }
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public Throwable getFailure() {
        return failure;
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
