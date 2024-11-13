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

import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.external.IExternalCredentialsCacheUpdater;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class RefreshAwsCredentialsRequest implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();

    private final String nodeId;
    private final long reqId;
    private final Map<String, String> configuration;

    public RefreshAwsCredentialsRequest(String nodeId, long reqId, Map<String, String> configuration) {
        this.nodeId = nodeId;
        this.reqId = reqId;
        this.configuration = configuration;
    }

    @Override
    public final void handle(ICcApplicationContext appCtx) throws HyracksDataException {
        try {
            IExternalCredentialsCacheUpdater cacheUpdater = appCtx.getExternalCredentialsCacheUpdater();
            Object credentials = cacheUpdater.generateAndCacheCredentials(configuration);
            AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentials;

            // respond with the credentials
            RefreshAwsCredentialsResponse response =
                    new RefreshAwsCredentialsResponse(reqId, sessionCredentials.accessKeyId(),
                            sessionCredentials.secretAccessKey(), sessionCredentials.sessionToken(), null);
            respond(appCtx, response);
        } catch (Exception e) {
            LOGGER.info("failed to refresh credentials", e);
            RefreshAwsCredentialsResponse response = new RefreshAwsCredentialsResponse(reqId, null, null, null, e);
            respond(appCtx, response);
        }
    }

    private void respond(ICcApplicationContext appCtx, RefreshAwsCredentialsResponse response)
            throws HyracksDataException {
        CCMessageBroker broker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            broker.sendApplicationMessageToNC(response, nodeId);
        } catch (Exception e) {
            LOGGER.info("failed to send reply to nc", e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
