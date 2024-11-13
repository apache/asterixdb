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
package org.apache.asterix.app.external;

import static org.apache.asterix.app.message.ExecuteStatementRequestMessage.DEFAULT_NC_TIMEOUT_MILLIS;
import static org.apache.asterix.common.api.IClusterManagementWork.ClusterState.ACTIVE;
import static org.apache.asterix.common.api.IClusterManagementWork.ClusterState.REBALANCE_REQUIRED;
import static org.apache.asterix.common.exceptions.ErrorCode.REJECT_BAD_CLUSTER_STATE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.RefreshAwsCredentialsRequest;
import org.apache.asterix.app.message.RefreshAwsCredentialsResponse;
import org.apache.asterix.app.message.UpdateAwsCredentialsCacheRequest;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.external.IExternalCredentialsCache;
import org.apache.asterix.common.external.IExternalCredentialsCacheUpdater;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.external.util.aws.s3.S3AuthUtils;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class ExternalCredentialsCacheUpdater implements IExternalCredentialsCacheUpdater {

    private static final Logger LOGGER = LogManager.getLogger();
    private final IApplicationContext appCtx;

    public ExternalCredentialsCacheUpdater(IApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    @Override
    public synchronized Object generateAndCacheCredentials(Map<String, String> configuration)
            throws HyracksDataException, CompilationException {
        IExternalCredentialsCache cache = appCtx.getExternalCredentialsCache();
        Object credentials = cache.getCredentials(configuration);
        if (credentials != null) {
            return credentials;
        }

        /*
         * if we are the CC, generate new creds and ask all NCs to update their cache
         * if we are the NC, send a message to the CC to generate new creds and ask all NCs to update their cache
         */
        String name = cache.getName(configuration);
        if (appCtx instanceof ICcApplicationContext) {
            ICcApplicationContext ccAppCtx = (ICcApplicationContext) appCtx;
            IClusterManagementWork.ClusterState state = ccAppCtx.getClusterStateManager().getState();
            if (!(state == ACTIVE || state == REBALANCE_REQUIRED)) {
                throw new RuntimeDataException(REJECT_BAD_CLUSTER_STATE, state);
            }

            String accessKeyId;
            String secretAccessKey;
            String sessionToken;
            Map<String, String> credentialsMap = new HashMap<>();
            try {
                LOGGER.info("attempting to update credentials for {}", name);
                AwsCredentialsProvider newCredentials = S3AuthUtils.assumeRoleAndGetCredentials(configuration);
                LOGGER.info("updated credentials successfully for {}", name);
                AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) newCredentials.resolveCredentials();
                accessKeyId = sessionCredentials.accessKeyId();
                secretAccessKey = sessionCredentials.secretAccessKey();
                sessionToken = sessionCredentials.sessionToken();
            } catch (CompilationException ex) {
                LOGGER.info("failed to refresh credentials for {}", name, ex);
                throw ex;
            }

            // credentials need refreshing
            credentialsMap.put(S3Constants.ACCESS_KEY_ID_FIELD_NAME, accessKeyId);
            credentialsMap.put(S3Constants.SECRET_ACCESS_KEY_FIELD_NAME, secretAccessKey);
            credentialsMap.put(S3Constants.SESSION_TOKEN_FIELD_NAME, sessionToken);

            // request all NCs to update their credentials cache with the latest creds
            updateNcsCredentialsCache(ccAppCtx, name, credentialsMap, configuration);
            cache.updateCache(configuration, credentialsMap);
            credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        } else {
            NCMessageBroker broker = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
            MessageFuture messageFuture = broker.registerMessageFuture();
            String nodeId = ((INCServiceContext) appCtx.getServiceContext()).getNodeId();
            long futureId = messageFuture.getFutureId();
            RefreshAwsCredentialsRequest request = new RefreshAwsCredentialsRequest(nodeId, futureId, configuration);
            try {
                LOGGER.info("no valid credentials found for {}, requesting credentials from CC", name);
                broker.sendMessageToPrimaryCC(request);
                RefreshAwsCredentialsResponse response = (RefreshAwsCredentialsResponse) messageFuture
                        .get(DEFAULT_NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (response.getFailure() != null) {
                    throw HyracksDataException.create(response.getFailure());
                }
                credentials = AwsSessionCredentials.create(response.getAccessKeyId(), response.getSecretAccessKey(),
                        response.getSessionToken());
            } catch (Exception ex) {
                LOGGER.info("failed to refresh credentials for {}", name, ex);
                throw HyracksDataException.create(ex);
            } finally {
                broker.deregisterMessageFuture(futureId);
            }
        }
        return credentials;
    }

    private void updateNcsCredentialsCache(ICcApplicationContext appCtx, String name, Map<String, String> credentials,
            Map<String, String> configuration) throws HyracksDataException {
        final List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
        CCMessageBroker broker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        UpdateAwsCredentialsCacheRequest request = new UpdateAwsCredentialsCacheRequest(configuration, credentials);

        try {
            LOGGER.info("requesting all NCs to update their credentials for {}", name);
            for (String nc : ncs) {
                broker.sendApplicationMessageToNC(request, nc);
            }
        } catch (Exception e) {
            LOGGER.info("failed to send message to nc", e);
            throw HyracksDataException.create(e);
        }
    }
}
