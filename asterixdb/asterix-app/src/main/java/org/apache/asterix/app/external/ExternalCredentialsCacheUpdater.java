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
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3AuthUtils;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * The class is responsible for generating new credentials based on the adapter type. Given a request:
 * - if we are the CC, generate new creds and ask all NCs to update their cache
 * - if we are the NC, send a message to the CC to generate new creds
 */
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
        String key = configuration.get(ExternalDataConstants.KEY_ENTITY_ID);
        Object credentials = cache.get(key);
        if (credentials != null) {
            return credentials;
        }

        String type = configuration.get(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE);
        if (ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3.equals(type)) {
            return generateAwsCredentials(configuration);
        } else {
            // this should never happen
            throw new IllegalArgumentException("Unsupported external source type: " + type);
        }
    }

    // TODO: this can probably be refactored out into something that is AWS-specific
    private AwsSessionCredentials generateAwsCredentials(Map<String, String> configuration)
            throws HyracksDataException, CompilationException {
        String key = configuration.get(ExternalDataConstants.KEY_ENTITY_ID);
        AwsSessionCredentials credentials;
        if (appCtx instanceof ICcApplicationContext) {
            validateClusterState();
            try {
                LOGGER.info("attempting to update AWS credentials for {}", key);
                AwsCredentialsProvider newCredentials = S3AuthUtils.assumeRoleAndGetCredentials(configuration);
                LOGGER.info("updated AWS credentials successfully for {}", key);
                credentials = (AwsSessionCredentials) newCredentials.resolveCredentials();
                appCtx.getExternalCredentialsCache().put(key, credentials);
            } catch (CompilationException ex) {
                LOGGER.info("failed to refresh AWS credentials for {}", key, ex);
                throw ex;
            }

            String accessKeyId = credentials.accessKeyId();
            String secretAccessKey = credentials.secretAccessKey();
            String sessionToken = credentials.sessionToken();
            UpdateAwsCredentialsCacheRequest request =
                    new UpdateAwsCredentialsCacheRequest(configuration, accessKeyId, secretAccessKey, sessionToken);

            // request all NCs to update their credentials cache with the latest creds
            updateNcsCredentialsCache(key, request);
        } else {
            NCMessageBroker broker = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
            MessageFuture messageFuture = broker.registerMessageFuture();
            String nodeId = ((INCServiceContext) appCtx.getServiceContext()).getNodeId();
            long futureId = messageFuture.getFutureId();
            RefreshAwsCredentialsRequest request = new RefreshAwsCredentialsRequest(nodeId, futureId, configuration);
            try {
                LOGGER.info("no valid AWS credentials found for {}, requesting AWS credentials from CC", key);
                broker.sendMessageToPrimaryCC(request);
                RefreshAwsCredentialsResponse response = (RefreshAwsCredentialsResponse) messageFuture
                        .get(DEFAULT_NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (response.getFailure() != null) {
                    throw HyracksDataException.create(response.getFailure());
                }
                credentials = AwsSessionCredentials.create(response.getAccessKeyId(), response.getSecretAccessKey(),
                        response.getSessionToken());
            } catch (Exception ex) {
                LOGGER.info("failed to refresh AWS credentials for {}", key, ex);
                throw HyracksDataException.create(ex);
            } finally {
                broker.deregisterMessageFuture(futureId);
            }
        }
        return credentials;
    }

    private void updateNcsCredentialsCache(String key, INcAddressedMessage request) throws HyracksDataException {
        ICcApplicationContext ccAppCtx = (ICcApplicationContext) appCtx;
        final List<String> ncs = new ArrayList<>(ccAppCtx.getClusterStateManager().getParticipantNodes());
        CCMessageBroker broker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            LOGGER.info("requesting all NCs to update their credentials for {}", key);
            for (String nc : ncs) {
                broker.sendApplicationMessageToNC(request, nc);
            }
        } catch (Exception e) {
            LOGGER.info("failed to send message to nc", e);
            throw HyracksDataException.create(e);
        }
    }

    private void validateClusterState() throws HyracksDataException {
        ICcApplicationContext ccAppCtx = (ICcApplicationContext) appCtx;
        IClusterManagementWork.ClusterState state = ccAppCtx.getClusterStateManager().getState();
        if (!(state == ACTIVE || state == REBALANCE_REQUIRED)) {
            throw new RuntimeDataException(REJECT_BAD_CLUSTER_STATE, state);
        }
    }
}
