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
package org.apache.asterix.messaging;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.messaging.AbstractApplicationMessage;
import org.apache.asterix.common.messaging.ReportMaxResourceIdMessage;
import org.apache.asterix.common.messaging.TakeoverMetadataNodeResponseMessage;
import org.apache.asterix.common.messaging.TakeoverPartitionsRequestMessage;
import org.apache.asterix.common.messaging.TakeoverPartitionsResponseMessage;
import org.apache.asterix.common.messaging.api.IApplicationMessage;
import org.apache.asterix.common.messaging.api.IApplicationMessageCallback;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.asterix.metadata.bootstrap.MetadataIndexImmutableProperties;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;

public class NCMessageBroker implements INCMessageBroker {
    private final NodeControllerService ncs;
    private final AtomicLong messageId = new AtomicLong(0);
    private final Map<Long, IApplicationMessageCallback> callbacks;

    public NCMessageBroker(NodeControllerService ncs) {
        this.ncs = ncs;
        callbacks = new ConcurrentHashMap<Long, IApplicationMessageCallback>();
    }

    @Override
    public void sendMessage(IApplicationMessage message, IApplicationMessageCallback callback) throws Exception {
        if (callback != null) {
            long uniqueMessageId = messageId.incrementAndGet();
            message.setId(uniqueMessageId);
            callbacks.put(uniqueMessageId, callback);
        }
        try {
            ncs.sendApplicationMessageToCC(JavaSerializationUtils.serialize(message), null);
        } catch (Exception e) {
            if (callback != null) {
                //remove the callback in case of failure
                callbacks.remove(message.getId());
            }
            throw e;
        }
    }

    @Override
    public void receivedMessage(IMessage message, String nodeId) throws Exception {
        AbstractApplicationMessage absMessage = (AbstractApplicationMessage) message;
        //if the received message is a response to a sent message, deliver it to the sender
        IApplicationMessageCallback callback = callbacks.remove(absMessage.getId());
        if (callback != null) {
            callback.deliverMessageResponse(absMessage);
        }

        //handle requests from CC
        switch (absMessage.getMessageType()) {
            case REPORT_MAX_RESOURCE_ID_REQUEST:
                reportMaxResourceId();
                break;
            case TAKEOVER_PARTITIONS_REQUEST:
                handleTakeoverPartitons(message);
                break;
            case TAKEOVER_METADATA_NODE_REQUEST:
                handleTakeoverMetadataNode(message);
                break;
            default:
                break;
        }
    }

    private void handleTakeoverPartitons(IMessage message) throws Exception {
        TakeoverPartitionsRequestMessage msg = (TakeoverPartitionsRequestMessage) message;
        IAsterixAppRuntimeContext appContext = (IAsterixAppRuntimeContext) ncs.getApplicationContext()
                .getApplicationObject();
        IRemoteRecoveryManager remoteRecoeryManager = appContext.getRemoteRecoveryManager();
        remoteRecoeryManager.takeoverPartitons(msg.getFailedNode(), msg.getPartitions());
        //send response after takeover is completed
        TakeoverPartitionsResponseMessage reponse = new TakeoverPartitionsResponseMessage(msg.getRequestId(),
                appContext.getTransactionSubsystem().getId(), msg.getPartitions());
        sendMessage(reponse, null);
    }

    private void handleTakeoverMetadataNode(IMessage message) throws Exception {
        IAsterixAppRuntimeContext appContext = (IAsterixAppRuntimeContext) ncs.getApplicationContext()
                .getApplicationObject();
        appContext.initializeMetadata(false);
        appContext.exportMetadataNodeStub();
        //send response after takeover is completed
        TakeoverMetadataNodeResponseMessage reponse = new TakeoverMetadataNodeResponseMessage(
                appContext.getTransactionSubsystem().getId());
        sendMessage(reponse, null);
    }

    @Override
    public void reportMaxResourceId() throws Exception {
        IAsterixAppRuntimeContext appContext = (IAsterixAppRuntimeContext) ncs.getApplicationContext()
                .getApplicationObject();
        ReportMaxResourceIdMessage maxResourceIdMsg = new ReportMaxResourceIdMessage();
        //resource ids < FIRST_AVAILABLE_USER_DATASET_ID are reserved for metadata indexes.
        long maxResourceId = Math.max(appContext.getLocalResourceRepository().getMaxResourceID(),
                MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID);
        maxResourceIdMsg.setMaxResourceId(maxResourceId);
        sendMessage(maxResourceIdMsg, null);
    }
}
