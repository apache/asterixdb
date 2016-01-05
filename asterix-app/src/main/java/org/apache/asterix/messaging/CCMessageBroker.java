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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.asterix.common.messaging.AbstractApplicationMessage;
import org.apache.asterix.common.messaging.ReportMaxResourceIdMessage;
import org.apache.asterix.common.messaging.ReportMaxResourceIdRequestMessage;
import org.apache.asterix.common.messaging.ResourceIdRequestMessage;
import org.apache.asterix.common.messaging.ResourceIdRequestResponseMessage;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;

public class CCMessageBroker implements IMessageBroker {

    private final static Logger LOGGER = Logger.getLogger(CCMessageBroker.class.getName());
    private final AtomicLong globalResourceId = new AtomicLong(0);
    private final ClusterControllerService ccs;
    private final Set<String> nodesReportedMaxResourceId = new HashSet<>();
    public static final long NO_CALLBACK_MESSAGE_ID = -1;

    public CCMessageBroker(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public void receivedMessage(IMessage message, String nodeId) throws Exception {
        AbstractApplicationMessage absMessage = (AbstractApplicationMessage) message;
        switch (absMessage.getMessageType()) {
            case RESOURCE_ID_REQUEST:
                handleResourceIdRequest(message, nodeId);
                break;
            case REPORT_MAX_RESOURCE_ID_RESPONSE:
                handleReportResourceMaxIdResponse(message, nodeId);
                break;
            default:
                LOGGER.warning("Unknown message: " + absMessage.getMessageType());
                break;
        }
    }

    private synchronized void handleResourceIdRequest(IMessage message, String nodeId) throws Exception {
        ResourceIdRequestMessage msg = (ResourceIdRequestMessage) message;
        ResourceIdRequestResponseMessage reponse = new ResourceIdRequestResponseMessage();
        reponse.setId(msg.getId());
        //cluster is not active
        if (!AsterixClusterProperties.isClusterActive()) {
            reponse.setResourceId(-1);
            reponse.setException(new Exception("Cannot generate global resource id when cluster is not active."));
        } else if (nodesReportedMaxResourceId.size() < AsterixClusterProperties.getNumberOfNodes()) {
            //some node has not reported max resource id
            reponse.setResourceId(-1);
            reponse.setException(new Exception("One or more nodes has not reported max resource id."));
            requestMaxResourceID();
        } else {
            reponse.setResourceId(globalResourceId.incrementAndGet());
        }
        sendApplicationMessageToNC(reponse, nodeId);
    }

    private synchronized void handleReportResourceMaxIdResponse(IMessage message, String nodeId) throws Exception {
        ReportMaxResourceIdMessage msg = (ReportMaxResourceIdMessage) message;
        globalResourceId.set(Math.max(msg.getMaxResourceId(), globalResourceId.get()));
        nodesReportedMaxResourceId.add(nodeId);
    }

    private void sendApplicationMessageToNC(IMessage msg, String nodeId) throws Exception {
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        NodeControllerState state = nodeMap.get(nodeId);
        state.getNodeController().sendApplicationMessageToNC(JavaSerializationUtils.serialize(msg), null, nodeId);
    }

    private void requestMaxResourceID() throws Exception {
        //send request to NCs that have not reported their max resource ids
        Set<String> getParticipantNodes = AsterixClusterProperties.INSTANCE.getParticipantNodes();
        ReportMaxResourceIdRequestMessage msg = new ReportMaxResourceIdRequestMessage();
        msg.setId(NO_CALLBACK_MESSAGE_ID);
        for (String nodeId : getParticipantNodes) {
            if (!nodesReportedMaxResourceId.contains(nodeId)) {
                sendApplicationMessageToNC(msg, nodeId);
            }
        }
    }
}
