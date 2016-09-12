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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.messaging.api.IApplicationMessage;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;

public class CCMessageBroker implements ICCMessageBroker {

    private final static Logger LOGGER = Logger.getLogger(CCMessageBroker.class.getName());
    private final ClusterControllerService ccs;

    public CCMessageBroker(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public void receivedMessage(IMessage message, String nodeId) throws Exception {
        IApplicationMessage absMessage = (IApplicationMessage) message;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Received message: " + absMessage);
        }
        absMessage.handle(ccs);
    }

    @Override
    public void sendApplicationMessageToNC(IApplicationMessage msg, String nodeId) throws Exception {
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        NodeControllerState state = nodeMap.get(nodeId);
        state.getNodeController().sendApplicationMessageToNC(JavaSerializationUtils.serialize(msg), null, nodeId);
    }
}
