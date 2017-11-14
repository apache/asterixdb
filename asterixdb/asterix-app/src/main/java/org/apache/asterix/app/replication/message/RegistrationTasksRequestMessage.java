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
package org.apache.asterix.app.replication.message;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.NodeControllerService;

public class RegistrationTasksRequestMessage implements INCLifecycleMessage, ICcAddressedMessage {

    private static final Logger LOGGER = Logger.getLogger(RegistrationTasksRequestMessage.class.getName());
    private static final long serialVersionUID = 1L;
    private final SystemState state;
    private final String nodeId;
    private final NodeStatus nodeStatus;

    public RegistrationTasksRequestMessage(String nodeId, NodeStatus nodeStatus, SystemState state) {
        this.state = state;
        this.nodeId = nodeId;
        this.nodeStatus = nodeStatus;
    }

    public static void send(NodeControllerService cs, NodeStatus nodeStatus, SystemState systemState)
            throws HyracksDataException {
        try {
            RegistrationTasksRequestMessage msg = new RegistrationTasksRequestMessage(cs.getId(), nodeStatus,
                    systemState);
            ((INCMessageBroker) cs.getContext().getMessageBroker()).sendMessageToCC(msg);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unable to send RegistrationTasksRequestMessage to CC", e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        appCtx.getFaultToleranceStrategy().process(this);
    }

    public SystemState getState() {
        return state;
    }

    public String getNodeId() {
        return nodeId;
    }

    public NodeStatus getNodeStatus() {
        return nodeStatus;
    }

    @Override
    public MessageType getType() {
        return MessageType.REGISTRATION_TASKS_REQUEST;
    }

}