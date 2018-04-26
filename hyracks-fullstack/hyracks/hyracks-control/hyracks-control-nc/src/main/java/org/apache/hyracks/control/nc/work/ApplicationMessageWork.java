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
package org.apache.hyracks.control.nc.work;

import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.application.NCServiceContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ApplicationMessageWork extends AbstractWork {
    private static final Logger LOGGER = LogManager.getLogger();
    private byte[] message;
    private DeploymentId deploymentId;
    private String nodeId;
    private NodeControllerService ncs;

    public ApplicationMessageWork(NodeControllerService ncs, byte[] message, DeploymentId deploymentId, String nodeId) {
        this.ncs = ncs;
        this.deploymentId = deploymentId;
        this.nodeId = nodeId;
        this.message = message;
    }

    @Override
    public void run() {
        NCServiceContext ctx = ncs.getContext();
        try {
            IMessage data = (IMessage) DeploymentUtils.deserialize(message, deploymentId, ctx);
            if (ctx.getMessageBroker() != null) {
                ctx.getMessageBroker().receivedMessage(data, nodeId);
            } else {
                LOGGER.log(Level.WARN, "Message was sent, but no Message Broker set!");
            }
        } catch (Exception e) {
            LOGGER.warn("Error in application message delivery!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return getName() + ": nodeId: " + nodeId;
    }
}
