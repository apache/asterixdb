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
package org.apache.hyracks.control.cc.work;

import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author rico
 */
public class ApplicationMessageWork extends AbstractHeartbeatWork {

    private static final Logger LOGGER = LogManager.getLogger();
    private byte[] message;
    private DeploymentId deploymentId;
    private String nodeId;
    private ClusterControllerService ccs;

    public ApplicationMessageWork(ClusterControllerService ccs, byte[] message, DeploymentId deploymentId,
            String nodeId) {
        super(ccs, nodeId, null);
        this.ccs = ccs;
        this.deploymentId = deploymentId;
        this.nodeId = nodeId;
        this.message = message;
    }

    @Override
    public void runWork() {
        final ICCServiceContext ctx = ccs.getContext();
        try {
            final IMessage data = (IMessage) DeploymentUtils.deserialize(message, deploymentId, ctx);
            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ctx.getMessageBroker().receivedMessage(data, nodeId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Error in stats reporting", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return getName() + ": nodeID: " + nodeId;
    }
}
