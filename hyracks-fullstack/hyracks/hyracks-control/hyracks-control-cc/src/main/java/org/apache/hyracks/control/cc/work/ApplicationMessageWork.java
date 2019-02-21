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

import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.messages.IMessage;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author rico
 */
public class ApplicationMessageWork extends AbstractHeartbeatWork {

    private static final Logger LOGGER = LogManager.getLogger();
    private byte[] message;
    private DeploymentId deploymentId;

    public ApplicationMessageWork(ClusterControllerService ccs, byte[] message, DeploymentId deploymentId,
            String nodeId) {
        super(ccs, nodeId, null);
        this.deploymentId = deploymentId;
        this.message = message;
    }

    @Override
    public void runWork() {
        final ICCServiceContext ctx = ccs.getContext();
        try {
            final IMessage data = (IMessage) DeploymentUtils.deserialize(message, deploymentId, ctx);
            notifyMessageBroker(ctx, data, nodeId);
        } catch (Exception e) {
            LOGGER.error("unexpected error", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        return getName() + ": nodeID: " + nodeId;
    }

    private static void notifyMessageBroker(ICCServiceContext ctx, IMessage msg, String nodeId) {
        final ExecutorService executor = ctx.getControllerService().getExecutor();
        executor.execute(() -> {
            try {
                ctx.getMessageBroker().receivedMessage(msg, nodeId);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
