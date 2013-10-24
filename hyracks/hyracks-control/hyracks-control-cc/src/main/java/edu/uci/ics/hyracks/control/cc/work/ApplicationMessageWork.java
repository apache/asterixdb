/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc.work;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.messages.IMessage;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentUtils;

/**
 * @author rico
 */
public class ApplicationMessageWork extends AbstractHeartbeatWork {

    private static final Logger LOGGER = Logger.getLogger(ApplicationMessageWork.class.getName());
    private byte[] message;
    private DeploymentId deploymentId;
    private String nodeId;
    private ClusterControllerService ccs;

    public ApplicationMessageWork(ClusterControllerService ccs, byte[] message, DeploymentId deploymentId, String nodeId) {
        super(ccs, nodeId, null);
        this.ccs = ccs;
        this.deploymentId = deploymentId;
        this.nodeId = nodeId;
        this.message = message;
    }

    @Override
    public void runWork() {
        final ICCApplicationContext ctx = ccs.getApplicationContext();
        try {
            final IMessage data = (IMessage) DeploymentUtils.deserialize(message, deploymentId, ctx);
            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    ctx.getMessageBroker().receivedMessage(data, nodeId);
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error in stats reporting", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "nodeID: " + nodeId;
    }
}