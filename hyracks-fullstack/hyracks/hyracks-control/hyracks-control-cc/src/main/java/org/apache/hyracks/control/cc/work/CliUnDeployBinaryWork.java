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

import java.util.Collection;
import java.util.UUID;

import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.deployment.DeploymentRun;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.work.IPCResponder;
import org.apache.hyracks.control.common.work.SynchronizableWork;

/***
 * This is the work happens on the CC for an undeployment.
 * It first undeploys the jar to CC application context by giving it a deployment id.
 * Then, it remotely calls each NC service to undeploy by giving them a deployment id.
 * NOTE: in current implementation, a user cannot undeploy with the same deployment id simultaneously.
 *
 * @author yingyib
 */
public class CliUnDeployBinaryWork extends SynchronizableWork {

    private ClusterControllerService ccs;
    private DeploymentId deploymentId;
    private IPCResponder<DeploymentId> callback;

    public CliUnDeployBinaryWork(ClusterControllerService ncs, DeploymentId deploymentId,
            IPCResponder<DeploymentId> callback) {
        this.ccs = ncs;
        this.deploymentId = deploymentId;
        this.callback = callback;
    }

    @Override
    public void doRun() {
        try {
            if (deploymentId == null) {
                deploymentId = new DeploymentId(UUID.randomUUID().toString());
            }
            /**
             * Deploy for the cluster controller
             */
            DeploymentUtils.undeploy(deploymentId, ccs.getContext().getJobSerializerDeserializerContainer(),
                    ccs.getServerContext());

            /**
             * Deploy for the node controllers
             */
            INodeManager nodeManager = ccs.getNodeManager();
            Collection<String> nodeIds = nodeManager.getAllNodeIds();
            final DeploymentRun dRun = new DeploymentRun(nodeIds);

            /** The following call prevents a user to undeploy with the same deployment id simultaneously. */
            ccs.addDeploymentRun(deploymentId, dRun);

            /***
             * deploy binaries to each node controller
             */
            for (NodeControllerState ncs : nodeManager.getAllNodeControllerStates()) {
                ncs.getNodeController().undeployBinary(deploymentId);
            }

            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        /**
                         * wait for completion
                         */
                        dRun.waitForCompletion();
                        ccs.removeDeploymentRun(deploymentId);
                        callback.setValue(null);
                    } catch (Exception e) {
                        callback.setException(e);
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
