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

import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.deployment.DeploymentRun;
import org.apache.hyracks.control.common.deployment.DeploymentStatus;

/***
 * This is the work happens on the CC when CC gets a deployment or undeployment notification status message from one NC.
 *
 * @author yingyib
 */
public class NotifyDeployBinaryWork extends AbstractHeartbeatWork {

    private final DeploymentId deploymentId;
    private DeploymentStatus deploymentStatus;

    public NotifyDeployBinaryWork(ClusterControllerService ccs, DeploymentId deploymentId, String nodeId,
            DeploymentStatus deploymentStatus) {
        super(ccs, nodeId, null);
        this.deploymentId = deploymentId;
        this.deploymentStatus = deploymentStatus;

    }

    @Override
    public void runWork() {
        // Triggered remotely by a NC to notify that the NC is deployed.
        DeploymentRun dRun = ccs.getDeploymentRun(deploymentId);
        dRun.notifyDeploymentStatus(nodeId, deploymentStatus);
    }

}
