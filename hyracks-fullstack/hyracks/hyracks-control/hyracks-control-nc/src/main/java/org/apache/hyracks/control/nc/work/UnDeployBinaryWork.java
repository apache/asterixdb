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

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.common.deployment.DeploymentStatus;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;

/**
 * undeploy binaries regarding to a deployment id
 *
 * @author yingyib
 */
public class UnDeployBinaryWork extends AbstractWork {

    private DeploymentId deploymentId;
    private NodeControllerService ncs;
    private final CcId ccId;

    public UnDeployBinaryWork(NodeControllerService ncs, DeploymentId deploymentId, CcId ccId) {
        this.deploymentId = deploymentId;
        this.ncs = ncs;
        this.ccId = ccId;
    }

    @Override
    public void run() {
        DeploymentStatus status;
        try {
            DeploymentUtils.undeploy(deploymentId, ncs.getContext().getJobSerializerDeserializerContainer(),
                    ncs.getServerContext());
            status = DeploymentStatus.SUCCEED;
        } catch (Exception e) {
            status = DeploymentStatus.FAIL;
        }
        try {
            IClusterController ccs = ncs.getClusterController(ccId);
            ccs.notifyDeployBinary(deploymentId, ncs.getId(), status);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
