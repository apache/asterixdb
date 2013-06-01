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

package edu.uci.ics.hyracks.control.nc.work;

import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentStatus;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentUtils;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

/**
 * undeploy binaries regarding to a deployment id
 * 
 * @author yingyib
 */
public class UnDeployBinaryWork extends AbstractWork {

    private DeploymentId deploymentId;
    private NodeControllerService ncs;

    public UnDeployBinaryWork(NodeControllerService ncs, DeploymentId deploymentId) {
        this.deploymentId = deploymentId;
        this.ncs = ncs;
    }

    @Override
    public void run() {
        DeploymentStatus status;
        try {
            DeploymentUtils.undeploy(deploymentId, ncs.getApplicationContext().getJobSerializerDeserializerContainer(),
                    ncs.getServerContext());
            status = DeploymentStatus.SUCCEED;
        } catch (Exception e) {
            status = DeploymentStatus.FAIL;
        }
        try {
            IClusterController ccs = ncs.getClusterController();
            ccs.notifyDeployBinary(deploymentId, ncs.getId(), status);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
