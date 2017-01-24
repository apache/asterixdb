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

package org.apache.hyracks.control.common.deployment;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/**
 * The class maintain the status of a deployment process and the states
 * of all slave machines involved in the deployment.
 *
 * @author yingyib
 */
public class DeploymentRun implements IDeploymentStatusConditionVariable {

    private DeploymentStatus deploymentStatus = DeploymentStatus.FAIL;
    private final Set<String> deploymentNodeIds = new TreeSet<String>();

    public DeploymentRun(Collection<String> nodeIds) {
        deploymentNodeIds.addAll(nodeIds);
    }

    /**
     * One notify the deployment status
     *
     * @param nodeId
     * @param status
     */
    public synchronized void notifyDeploymentStatus(String nodeId, DeploymentStatus status) {
        if (status == DeploymentStatus.SUCCEED) {
            deploymentNodeIds.remove(nodeId);
            if (deploymentNodeIds.size() == 0) {
                deploymentStatus = DeploymentStatus.SUCCEED;
                notifyAll();
            }
        } else {
            deploymentNodeIds.clear();
            deploymentStatus = DeploymentStatus.FAIL;
            notifyAll();
        }
    }

    @Override
    public synchronized DeploymentStatus waitForCompletion() throws Exception {
        wait();
        return deploymentStatus;
    }

}
